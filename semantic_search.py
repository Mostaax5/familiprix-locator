"""Persistent multilingual semantic retrieval for the store catalogue.

The answer model is deliberately not used to discover catalogue vocabulary.
Product and category embeddings live in PostgreSQL/pgvector, so a Render worker
restart does not rebuild a large model or index in application memory.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
import unicodedata
from array import array
from collections import OrderedDict
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from database import DB_BACKEND, connect_db


def _env_flag(name, default=True):
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "on"}


SEMANTIC_SEARCH_ENABLED = _env_flag("SEMANTIC_SEARCH_ENABLED", True)
SEMANTIC_EMBEDDING_API_KEY = os.environ.get(
    "SEMANTIC_EMBEDDING_API_KEY",
    os.environ.get("GEMINI_API_KEY", ""),
).strip()
SEMANTIC_EMBEDDING_MODEL = (
    os.environ.get("SEMANTIC_EMBEDDING_MODEL", "gemini-embedding-2").strip()
    or "gemini-embedding-2"
)
SEMANTIC_EMBEDDING_DIMENSIONS = 768
SEMANTIC_EMBEDDING_BASE_URL = os.environ.get(
    "SEMANTIC_EMBEDDING_BASE_URL",
    "https://generativelanguage.googleapis.com/v1beta",
).rstrip("/")
SEMANTIC_INDEX_BATCH_SIZE = max(
    4, min(int(os.environ.get("SEMANTIC_INDEX_BATCH_SIZE", "40") or 40), 100)
)
SEMANTIC_INDEX_BATCH_PAUSE_SECONDS = max(
    0.0,
    min(
        float(
            os.environ.get("SEMANTIC_INDEX_BATCH_PAUSE_SECONDS", "31") or 31
        ),
        120.0,
    ),
)
SEMANTIC_QUERY_TIMEOUT_SECONDS = max(
    1.0,
    min(float(os.environ.get("SEMANTIC_QUERY_TIMEOUT_SECONDS", "4") or 4), 8.0),
)
SEMANTIC_INDEX_TIMEOUT_SECONDS = max(
    8.0,
    min(float(os.environ.get("SEMANTIC_INDEX_TIMEOUT_SECONDS", "30") or 30), 60.0),
)

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False
_INDEX_LOCK = threading.Lock()
_INDEX_RUNNING = False
_QUERY_CACHE_LOCK = threading.Lock()
_QUERY_CACHE = OrderedDict()
_QUERY_CACHE_MAX = 128
_STATUS_LOCK = threading.Lock()
_STATUS = {
    "enabled": bool(SEMANTIC_SEARCH_ENABLED),
    "configured": bool(SEMANTIC_EMBEDDING_API_KEY),
    "backend_supported": DB_BACKEND == "postgres",
    "schema_ready": False,
    "vector_index": "unavailable",
    "ready": False,
    "indexing": False,
    "stage": "idle",
    "model": SEMANTIC_EMBEDDING_MODEL,
    "dimensions": SEMANTIC_EMBEDDING_DIMENSIONS,
    "category_documents": 0,
    "product_documents": 0,
    "total_products": 0,
    "indexed_this_run": 0,
    "skipped_this_run": 0,
    "last_query_ms": 0,
    "last_query_hits": 0,
    "last_category_hits": [],
    "last_product_hits": [],
    "last_error": "",
    "started_at": 0.0,
    "updated_at": 0.0,
    "completed_at": 0.0,
}


def _set_status(**changes):
    with _STATUS_LOCK:
        _STATUS.update(changes)
        _STATUS["updated_at"] = time.time()


def semantic_search_status():
    with _STATUS_LOCK:
        status = dict(_STATUS)
    if not status["enabled"]:
        status["reason"] = "disabled"
    elif not status["backend_supported"]:
        status["reason"] = "postgres_required"
    elif not status["configured"]:
        status["reason"] = "embedding_key_missing"
    elif status["last_error"] and not status["ready"]:
        status["reason"] = "index_unavailable"
    else:
        status["reason"] = ""
    return status


def _normalize_text(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.replace("œ", "oe").replace("Œ", "OE")
    return re.sub(r"\s+", " ", text).strip().lower()


def _compact_text(value, limit):
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _document_key(row):
    gtin_key = re.sub(r"\D", "", str(row.get("gtin_key", "") or ""))
    barcode = re.sub(r"\D", "", str(row.get("barcode", "") or ""))
    if gtin_key:
        return f"gtin:{gtin_key}"
    if barcode:
        return f"barcode:{barcode}"
    return f"product:{int(row.get('id') or 0)}"


def _product_embedding_text(row):
    fields = (
        ("Nom officiel francais", row.get("official_name_fr")),
        ("Nom officiel anglais", row.get("official_name_en")),
        ("Nom planogramme", row.get("name")),
        ("Marque", row.get("brand")),
        ("Categorie", row.get("category")),
        ("Variante", row.get("variant")),
        ("Saveur ou parfum", row.get("flavour")),
        ("Format", " ".join(filter(None, (
            str(row.get("package_size", "") or "").strip(),
            str(row.get("package_unit", "") or "").strip(),
        )))),
        ("Concentration", row.get("strength")),
        ("Forme", row.get("dosage_form")),
        ("Usage", row.get("purpose")),
        ("Compatibilite", row.get("compatibility")),
        ("Ingredients", row.get("ingredients")),
        ("Description", _compact_text(row.get("description"), 1600)),
        ("Termes de recherche", _compact_text(row.get("search_terms"), 500)),
        ("Notes d usage", _compact_text(row.get("usage_notes"), 500)),
    )
    lines = [
        f"{label}: {_compact_text(value, 1800)}"
        for label, value in fields
        if _compact_text(value, 1800)
    ]
    return "\n".join(lines)[:5000]


def _category_embedding_text(category, sample_names):
    samples = " | ".join(sample_names[:12])
    return (
        f"Categorie de produits du magasin: {category}\n"
        f"Exemples de produits de cette categorie: {samples}"
    )[:4000]


def _content_hash(content):
    return hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()


def _vector_literal(values):
    return "[" + ",".join(f"{float(value):.8g}" for value in values) + "]"


def _normalized_embedding(values):
    vector = [float(value) for value in values]
    if len(vector) != SEMANTIC_EMBEDDING_DIMENSIONS:
        raise ValueError(
            f"dimension inattendue: {len(vector)} au lieu de "
            f"{SEMANTIC_EMBEDDING_DIMENSIONS}"
        )
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 0:
        raise ValueError("vecteur d embedding vide")
    return array("f", (value / norm for value in vector))


def _safe_provider_error(exc):
    if isinstance(exc, HTTPError):
        return f"HTTP {exc.code}"
    if isinstance(exc, URLError):
        return f"reseau: {str(exc.reason)[:120]}"
    return f"{type(exc).__name__}: {str(exc)[:160]}"


def _embed_texts(texts, task_type, timeout_seconds, max_attempts=3):
    if not texts:
        return []
    if not SEMANTIC_EMBEDDING_API_KEY:
        raise RuntimeError("cle d embedding absente")
    requests = [{
        "model": f"models/{SEMANTIC_EMBEDDING_MODEL}",
        "content": {"parts": [{"text": str(text)}]},
        "taskType": task_type,
        "outputDimensionality": SEMANTIC_EMBEDDING_DIMENSIONS,
    } for text in texts]
    url = (
        f"{SEMANTIC_EMBEDDING_BASE_URL}/models/"
        f"{SEMANTIC_EMBEDDING_MODEL}:batchEmbedContents?"
        f"{urlencode({'key': SEMANTIC_EMBEDDING_API_KEY})}"
    )
    payload = json.dumps({"requests": requests}, ensure_ascii=False).encode("utf-8")
    last_error = None
    attempt_count = max(1, min(int(max_attempts or 1), 3))
    for attempt in range(attempt_count):
        try:
            request = Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=timeout_seconds) as response:
                parsed = json.loads(response.read(12 * 1024 * 1024).decode("utf-8"))
            embeddings = parsed.get("embeddings") if isinstance(parsed, dict) else None
            if not isinstance(embeddings, list) or len(embeddings) != len(texts):
                raise ValueError("reponse d embedding incomplete")
            return [
                _normalized_embedding(embedding.get("values", []))
                for embedding in embeddings
            ]
        except HTTPError as exc:
            last_error = exc
            if (
                exc.code not in {429, 500, 502, 503, 504}
                or attempt == attempt_count - 1
            ):
                break
            retry_after = exc.headers.get("Retry-After", "") if exc.headers else ""
            try:
                delay = max(1.0, min(float(retry_after), 8.0))
            except (TypeError, ValueError):
                delay = float(2 ** attempt)
            time.sleep(delay)
        except (URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == attempt_count - 1:
                break
            time.sleep(float(2 ** attempt))
    raise RuntimeError(_safe_provider_error(last_error or RuntimeError("embedding impossible")))


def _query_embedding(query):
    normalized = _normalize_text(query)[:2000]
    if not normalized:
        return None
    cache_key = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    with _QUERY_CACHE_LOCK:
        cached = _QUERY_CACHE.pop(cache_key, None)
        if cached is not None:
            _QUERY_CACHE[cache_key] = cached
            return cached
    vector = _embed_texts(
        [normalized], "RETRIEVAL_QUERY", SEMANTIC_QUERY_TIMEOUT_SECONDS,
        max_attempts=1,
    )[0]
    with _QUERY_CACHE_LOCK:
        _QUERY_CACHE[cache_key] = vector
        while len(_QUERY_CACHE) > _QUERY_CACHE_MAX:
            _QUERY_CACHE.popitem(last=False)
    return vector


def ensure_semantic_search_schema(db=None):
    """Create pgvector storage independently from the main boot migration."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return True
    if not (
        SEMANTIC_SEARCH_ENABLED
        and SEMANTIC_EMBEDDING_API_KEY
        and DB_BACKEND == "postgres"
    ):
        return False
    owns_connection = db is None
    db = db or connect_db()
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            if owns_connection:
                db.close()
            return True
        try:
            db.execute("SELECT set_config('lock_timeout', ?, true)", ("5s",))
            db.execute("SELECT set_config('statement_timeout', ?, true)", ("30s",))
            db.execute("CREATE EXTENSION IF NOT EXISTS vector")
            db.execute(f"""
                CREATE TABLE IF NOT EXISTS product_semantic_documents (
                    document_key     TEXT PRIMARY KEY,
                    document_type    TEXT NOT NULL,
                    product_id       BIGINT NOT NULL DEFAULT 0,
                    barcode          TEXT NOT NULL DEFAULT '',
                    category         TEXT NOT NULL DEFAULT '',
                    content_hash     TEXT NOT NULL,
                    content_preview  TEXT NOT NULL DEFAULT '',
                    embedding_model  TEXT NOT NULL,
                    dimensions       INTEGER NOT NULL,
                    embedding        vector({SEMANTIC_EMBEDDING_DIMENSIONS}) NOT NULL,
                    indexed_at       TEXT NOT NULL DEFAULT ''
                )
            """)
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_semantic_documents_type "
                "ON product_semantic_documents(document_type)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_semantic_documents_product "
                "ON product_semantic_documents(product_id)"
            )
            db.commit()
            _SCHEMA_READY = True
            _set_status(
                schema_ready=True, vector_index="exact", last_error="",
            )
            # HNSW makes nearest-neighbour lookup essentially constant-time,
            # but semantic search remains correct with pgvector's exact scan if
            # an older managed PostgreSQL plan does not expose HNSW yet.
            try:
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_semantic_product_embedding "
                    "ON product_semantic_documents USING hnsw "
                    "(embedding vector_cosine_ops) "
                    "WHERE document_type='product'"
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_semantic_category_embedding "
                    "ON product_semantic_documents USING hnsw "
                    "(embedding vector_cosine_ops) "
                    "WHERE document_type='category'"
                )
                db.commit()
                _set_status(vector_index="hnsw")
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
            return True
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                pass
            _set_status(
                schema_ready=False,
                ready=False,
                stage="schema_error",
                last_error=_safe_provider_error(exc),
            )
            return False
        finally:
            if owns_connection:
                db.close()


_PRODUCT_DOCUMENT_COLUMNS = """
    id, name, brand, description, search_terms, usage_notes, barcode, gtin_key,
    category, package_size, package_unit, variant, flavour, strength,
    dosage_form, ingredients, compatibility, purpose,
    official_name_fr, official_name_en, in_stock
"""


_SEMANTIC_VERIFIED_FIELDS = frozenset({
    "brand", "category", "package_size", "package_unit", "variant",
    "flavour", "strength", "dosage_form", "ingredients", "compatibility",
    "purpose", "official_name_fr", "official_name_en",
})


def _verified_fields_by_product(db):
    verified = {}
    try:
        for raw in db.execute(
            """SELECT product_id, field_name FROM product_field_evidence
               WHERE active=1 AND verification_status='verified'"""
        ):
            row = dict(raw)
            field = str(row.get("field_name", "") or "")
            if field in _SEMANTIC_VERIFIED_FIELDS:
                verified.setdefault(int(row.get("product_id") or 0), set()).add(field)
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return {}
    return verified


def _iter_product_documents(db, verified_fields_by_product):
    cursor = db.execute(
        f"SELECT {_PRODUCT_DOCUMENT_COLUMNS} FROM products "
        "ORDER BY CASE WHEN in_stock=1 THEN 0 ELSE 1 END, id"
    )
    seen = set()
    while True:
        rows = cursor.fetchmany(200)
        if not rows:
            break
        for raw in rows:
            row = dict(raw)
            verified_fields = verified_fields_by_product.get(
                int(row.get("id") or 0), set()
            )
            for field in _SEMANTIC_VERIFIED_FIELDS - verified_fields:
                row[field] = ""
            key = _document_key(row)
            if key in seen:
                continue
            seen.add(key)
            content = _product_embedding_text(row)
            if not content:
                continue
            yield {
                "document_key": key,
                "document_type": "product",
                "product_id": int(row.get("id") or 0),
                "barcode": re.sub(r"\D", "", str(row.get("barcode", "") or "")),
                "category": _compact_text(row.get("category"), 500),
                "content": content,
                "content_hash": _content_hash(content),
            }


def _category_documents(db, verified_fields_by_product):
    cursor = db.execute(
        "SELECT id, category, name, official_name_fr, official_name_en "
        "FROM products WHERE TRIM(COALESCE(category,''))<>'' ORDER BY category, id"
    )
    categories = OrderedDict()
    while True:
        rows = cursor.fetchmany(300)
        if not rows:
            break
        for raw in rows:
            row = dict(raw)
            verified_fields = verified_fields_by_product.get(
                int(row.get("id") or 0), set()
            )
            if "category" not in verified_fields:
                continue
            category = _compact_text(row.get("category"), 500)
            normalized = _normalize_text(category)
            if not normalized:
                continue
            entry = categories.setdefault(normalized, {
                "category": category, "samples": [],
            })
            if len(entry["samples"]) >= 12:
                continue
            name = _compact_text(
                (
                    row.get("official_name_fr")
                    if "official_name_fr" in verified_fields else ""
                )
                or (
                    row.get("official_name_en")
                    if "official_name_en" in verified_fields else ""
                )
                or row.get("name"),
                240,
            )
            if name and name not in entry["samples"]:
                entry["samples"].append(name)
    documents = []
    for normalized, entry in categories.items():
        content = _category_embedding_text(entry["category"], entry["samples"])
        documents.append({
            "document_key": "category:" + hashlib.sha256(
                normalized.encode("utf-8")
            ).hexdigest()[:32],
            "document_type": "category",
            "product_id": 0,
            "barcode": "",
            "category": entry["category"],
            "content": content,
            "content_hash": _content_hash(content),
        })
    return documents


def _existing_documents(db):
    existing = {}
    for row in db.execute(
        "SELECT document_key, content_hash, product_id, barcode, category "
        "FROM product_semantic_documents "
        "WHERE embedding_model=? AND dimensions=?",
        (SEMANTIC_EMBEDDING_MODEL, SEMANTIC_EMBEDDING_DIMENSIONS),
    ):
        values = dict(row)
        existing[str(values.get("document_key", ""))] = values
    return existing


def _upsert_document_batch(db, documents, vectors):
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    values = []
    for document, vector in zip(documents, vectors):
        values.append((
            document["document_key"], document["document_type"],
            document["product_id"], document["barcode"], document["category"],
            document["content_hash"], document["content"][:1200],
            SEMANTIC_EMBEDDING_MODEL, SEMANTIC_EMBEDDING_DIMENSIONS,
            _vector_literal(vector), now,
        ))
    db.executemany(
        """INSERT INTO product_semantic_documents(
               document_key, document_type, product_id, barcode, category,
               content_hash, content_preview, embedding_model, dimensions,
               embedding, indexed_at
           ) VALUES(?,?,?,?,?,?,?,?,?,?::vector,?)
           ON CONFLICT(document_key) DO UPDATE SET
             document_type=excluded.document_type,
             product_id=excluded.product_id,
             barcode=excluded.barcode,
             category=excluded.category,
             content_hash=excluded.content_hash,
             content_preview=excluded.content_preview,
             embedding_model=excluded.embedding_model,
             dimensions=excluded.dimensions,
             embedding=excluded.embedding,
             indexed_at=excluded.indexed_at""",
        values,
    )
    db.commit()


def _refresh_document_metadata(db, document):
    db.execute(
        """UPDATE product_semantic_documents
           SET product_id=?, barcode=?, category=?
           WHERE document_key=? AND content_hash=?
             AND embedding_model=? AND dimensions=?""",
        (
            document["product_id"], document["barcode"], document["category"],
            document["document_key"], document["content_hash"],
            SEMANTIC_EMBEDDING_MODEL, SEMANTIC_EMBEDDING_DIMENSIONS,
        ),
    )


def _index_documents(db, documents, existing, stage):
    pending = []
    indexed = 0
    skipped = 0
    metadata_updates = 0
    _set_status(stage=stage)

    def flush():
        nonlocal indexed
        if not pending:
            return
        vectors = _embed_texts(
            [document["content"] for document in pending],
            "RETRIEVAL_DOCUMENT",
            SEMANTIC_INDEX_TIMEOUT_SECONDS,
        )
        _upsert_document_batch(db, pending, vectors)
        indexed += len(pending)
        _set_status(indexed_this_run=indexed)
        pending.clear()

    for document in documents:
        previous = existing.get(document["document_key"])
        if (
            previous
            and str(previous.get("content_hash", "")) == document["content_hash"]
        ):
            skipped += 1
            if (
                int(previous.get("product_id") or 0) != document["product_id"]
                or str(previous.get("barcode", "") or "") != document["barcode"]
                or str(previous.get("category", "") or "") != document["category"]
            ):
                _refresh_document_metadata(db, document)
                metadata_updates += 1
                if metadata_updates % 200 == 0:
                    db.commit()
            continue
        pending.append(document)
        if len(pending) >= SEMANTIC_INDEX_BATCH_SIZE:
            flush()
            # Keep the one-time catalogue build below the embedding provider's
            # free-tier rolling quota and leave headroom for live employee
            # queries. Completed batches are already durable in PostgreSQL.
            if SEMANTIC_INDEX_BATCH_PAUSE_SECONDS:
                time.sleep(SEMANTIC_INDEX_BATCH_PAUSE_SECONDS)
    flush()
    if metadata_updates:
        db.commit()
    return indexed, skipped


def _refresh_index_counts(db):
    row = db.execute(
        """SELECT
             SUM(CASE WHEN document_type='category' THEN 1 ELSE 0 END) AS categories,
             SUM(CASE WHEN document_type='product' THEN 1 ELSE 0 END) AS products
           FROM product_semantic_documents
           WHERE embedding_model=? AND dimensions=?""",
        (SEMANTIC_EMBEDDING_MODEL, SEMANTIC_EMBEDDING_DIMENSIONS),
    ).fetchone()
    values = dict(row) if row else {}
    categories = int(values.get("categories") or 0)
    products = int(values.get("products") or 0)
    _set_status(
        category_documents=categories,
        product_documents=products,
        ready=bool(categories or products),
    )
    return categories, products


def _semantic_index_worker():
    global _INDEX_RUNNING
    db = None
    indexed_total = 0
    skipped_total = 0
    _set_status(
        indexing=True, stage="starting", started_at=time.time(),
        completed_at=0.0, indexed_this_run=0, skipped_this_run=0,
        last_error="",
    )
    try:
        db = connect_db()
        if not ensure_semantic_search_schema(db):
            return
        count_row = db.execute(
            "SELECT COUNT(DISTINCT COALESCE(NULLIF(gtin_key,''), NULLIF(barcode,''), 'product:' || id)) AS count FROM products"
        ).fetchone()
        values = dict(count_row) if count_row else {}
        _set_status(total_products=int(values.get("count") or 0))
        existing = _existing_documents(db)
        # A previous quota-limited run may already have committed useful
        # category/product batches. Publish them before requesting one more
        # embedding so searches never wait for a full-catalogue pass.
        _refresh_index_counts(db)
        verified_fields = _verified_fields_by_product(db)

        category_documents = _category_documents(db, verified_fields)
        indexed, skipped = _index_documents(
            db, category_documents, existing, "categories",
        )
        indexed_total += indexed
        skipped_total += skipped
        _refresh_index_counts(db)

        indexed, skipped = _index_documents(
            db, _iter_product_documents(db, verified_fields), existing, "products",
        )
        indexed_total += indexed
        skipped_total += skipped
        _refresh_index_counts(db)
        _set_status(
            stage="ready", ready=True, completed_at=time.time(),
            indexed_this_run=indexed_total, skipped_this_run=skipped_total,
            last_error="",
        )
        verified_fields.clear()
    except Exception as exc:
        if db is not None:
            try:
                db.rollback()
            except Exception:
                pass
            try:
                _refresh_index_counts(db)
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
        _set_status(
            stage="error", last_error=_safe_provider_error(exc),
            indexed_this_run=indexed_total, skipped_this_run=skipped_total,
        )
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
        with _INDEX_LOCK:
            _INDEX_RUNNING = False
        _set_status(indexing=False)


def schedule_semantic_product_index(force=False):
    """Start a bounded background sync; return False when unavailable/running."""
    global _INDEX_RUNNING
    if not (
        SEMANTIC_SEARCH_ENABLED
        and SEMANTIC_EMBEDDING_API_KEY
        and DB_BACKEND == "postgres"
    ):
        return False
    with _INDEX_LOCK:
        if _INDEX_RUNNING:
            return False
        status = semantic_search_status()
        if (
            not force
            and status.get("completed_at")
            and time.time() - float(status["completed_at"]) < 300
        ):
            return False
        _INDEX_RUNNING = True
    threading.Thread(
        target=_semantic_index_worker,
        daemon=True,
        name="semantic-product-index",
    ).start()
    return True


def maybe_resume_semantic_product_index():
    status = semantic_search_status()
    if status.get("indexing") or not status.get("configured"):
        return False
    if status.get("stage") == "error" and (
        time.time() - float(status.get("updated_at", 0) or 0) < 300
    ):
        return False
    return schedule_semantic_product_index(force=False)


def semantic_product_hits(query, product_limit=80, category_limit=12):
    """Return independent product/category vector ranks for RRF fusion."""
    status = semantic_search_status()
    if not status.get("ready") or not status.get("schema_ready"):
        return []
    if (
        "HTTP 429" in str(status.get("last_error", "") or "")
        and time.time() - float(status.get("updated_at", 0) or 0) < 45
    ):
        # The same embedding quota powers indexing and query vectors. Repeating
        # a known rate-limited request three times made an employee wait tens of
        # seconds before the lexical/AI-planner recovery could run.
        return []
    started = time.perf_counter()
    db = None
    try:
        vector = _query_embedding(query)
        if vector is None:
            return []
        literal = _vector_literal(vector)
        db = connect_db()
        db.execute("SELECT set_config('statement_timeout', ?, true)", ("2500ms",))
        params = (
            SEMANTIC_EMBEDDING_MODEL,
            SEMANTIC_EMBEDDING_DIMENSIONS,
            literal,
            max(1, min(int(product_limit or 80), 160)),
        )
        product_rows = db.execute(
            """SELECT document_type, product_id, barcode, category,
                      1 - (embedding <=> ?::vector) AS similarity
               FROM product_semantic_documents
               WHERE document_type='product' AND embedding_model=?
                 AND dimensions=?
               ORDER BY embedding <=> ?::vector
               LIMIT ?""",
            (literal, *params[:2], literal, params[3]),
        ).fetchall()
        category_rows = db.execute(
            """SELECT document_type, product_id, barcode, category,
                      1 - (embedding <=> ?::vector) AS similarity
               FROM product_semantic_documents
               WHERE document_type='category' AND embedding_model=?
                 AND dimensions=?
               ORDER BY embedding <=> ?::vector
               LIMIT ?""",
            (
                literal, SEMANTIC_EMBEDDING_MODEL,
                SEMANTIC_EMBEDDING_DIMENSIONS, literal,
                max(1, min(int(category_limit or 12), 30)),
            ),
        ).fetchall()
        hits = []
        for kind, rows in (("product", product_rows), ("category", category_rows)):
            for rank, raw in enumerate(rows, start=1):
                row = dict(raw)
                similarity = float(row.get("similarity") or 0)
                if similarity < 0.20:
                    continue
                hits.append({
                    "kind": kind,
                    "rank": rank,
                    "product_id": int(row.get("product_id") or 0),
                    "barcode": str(row.get("barcode", "") or ""),
                    "category": str(row.get("category", "") or ""),
                    "similarity": round(similarity, 5),
                })
        elapsed_ms = int(round((time.perf_counter() - started) * 1000))
        category_diagnostics = [
            {
                "rank": hit["rank"],
                "category": hit["category"][:240],
                "similarity": hit["similarity"],
            }
            for hit in hits if hit["kind"] == "category"
        ][:8]
        product_diagnostics = [
            {
                "rank": hit["rank"],
                "product_id": hit["product_id"],
                "similarity": hit["similarity"],
            }
            for hit in hits if hit["kind"] == "product"
        ][:8]
        _set_status(
            last_query_ms=elapsed_ms,
            last_query_hits=len(hits),
            last_category_hits=category_diagnostics,
            last_product_hits=product_diagnostics,
            stage=(
                "partial_ready"
                if semantic_search_status().get("stage") == "error"
                else semantic_search_status().get("stage", "ready")
            ),
            last_error="",
        )
        return hits
    except Exception as exc:
        if db is not None:
            try:
                db.rollback()
            except Exception:
                pass
        _set_status(
            last_query_ms=int(round((time.perf_counter() - started) * 1000)),
            last_query_hits=0,
            last_category_hits=[],
            last_product_hits=[],
            last_error=_safe_provider_error(exc),
        )
        return []
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
