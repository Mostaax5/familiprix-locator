import json
import gc
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, jsonify, request

from auth import require_editor, utc_now_iso
from database import connect_db, ensure_product_data_ready, get_db
from memory_guard import release_unused_memory
from product_data import (
    create_review_issue,
    exact_gtin_variants,
    gtin_identity_key,
    sync_reference_identifiers_to_product,
    upsert_reference_candidate,
    upsert_reference_identifier,
)
from regulatory_data import (
    HEALTH_CANADA_AUTHORITY,
    HEALTH_CANADA_DPD_PAGE,
    HEALTH_CANADA_DPD_SOURCE,
    HEALTH_CANADA_DPD_UPC_NOTICE,
    extract_regulatory_identifiers,
    group_unambiguous_dpd_matches,
    merge_regulatory_candidates,
    verify_regulatory_candidate,
)


regulatory_bp = Blueprint("regulatory", __name__)

_SYNC_SOURCE = "health_canada_regulatory"
_DPD_CHECK_SOURCE = "health_canada_dpd_marketed"
_ONLINE_CHECK_SOURCE = "exact_upc_regulatory_labels"
_SYNC_LOCK = threading.Lock()
_STOP_EVENT = threading.Event()
_STATE = {
    "running": False,
    "status": "idle",
    "phase": "",
    "started_at": "",
    "updated_at": "",
    "completed_at": "",
    "source_version": "",
    "catalogue_gtins": 0,
    "checked_gtins": 0,
    "exact_matches": 0,
    "verified_identifiers": 0,
    "review_candidates": 0,
    "conflicts": 0,
    "online_checked": 0,
    "remaining_online": 0,
    "error": "",
}


def _bounded_env_int(name, default, minimum, maximum):
    try:
        return min(max(int(os.environ.get(name, default)), minimum), maximum)
    except (TypeError, ValueError):
        return default


_ONLINE_BATCH_LIMIT = _bounded_env_int(
    "REGULATORY_ONLINE_BATCH", 200, 10, 1000
)


def _state_update(**values):
    with _SYNC_LOCK:
        for key, value in values.items():
            if key in _STATE:
                _STATE[key] = value
        _STATE["updated_at"] = utc_now_iso()


def _state_snapshot():
    with _SYNC_LOCK:
        return dict(_STATE)


def _save_state(db):
    state = _state_snapshot()
    db.execute(
        """INSERT INTO regulatory_sync_state
           (source, status, phase, started_at, updated_at, completed_at,
            source_version, catalogue_gtins, checked_gtins, exact_matches,
            verified_identifiers, review_candidates, conflicts, online_checked,
            remaining_online, error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
             status=excluded.status, phase=excluded.phase,
             started_at=excluded.started_at, updated_at=excluded.updated_at,
             completed_at=excluded.completed_at,
             source_version=excluded.source_version,
             catalogue_gtins=excluded.catalogue_gtins,
             checked_gtins=excluded.checked_gtins,
             exact_matches=excluded.exact_matches,
             verified_identifiers=excluded.verified_identifiers,
             review_candidates=excluded.review_candidates,
             conflicts=excluded.conflicts,
             online_checked=excluded.online_checked,
             remaining_online=excluded.remaining_online,
             error=excluded.error""",
        (
            _SYNC_SOURCE, state["status"], state["phase"],
            state["started_at"], state["updated_at"], state["completed_at"],
            state["source_version"], state["catalogue_gtins"],
            state["checked_gtins"], state["exact_matches"],
            state["verified_identifiers"], state["review_candidates"],
            state["conflicts"], state["online_checked"],
            state["remaining_online"], state["error"],
        ),
    )


def _catalogue_items(db):
    items = {}
    queries = (
        """SELECT barcode, name, brand,
                  SUBSTR(COALESCE(description,''), 1, 800) AS description,
                  source
           FROM product_reference
           WHERE TRIM(COALESCE(barcode,''))<>''""",
        """SELECT barcode, name, brand,
                  SUBSTR(COALESCE(description,''), 1, 800) AS description,
                  '' AS source
           FROM products
           WHERE TRIM(COALESCE(barcode,''))<>''""",
    )
    for query in queries:
        for row in db.execute(query).fetchall():
            item = dict(row)
            key = gtin_identity_key(item.get("barcode", ""))
            if not key:
                continue
            current = items.setdefault(key, {
                "gtin_key": key,
                "barcode": str(item.get("barcode", "") or "").strip(),
                "name": "", "brand": "", "description": "", "source": "",
            })
            for field in ("name", "brand", "description", "source"):
                value = str(item.get(field, "") or "").strip()
                if value and not current[field]:
                    current[field] = value
    return items


def _product_rows_for_key(db, key, barcode=""):
    """Load only products touched by one identifier association."""
    rows = [dict(row) for row in db.execute(
        "SELECT * FROM products WHERE gtin_key=?", (str(key or ""),)
    ).fetchall()]
    if rows or not barcode:
        return rows
    variants = exact_gtin_variants(barcode)
    if not variants:
        return []
    placeholders = ",".join("?" for _ in variants)
    return [
        dict(row) for row in db.execute(
            f"SELECT * FROM products WHERE barcode IN ({placeholders})",
            tuple(variants),
        ).fetchall()
        if gtin_identity_key(dict(row).get("barcode", "")) == key
    ]


def _seed_catalogue_label_candidates(db, now):
    """Preserve regulatory labels already attached to an exact catalogue UPC."""
    queries = (
        """SELECT barcode, name,
                  SUBSTR(COALESCE(description,''), 1, 2000) AS description,
                  SUBSTR(COALESCE(ingredients,''), 1, 1000) AS ingredients,
                  source, source_url
           FROM product_reference
           WHERE TRIM(COALESCE(barcode,''))<>''""",
        """SELECT barcode, name,
                  SUBSTR(COALESCE(description,''), 1, 2000) AS description,
                  SUBSTR(COALESCE(ingredients,''), 1, 1000) AS ingredients,
                  COALESCE(primary_source, '') AS source,
                  COALESCE(NULLIF(primary_source_url,''), source_url, '')
                    AS source_url
           FROM products
           WHERE TRIM(COALESCE(barcode,''))<>''""",
    )
    seeded = 0
    seen = set()
    for query in queries:
        for raw in db.execute(query).fetchall():
            row = dict(raw)
            barcode = str(row.get("barcode", "") or "").strip()
            gtin_key = gtin_identity_key(barcode)
            if not gtin_key:
                continue
            for identifier in extract_regulatory_identifiers(
                row.get("name", ""), row.get("description", ""),
                row.get("ingredients", ""),
            ):
                key = (
                    gtin_key, identifier["type"], identifier["value"]
                )
                if key in seen:
                    continue
                seen.add(key)
                saved = upsert_reference_identifier(
                    db, barcode, identifier["type"], identifier["value"],
                    authority=HEALTH_CANADA_AUTHORITY,
                    source=str(row.get("source", "") or "Catalogue exact UPC"),
                    source_url=str(row.get("source_url", "") or ""),
                    source_record_id=str(row.get("name", "") or ""),
                    match_method="exact_gtin_labeled_source",
                    confidence=0.75,
                    verification_status="requires_review",
                    imported_at=now,
                )
                seeded += int(bool(saved))
    return seeded


def _checked_keys(db, source):
    return {
        str(dict(row).get("gtin_key", ""))
        for row in db.execute(
            """SELECT gtin_key FROM regulatory_gtin_checks
               WHERE source=? AND status NOT IN ('transient_error','interrupted')""",
            (source,),
        ).fetchall()
    }


def _write_checks(db, source, rows):
    values = []
    for row in rows:
        values.append((
            str(row.get("gtin_key", "")), str(row.get("barcode", "")), source,
            str(row.get("status", "")), str(row.get("checked_at", "")),
            str(row.get("source_version", "")),
            json.dumps(row.get("details", {}), ensure_ascii=False)[:12000],
        ))
    if not values:
        return
    db.executemany(
        """INSERT INTO regulatory_gtin_checks
           (gtin_key, barcode, source, status, checked_at, source_version,
            details_json) VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(gtin_key, source) DO UPDATE SET
             barcode=excluded.barcode, status=excluded.status,
             checked_at=excluded.checked_at,
             source_version=excluded.source_version,
             details_json=excluded.details_json""",
        values,
    )


def _official_reference_metadata(barcode, record):
    brand_en = str(record.get("brand_name", "") or "").strip()
    brand_fr = str(record.get("brand_name_fr", "") or "").strip()
    descriptor = str(
        record.get("descriptor_fr", "") or record.get("descriptor", "") or ""
    ).strip()
    product_information = str(record.get("product_information", "") or "").strip()
    description_parts = [part for part in (brand_fr or brand_en, descriptor, product_information) if part]
    return {
        "barcode": barcode,
        "official_name_en": brand_en,
        "official_name_fr": brand_fr,
        "description": ". ".join(description_parts)[:6000] if len(description_parts) > 1 else "",
        "source": HEALTH_CANADA_DPD_SOURCE,
        "source_url": HEALTH_CANADA_DPD_PAGE,
        "source_record_id": str(record.get("drug_code", "") or ""),
    }


def _store_dpd_results(db, items, matches, version, now):
    verified_groups, conflicts = group_unambiguous_dpd_matches(matches)
    verified_count = 0
    affected_ids = set()
    checks = []
    for key, item in items.items():
        rows = verified_groups.get(key, [])
        conflict_rows = conflicts.get(key, [])
        if rows:
            record = rows[0]
            barcode = item["barcode"]
            saved_din = upsert_reference_identifier(
                db, barcode, "DIN", record.get("din", ""),
                authority=HEALTH_CANADA_AUTHORITY,
                source=HEALTH_CANADA_DPD_SOURCE,
                source_url=HEALTH_CANADA_DPD_PAGE,
                source_record_id=record.get("drug_code", ""),
                match_method="exact_gtin_health_canada_packaging",
                confidence=1.0, verification_status="verified",
                imported_at=now, last_verified_at=now,
            )
            saved_hc = upsert_reference_identifier(
                db, barcode, "HEALTH_CANADA_ID", record.get("drug_code", ""),
                authority=HEALTH_CANADA_AUTHORITY,
                source=HEALTH_CANADA_DPD_SOURCE,
                source_url=HEALTH_CANADA_DPD_PAGE,
                source_record_id=record.get("drug_code", ""),
                match_method="exact_gtin_health_canada_packaging",
                confidence=1.0, verification_status="verified",
                imported_at=now, last_verified_at=now,
            )
            upsert_reference_candidate(
                db, _official_reference_metadata(barcode, record),
                imported_at=now,
            )
            verified_count += int(bool(saved_din)) + int(bool(saved_hc))
            for product in _product_rows_for_key(db, key, barcode):
                sync_reference_identifiers_to_product(
                    db, product, imported_at=now
                )
                affected_ids.add(int(product["id"]))
            checks.append({
                "gtin_key": key, "barcode": barcode, "status": "exact_match",
                "checked_at": now, "source_version": version,
                "details": {"din": record.get("din", ""), "drug_code": record.get("drug_code", "")},
            })
        elif conflict_rows:
            dins = sorted({str(row.get("din", "")) for row in conflict_rows})
            for row in conflict_rows:
                upsert_reference_identifier(
                    db, item["barcode"], "DIN", row.get("din", ""),
                    authority=HEALTH_CANADA_AUTHORITY,
                    source=HEALTH_CANADA_DPD_SOURCE,
                    source_url=HEALTH_CANADA_DPD_PAGE,
                    source_record_id=row.get("drug_code", ""),
                    match_method="exact_gtin_health_canada_conflict",
                    confidence=1.0, verification_status="requires_review",
                    imported_at=now,
                )
            for product in _product_rows_for_key(
                db, key, item.get("barcode", "")
            ):
                create_review_issue(
                    db, product["id"], "identifier_conflict",
                    candidate_value="DIN: " + " | ".join(dins),
                    source=HEALTH_CANADA_DPD_SOURCE,
                    source_url=HEALTH_CANADA_DPD_PAGE,
                    match_method="multiple_dins_for_exact_gtin",
                    confidence=1.0,
                    details={"reason": "multiple_dins_for_exact_gtin", "dins": dins},
                    created_at=now,
                )
                affected_ids.add(int(product["id"]))
            checks.append({
                "gtin_key": key, "barcode": item["barcode"], "status": "conflict",
                "checked_at": now, "source_version": version,
                "details": {"dins": dins},
            })
        else:
            checks.append({
                "gtin_key": key, "barcode": item["barcode"], "status": "no_match",
                "checked_at": now, "source_version": version, "details": {},
            })
    _write_checks(db, _DPD_CHECK_SOURCE, checks)
    return {
        "exact_matches": len(verified_groups),
        "verified_identifiers": verified_count,
        "conflicts": len(conflicts),
        "affected_ids": affected_ids,
    }


_REGULATED_HINTS = {
    "acetaminophene", "advil", "allergie", "analgesique", "aspirine", "calcium",
    "capsule", "capsules", "collagene", "comprime", "comprimes", "dermatologie",
    "digestion", "din", "douleur", "echinacea", "fer", "fievre", "gummy",
    "gummies", "homeopath", "ibuprofene", "magnesium", "melatonine", "mineral",
    "motrin", "naturel", "npn", "omega", "probiot", "sirop", "sirops",
    "solaire", "supplement", "supplements", "tabac", "tylenol", "vitamine",
    "vitamin", "zinc",
}


def _likely_regulated(item):
    from routes.products import normalize_search_text

    text = normalize_search_text(" ".join(
        str(item.get(field, "") or "")
        for field in ("name", "brand", "description", "source")
    ))
    tokens = set(text.split())
    if tokens & _REGULATED_HINTS:
        return True
    return bool(re.search(r"(?:^| )\d+(?:[.,]\d+)?\s*(?:mg|mcg|ug|ui|iu)(?: |$)", text))


def _metadata_from_verification(barcode, result):
    return {
        "barcode": barcode,
        "official_name_en": result.get("official_name", ""),
        "manufacturer": result.get("manufacturer", ""),
        "dosage_form": result.get("dosage_form", ""),
        "ingredients": result.get("ingredients", ""),
        "purpose": result.get("purpose", ""),
        "route_of_administration": result.get("route_of_administration", ""),
        "description": result.get("description", ""),
        "source": result.get("source", ""),
        "source_url": result.get("source_url", ""),
        "source_record_id": result.get("source_record_id", ""),
    }


def _store_verified_candidate(db, item, candidate, result, now):
    identifier_type = result.get("identifier_type", candidate.get("type", ""))
    saved = upsert_reference_identifier(
        db, item["barcode"], identifier_type, result.get("value", ""),
        authority=HEALTH_CANADA_AUTHORITY,
        source=result.get("source", ""), source_url=result.get("source_url", ""),
        source_record_id=result.get("source_record_id", ""),
        match_method=result.get("match_method", "official_verification"),
        confidence=result.get("confidence", 0.98),
        verification_status="verified", imported_at=now,
        last_verified_at=now,
    )
    if result.get("source_record_id"):
        upsert_reference_identifier(
            db, item["barcode"], "HEALTH_CANADA_ID",
            result.get("source_record_id", ""),
            authority=HEALTH_CANADA_AUTHORITY,
            source=result.get("source", ""), source_url=result.get("source_url", ""),
            source_record_id=result.get("source_record_id", ""),
            match_method=result.get("match_method", "official_verification"),
            confidence=result.get("confidence", 0.98),
            verification_status="verified", imported_at=now,
            last_verified_at=now,
        )
    upsert_reference_candidate(
        db, _metadata_from_verification(item["barcode"], result),
        imported_at=now,
    )
    affected_ids = set()
    for product in _product_rows_for_key(
        db, item["gtin_key"], item.get("barcode", "")
    ):
        sync_reference_identifiers_to_product(db, product, imported_at=now)
        affected_ids.add(int(product["id"]))
    return int(bool(saved)), affected_ids


def _reject_candidate(db, row):
    """Remove a definite official mismatch from employee-facing search."""
    db.execute(
        """UPDATE product_reference_identifiers
           SET verification_status='rejected'
           WHERE id=? AND verification_status='requires_review'""",
        (row["id"],),
    )
    for product in _product_rows_for_key(
        db, row.get("gtin_key"), row.get("barcode", "")
    ):
        db.execute(
            """UPDATE product_identifiers SET verification_status='rejected'
               WHERE product_id=? AND identifier_type=? AND identifier_value=?
                 AND verification_status='requires_review'""",
            (
                product["id"], row.get("identifier_type", ""),
                row.get("identifier_value", ""),
            ),
        )


def _verify_candidates(db, items, now):
    rows = [dict(row) for row in db.execute(
        """SELECT * FROM product_reference_identifiers
           WHERE verification_status='requires_review'
             AND identifier_type IN ('DIN','NPN','DIN_HM')
             AND match_method='exact_gtin_labeled_source'
           ORDER BY id LIMIT 1000"""
    ).fetchall()]
    if not rows:
        return {"verified": 0, "review": 0, "affected_ids": set()}

    def verify(row):
        item = items.get(row.get("gtin_key"), {})
        candidate = {
            "type": row.get("identifier_type", ""),
            "value": row.get("identifier_value", ""),
            "barcode": row.get("barcode", ""),
            "product_name": row.get("source_record_id", "") or item.get("name", ""),
            "source": row.get("source", ""),
            "source_url": row.get("source_url", ""),
        }
        return row, item, candidate, verify_regulatory_candidate(
            candidate, catalog_name=item.get("name", "")
        )

    verified = review = 0
    affected_ids = set()
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(verify, row) for row in rows]
        for future in as_completed(futures):
            if _STOP_EVENT.is_set():
                break
            row, item, candidate, result = future.result()
            if result.get("verified"):
                saved, affected = _store_verified_candidate(
                    db, item, candidate, result, now
                )
                verified += saved
                affected_ids.update(affected)
                db.execute(
                    """UPDATE product_reference_identifiers
                       SET verification_status='verified', confidence=?,
                           last_verified_at=? WHERE id=?""",
                    (result.get("confidence", 0.98), now, row["id"]),
                )
            elif (
                result.get("reason") == "official_source_unavailable"
                or result.get("probable")
            ):
                # The exact-UPC page explicitly labels this identifier. Keep it
                # useful as "À confirmer" until the official API is reachable.
                for product in _product_rows_for_key(
                    db, row.get("gtin_key"), row.get("barcode", "")
                ):
                    sync_reference_identifiers_to_product(
                        db, product, imported_at=now
                    )
                    affected_ids.add(int(product["id"]))
            else:
                review += 1
                _reject_candidate(db, row)
                for product in _product_rows_for_key(
                    db, row.get("gtin_key"), row.get("barcode", "")
                ):
                    create_review_issue(
                        db, product["id"], "identifier_conflict",
                        candidate_value=(
                            f"{row.get('identifier_type', '')}: "
                            f"{row.get('identifier_value', '')}"
                        ),
                        source=row.get("source", ""),
                        source_url=row.get("source_url", ""),
                        match_method="official_identity_not_confirmed",
                        confidence=row.get("confidence", 0),
                        details={"reason": result.get("reason", "not_confirmed")},
                        created_at=now,
                    )
                    affected_ids.add(int(product["id"]))
            if (verified + review) % 25 == 0:
                db.commit()
    return {"verified": verified, "review": review, "affected_ids": affected_ids}


def _discover_online(db, batch, now, remaining_after_batch=0):
    from routes.ai import _reference_upsert, lookup_regulatory_product_online

    if not batch:
        return {
            "checked": 0, "verified": 0, "review": 0,
            "remaining": max(0, int(remaining_after_batch)),
            "affected_ids": set(),
        }

    def lookup(item):
        try:
            return item, lookup_regulatory_product_online(item["barcode"]), ""
        except Exception as exc:
            return item, None, f"{type(exc).__name__}: {exc}"[:200]

    checked = verified = review = 0
    affected_ids = set()
    checks = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(lookup, item) for item in batch]
        for future in as_completed(futures):
            if _STOP_EVENT.is_set():
                break
            item, online, lookup_error = future.result()
            checked += 1
            status = "no_labeled_identifier"
            details = {}
            if lookup_error:
                status = "transient_error"
                details = {"error": lookup_error}
            elif online:
                _reference_upsert(db, online)
                source_candidates = merge_regulatory_candidates(
                    online.get("regulatory_identifiers")
                )
                if source_candidates:
                    status = "candidate_review"
                for candidate in source_candidates:
                    result = verify_regulatory_candidate(
                        candidate, catalog_name=item.get("name", "")
                    )
                    if result.get("verified"):
                        saved, affected = _store_verified_candidate(
                            db, item, candidate, result, now
                        )
                        verified += saved
                        affected_ids.update(affected)
                        status = "verified"
                    elif (
                        result.get("reason") == "official_source_unavailable"
                        or result.get("probable")
                    ):
                        # Keep the explicitly labelled exact-UPC candidate
                        # searchable while the official service is unavailable.
                        for product in _product_rows_for_key(
                            db, item["gtin_key"], item.get("barcode", "")
                        ):
                            sync_reference_identifiers_to_product(
                                db, product, imported_at=now
                            )
                            affected_ids.add(int(product["id"]))
                        status = "candidate_review"
                    else:
                        review += 1
                        rejected_rows = db.execute(
                            """SELECT * FROM product_reference_identifiers
                               WHERE gtin_key=? AND identifier_type=?
                                 AND identifier_value=?
                                 AND verification_status='requires_review'""",
                            (
                                item["gtin_key"], candidate.get("type", ""),
                                candidate.get("value", ""),
                            ),
                        ).fetchall()
                        for rejected_row in rejected_rows:
                            _reject_candidate(
                                db, dict(rejected_row)
                            )
                details = {
                    "candidate_count": len(source_candidates),
                    "source": str(online.get("source", "") or ""),
                }
            checks.append({
                "gtin_key": item["gtin_key"], "barcode": item["barcode"],
                "status": status, "checked_at": now, "source_version": "",
                "details": details,
            })
            if checked % 20 == 0:
                _write_checks(db, _ONLINE_CHECK_SOURCE, checks)
                checks = []
                db.commit()
                _state_update(online_checked=checked)
                release_unused_memory()
    _write_checks(db, _ONLINE_CHECK_SOURCE, checks)
    remaining = max(
        0, int(remaining_after_batch) + len(batch) - checked
    )
    return {
        "checked": checked, "verified": verified, "review": review,
        "remaining": remaining, "affected_ids": affected_ids,
    }


def _regulatory_worker(force=False):
    db = None
    affected_ids = set()
    try:
        db = connect_db()
        ensure_product_data_ready(db)
        items = _catalogue_items(db)
        now = utc_now_iso()
        _state_update(
            running=True, status="running", phase="prepare", started_at=now,
            completed_at="", source_version="", catalogue_gtins=len(items),
            checked_gtins=0, exact_matches=0, verified_identifiers=0,
            review_candidates=0, conflicts=0, online_checked=0,
            remaining_online=0, error="",
        )
        _save_state(db)
        db.commit()

        _state_update(phase="read_existing_identifier_labels")
        _seed_catalogue_label_candidates(db, now)
        db.commit()

        # Health Canada removed UPC values from its DPD packaging feed in May
        # 2025. Do not download the complete feed or infer DINs by name. Exact
        # UPC sources are inspected below and each labelled identifier is then
        # checked against the appropriate official Health Canada database.
        dpd_items = {}
        dpd_result = {
            "exact_matches": 0, "verified_identifiers": 0,
            "conflicts": 0, "affected_ids": set(),
        }
        version = HEALTH_CANADA_DPD_UPC_NOTICE
        _state_update(
            checked_gtins=len(dpd_items),
            exact_matches=dpd_result["exact_matches"],
            verified_identifiers=dpd_result["verified_identifiers"],
            conflicts=dpd_result["conflicts"],
            source_version=version,
        )

        if not _STOP_EVENT.is_set():
            _state_update(phase="verify_labeled_identifiers")
            verified = _verify_candidates(db, items, now)
            affected_ids.update(verified["affected_ids"])
            _state_update(
                verified_identifiers=(
                    _state_snapshot()["verified_identifiers"] + verified["verified"]
                ),
                review_candidates=verified["review"],
            )
            db.commit()

        if not _STOP_EVENT.is_set():
            _state_update(phase="inspect_exact_upc_sources")
            already_checked = (
                set() if force else _checked_keys(db, _ONLINE_CHECK_SOURCE)
            )
            candidates = [
                item for key, item in items.items()
                if key not in already_checked and _likely_regulated(item)
            ]
            batch = candidates[:_ONLINE_BATCH_LIMIT]
            remaining_after_batch = max(0, len(candidates) - len(batch))
            del candidates
            del items
            gc.collect()
            online = _discover_online(
                db, batch, now,
                remaining_after_batch=remaining_after_batch,
            )
            affected_ids.update(online["affected_ids"])
            snapshot = _state_snapshot()
            _state_update(
                online_checked=online["checked"],
                verified_identifiers=(
                    snapshot["verified_identifiers"] + online["verified"]
                ),
                review_candidates=(
                    snapshot["review_candidates"] + online["review"]
                ),
                remaining_online=online["remaining"],
            )
            db.commit()

        if affected_ids:
            from routes.products import (
                audit_product_data,
                bump_reference_cache,
                sync_reference_metadata_to_products,
            )
            _state_update(phase="refresh_product_quality")
            sync_reference_metadata_to_products(
                db, now=utc_now_iso(), product_ids=sorted(affected_ids)
            )
            audit_product_data(
                db, sorted(affected_ids),
                trigger_type="regulatory_enrichment", employee="system",
                now=utc_now_iso(),
            )
            db.commit()
            bump_reference_cache()

        stopped = _STOP_EVENT.is_set()
        remaining = _state_snapshot()["remaining_online"]
        final_status = "interrupted" if stopped else ("partial" if remaining else "completed")
        _state_update(
            running=False, status=final_status, phase="",
            completed_at=utc_now_iso(),
        )
        _save_state(db)
        db.commit()
    except Exception as exc:
        _state_update(
            running=False, status="error", phase="",
            completed_at=utc_now_iso(),
            error=f"{type(exc).__name__}: {exc}"[:500],
        )
        if db is not None:
            try:
                db.rollback()
                _save_state(db)
                db.commit()
            except Exception:
                pass
        print(f"[Regulatory] synchronization failed: {type(exc).__name__}: {exc}")
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
        release_unused_memory()


def schedule_regulatory_enrichment(force=False):
    with _SYNC_LOCK:
        if _STATE["running"]:
            return False
        _STATE["running"] = True
        _STATE["status"] = "starting"
        _STATE["error"] = ""
    _STOP_EVENT.clear()
    threading.Thread(
        target=_regulatory_worker, args=(bool(force),), daemon=True,
        name="regulatory-enrichment",
    ).start()
    return True


def maybe_resume_regulatory_enrichment():
    if _state_snapshot()["running"]:
        return False
    try:
        db = connect_db()
        ensure_product_data_ready(db)
        row = db.execute(
            "SELECT status FROM regulatory_sync_state WHERE source=?",
            (_SYNC_SOURCE,),
        ).fetchone()
        status = str(dict(row).get("status", "") if row else "")
        db.close()
    except Exception:
        return False
    if status in {"running", "starting", "partial", "interrupted"}:
        return schedule_regulatory_enrichment(force=False)
    return False


@regulatory_bp.route("/api/product-quality/regulatory/start", methods=["POST"])
def regulatory_start():
    _username, error = require_editor()
    if error:
        return error
    data = request.get_json(silent=True) or {}
    started = schedule_regulatory_enrichment(force=bool(data.get("force")))
    return jsonify({"success": True, "started": started, **_state_snapshot()}), 202


@regulatory_bp.route("/api/product-quality/regulatory/status", methods=["GET"])
def regulatory_status():
    state = _state_snapshot()
    db = None
    if not state["running"]:
        try:
            db = get_db()
            row = db.execute(
                "SELECT * FROM regulatory_sync_state WHERE source=?",
                (_SYNC_SOURCE,),
            ).fetchone()
            if row:
                stored = dict(row)
                for key in state:
                    if key in stored and key != "running":
                        state[key] = stored[key]
        except Exception:
            pass
    try:
        db = db or get_db()
        counts = db.execute(
            """SELECT
                 SUM(CASE WHEN verification_status='verified'
                           AND identifier_type IN ('DIN','NPN','DIN_HM')
                          THEN 1 ELSE 0 END) AS confirmed,
                 SUM(CASE WHEN verification_status='requires_review'
                           AND identifier_type IN ('DIN','NPN','DIN_HM')
                           AND match_method IN ('exact_gtin_labeled_source','imported_typed_identifier')
                           AND confidence>=0.7
                          THEN 1 ELSE 0 END) AS probable
               FROM product_identifiers"""
        ).fetchone()
        values = dict(counts) if counts else {}
        state["confirmed_catalog_identifiers"] = int(
            values.get("confirmed") or 0
        )
        state["probable_catalog_identifiers"] = int(
            values.get("probable") or 0
        )
    except Exception:
        state.setdefault("confirmed_catalog_identifiers", 0)
        state.setdefault("probable_catalog_identifiers", 0)
    return jsonify({"success": True, **state})


@regulatory_bp.route("/api/product-quality/regulatory/stop", methods=["POST"])
def regulatory_stop():
    _username, error = require_editor()
    if error:
        return error
    _STOP_EVENT.set()
    return jsonify({"success": True, "stopping": True, **_state_snapshot()})
