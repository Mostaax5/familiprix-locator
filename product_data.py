"""Conservative product identity, provenance, and quality helpers.

Location data and descriptive metadata have different trust requirements. A
planogram can place a row even when its descriptive data is incomplete, but a
description or image is attached automatically only when exact package identity
and source quality are sufficient.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable


GTIN_LENGTHS = {8, 12, 13, 14}
VERIFICATION_STATUSES = {
    "verified", "unverified", "rejected", "requires_review",
}
IDENTIFIER_TYPES = {
    "GTIN", "UPC", "FAMILIPRIX_CODE", "MANUFACTURER_PART_NUMBER",
    "SUPPLIER_ITEM_NUMBER", "WHOLESALER_ITEM_NUMBER", "CASE_GTIN",
    "INNER_GTIN", "DIN", "NPN", "DIN_HM", "PIN", "NIP",
    "PSEUDO_DIN", "RAMQ_BILLING_CODE", "INSURER_BILLING_CODE",
    "HEALTH_CANADA_ID", "CLINICAL_ID",
}
AUTHORITY_REQUIRED_TYPES = {
    "PIN", "NIP", "PSEUDO_DIN", "RAMQ_BILLING_CODE",
    "INSURER_BILLING_CODE", "CLINICAL_ID",
}

FIELD_NAMES = {
    "name", "brand", "description", "image_url", "package_size",
    "package_unit", "variant", "flavour", "colour", "strength",
    "dosage_form", "manufacturer", "category", "ingredients",
    "compatibility", "purpose", "route_of_administration",
    "official_name_fr", "official_name_en",
}

REFERENCE_FIELDS = (
    "name", "brand", "description", "image_url", "product_code",
    "package_size", "package_unit", "variant", "flavour", "colour",
    "strength", "dosage_form", "manufacturer", "category", "ingredients",
    "compatibility", "purpose", "route_of_administration",
    "official_name_fr", "official_name_en",
)

SOURCE_PRIORITIES = {
    "store_catalog": 100,
    "manufacturer": 90,
    "supplier": 80,
    "gs1": 75,
    "health_canada": 70,
    "exact_retailer": 60,
    "manual": 55,
    "open_database": 35,
    "general_web": 15,
    "ai_suggestion": 5,
    "unknown": 0,
}

AUTO_APPLY_SOURCE_TYPES = {
    "store_catalog", "manufacturer", "supplier", "gs1",
    "health_canada", "exact_retailer", "manual",
}

_GENERIC_NAME_WORDS = {
    "avec", "and", "aux", "the", "pour", "sans", "produit", "product",
    "format", "nouveau", "new", "extra", "plus", "original", "regulier",
    "regular", "un", "une", "de", "des", "du", "la", "le", "les",
}
_BRAND_NOISE = {"inc", "ltd", "limited", "corp", "corporation", "co", "canada"}

_UNIT_ALIASES = {
    "mcg": "mcg", "ug": "mcg", "microgramme": "mcg", "microgrammes": "mcg",
    "mg": "mg", "g": "g", "kg": "kg", "ml": "ml", "l": "l",
    "oz": "oz", "fl oz": "fl_oz", "%": "%",
}
_COUNT_WORDS = {
    "comprime": "tablet", "comprimes": "tablet", "tablet": "tablet",
    "tablets": "tablet", "caplet": "tablet", "caplets": "tablet",
    "capsule": "capsule", "capsules": "capsule", "gelule": "capsule",
    "gelules": "capsule", "dose": "dose", "doses": "dose",
    "unite": "unit", "unites": "unit", "unit": "unit", "units": "unit",
    "sachet": "sachet", "sachets": "sachet", "piece": "unit", "pieces": "unit",
    "lingette": "wipe", "lingettes": "wipe", "wipe": "wipe", "wipes": "wipe",
}
_FORMAT_GROUPS = (
    {"shampoo", "shampoing"},
    {"conditioner", "revitalisant", "apres shampoing"},
    {"refill", "recharge", "replacement", "remplacement"},
    {"device", "appareil", "machine", "brosse electrique"},
    {"liquid", "liquide", "suspension", "sirop"},
    {"tablet", "tablette", "comprime", "caplet"},
    {"capsule", "gelule", "liqui gel", "liquigel"},
    {"cream", "creme"},
    {"lotion"},
    {"spray", "vaporisateur"},
)
_FLAVOURS = {
    "menthe", "mint", "raisin", "grape", "fraise", "strawberry", "cerise",
    "cherry", "orange", "citron", "lemon", "lime", "framboise", "raspberry",
    "vanille", "vanilla", "chocolat", "chocolate", "original", "sans saveur",
    "unflavoured", "unflavored",
}
_COLOURS = {
    "rouge", "red", "bleu", "blue", "vert", "green", "noir", "black",
    "blanc", "white", "rose", "pink", "brun", "brown", "gris", "grey", "gray",
    "violet", "purple", "orange", "jaune", "yellow",
}


def text_digits(value) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_text(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9%]+", " ", text.lower()).strip()


def gtin_check_digit_valid(value) -> bool:
    digits = text_digits(value)
    if len(digits) not in GTIN_LENGTHS:
        return False
    body = digits[:-1]
    expected = int(digits[-1])
    total = sum(
        int(digit) * (3 if index % 2 == 0 else 1)
        for index, digit in enumerate(reversed(body))
    )
    return (10 - (total % 10)) % 10 == expected


def canonical_gtin(value, require_valid=True) -> str:
    digits = text_digits(value)
    if len(digits) not in GTIN_LENGTHS:
        return ""
    if require_valid and not gtin_check_digit_valid(digits):
        return ""
    return digits.zfill(14)


def gtin_identity_key(value) -> str:
    """Canonical exact-package key without discarding meaningful zeroes.

    Valid GTIN-8/12/13/14 representations share one zero-padded key. Invalid or
    non-standard store codes remain exact-only and can never collide after an
    arbitrary lstrip operation.
    """
    digits = text_digits(value)
    canonical = canonical_gtin(digits, require_valid=True)
    if canonical:
        return f"gtin:{canonical}"
    return f"raw:{digits}" if digits else ""


def exact_gtin_variants(value) -> list[str]:
    """Representations that are mathematically the same GTIN, never partials."""
    digits = text_digits(value)
    key = gtin_identity_key(digits)
    if not key:
        return []
    if key.startswith("raw:"):
        return [digits]
    canonical = key.split(":", 1)[1]
    values = []
    for length in (len(digits), 14, 13, 12, 8):
        if length not in GTIN_LENGTHS or length > 14:
            continue
        prefix = canonical[:14 - length]
        candidate = canonical[14 - length:]
        if set(prefix) <= {"0"} and gtin_check_digit_valid(candidate) and candidate not in values:
            values.append(candidate)
    if digits not in values:
        values.insert(0, digits)
    return values


def normalize_identifier(identifier_type, value, authority="") -> str:
    identifier_type = str(identifier_type or "").strip().upper().replace("-", "_")
    raw = str(value or "").strip()
    if not raw:
        return ""
    if identifier_type in {"GTIN", "UPC", "CASE_GTIN", "INNER_GTIN"}:
        return gtin_identity_key(raw)
    if identifier_type in {"DIN", "NPN", "DIN_HM"}:
        digits = text_digits(raw)
        return digits if len(digits) == 8 else ""
    normalized = unicodedata.normalize("NFKC", raw).upper()
    normalized = re.sub(r"[\s\-]+", "", normalized)
    if identifier_type in AUTHORITY_REQUIRED_TYPES and not str(authority or "").strip():
        return ""
    return normalized


def classify_source(source="", source_url="") -> tuple[str, int]:
    value = normalize_text(f"{source} {source_url}")
    if any(token in value for token in (
        "familiprix", "planogramme", "store catalogue", "catalogue magasin",
        "fiche magasin", "store product",
    )):
        source_type = "store_catalog"
    elif any(token in value for token in ("manufacturer", "fabricant")):
        source_type = "manufacturer"
    elif any(token in value for token in ("supplier", "fournisseur", "wholesaler", "grossiste")):
        source_type = "supplier"
    elif "gs1" in value:
        source_type = "gs1"
    elif any(token in value for token in ("health canada", "sante canada", "canada ca", "drug product database")):
        source_type = "health_canada"
    elif any(token in value for token in ("jean coutu", "brunet", "pharmaprix", "retailer")):
        source_type = "exact_retailer"
    elif any(token in value for token in ("manual", "manuel", "employee", "employe", "manager", "pharmacist")):
        source_type = "manual"
    elif any(token in value for token in ("open food facts", "open beauty facts", "open products facts", "open drug facts", "upc item db", "datakick", "brocade")):
        source_type = "open_database"
    elif any(token in value for token in ("recherche ia", "ai suggestion", "deepseek", "gemini")):
        source_type = "ai_suggestion"
    elif value:
        source_type = "general_web"
    else:
        source_type = "unknown"
    return source_type, SOURCE_PRIORITIES[source_type]


def _decimal(value) -> str:
    raw = str(value or "").replace(",", ".")
    try:
        number = float(raw)
    except ValueError:
        return raw
    return str(int(number)) if number.is_integer() else (f"{number:.4f}".rstrip("0").rstrip("."))


def extract_package_facts(*values) -> dict[str, set[str]]:
    text = normalize_text(" ".join(str(value or "") for value in values))
    facts = {
        "sizes": set(), "counts": set(), "strengths": set(), "formats": set(),
        "flavours": set(), "colours": set(),
    }
    for number, unit in re.findall(r"\b(\d+(?:[.,]\d+)?)\s*(mcg|ug|mg|kg|ml|g|l|oz|%)\b", text):
        normalized = f"{_decimal(number)}:{_UNIT_ALIASES.get(unit, unit)}"
        facts["sizes"].add(normalized)
    for number, unit in re.findall(
        r"\b(\d+)\s*(comprimes?|tablets?|caplets?|capsules?|gelules?|doses?|unites?|units?|sachets?|pieces?|lingettes?|wipes?)\b",
        text,
    ):
        facts["counts"].add(f"{int(number)}:{_COUNT_WORDS.get(unit, unit)}")
    for count in re.findall(r"\b(?:ca|co|caps?|ct|x)\s*(\d{1,4})\b", text):
        facts["counts"].add(f"{int(count)}:unit")
    for left, left_unit, right, right_unit in re.findall(
        r"\b(\d+(?:[.,]\d+)?)\s*(mcg|ug|mg|g|%)\s*(?:/|par)\s*(\d+(?:[.,]\d+)?)?\s*(ml|l|dose)?\b",
        text,
    ):
        facts["strengths"].add(
            f"{_decimal(left)}:{_UNIT_ALIASES.get(left_unit, left_unit)}/"
            f"{_decimal(right or '1')}:{_UNIT_ALIASES.get(right_unit or 'dose', right_unit or 'dose')}"
        )
    def contains_term(term):
        return bool(re.search(rf"(?:^| ){re.escape(term)}(?: |$)", text))

    for group in _FORMAT_GROUPS:
        for term in group:
            if contains_term(term):
                facts["formats"].add(term)
    facts["flavours"] = {term for term in _FLAVOURS if contains_term(term)}
    facts["colours"] = {term for term in _COLOURS if re.search(rf"\b{re.escape(term)}\b", text)}
    return facts


def _tokens(value, noise=()) -> set[str]:
    return {
        token for token in normalize_text(value).split()
        if len(token) >= 3 and token not in set(noise)
    }


def _token_sets_overlap(left: Iterable[str], right: Iterable[str]) -> bool:
    for a in left:
        for b in right:
            if a == b or (len(a) >= 4 and len(b) >= 4 and (a.startswith(b) or b.startswith(a))):
                return True
    return False


def _brand_conflicts(left, right) -> bool:
    left_tokens = _tokens(left, _BRAND_NOISE)
    right_tokens = _tokens(right, _BRAND_NOISE)
    return bool(left_tokens and right_tokens and not _token_sets_overlap(left_tokens, right_tokens))


def _name_conflicts(left, right) -> bool:
    left_tokens = _tokens(left, _GENERIC_NAME_WORDS)
    right_tokens = _tokens(right, _GENERIC_NAME_WORDS)
    if not left_tokens or not right_tokens:
        return False
    return not _token_sets_overlap(left_tokens, right_tokens)


def _set_conflict(left: set[str], right: set[str]) -> bool:
    return bool(left and right and left.isdisjoint(right))


def _count_conflict(left: set[str], right: set[str]) -> bool:
    """CO50 and 50 tablets describe the same count despite different labels."""
    if not left or not right:
        return False
    left_counts = {value.split(":", 1)[0] for value in left}
    right_counts = {value.split(":", 1)[0] for value in right}
    return left_counts.isdisjoint(right_counts)


@dataclass
class CandidateAssessment:
    accepted: bool
    auto_apply: bool
    verification_status: str
    source_type: str
    source_priority: int
    confidence: float
    issues: list[dict] = field(default_factory=list)


def assess_metadata_candidate(existing, candidate, match_method="exact_gtin") -> CandidateAssessment:
    existing = dict(existing or {})
    candidate = dict(candidate or {})
    source_type, source_priority = classify_source(
        candidate.get("source", ""), candidate.get("source_url", "")
    )
    issues = []
    existing_key = gtin_identity_key(existing.get("barcode", ""))
    candidate_key = gtin_identity_key(candidate.get("barcode", ""))

    exact_identifier = False
    if match_method == "exact_gtin":
        exact_identifier = bool(existing_key and candidate_key and existing_key == candidate_key)
        if not candidate_key:
            issues.append({"type": "upc_conflict", "field": "barcode", "reason": "candidate_missing_exact_gtin"})
        elif existing_key != candidate_key:
            issues.append({"type": "upc_conflict", "field": "barcode", "reason": "different_gtin"})
        elif not canonical_gtin(existing.get("barcode", ""), require_valid=True):
            issues.append({"type": "upc_conflict", "field": "barcode", "reason": "invalid_gtin_check_digit"})
    elif match_method in {"exact_familiprix_code", "exact_manufacturer_part", "exact_supplier_code"}:
        exact_identifier = True
    else:
        issues.append({"type": "multiple_possible_matches", "field": "identity", "reason": "non_deterministic_match"})

    if _brand_conflicts(existing.get("brand"), candidate.get("brand")):
        issues.append({"type": "brand_conflict", "field": "brand", "reason": "different_brand"})

    existing_facts = extract_package_facts(
        existing.get("name"), existing.get("description"), existing.get("package_size"),
        existing.get("strength"), existing.get("variant"), existing.get("flavour"),
        existing.get("colour"), existing.get("dosage_form"),
    )
    candidate_facts = extract_package_facts(
        candidate.get("name"), candidate.get("description"), candidate.get("package_size"),
        candidate.get("strength"), candidate.get("variant"), candidate.get("flavour"),
        candidate.get("colour"), candidate.get("dosage_form"),
    )
    for fact_name, issue_type in (
        ("sizes", "package_size_conflict"),
        ("strengths", "strength_conflict"),
        ("flavours", "variant_conflict"),
        ("colours", "variant_conflict"),
    ):
        if _set_conflict(existing_facts[fact_name], candidate_facts[fact_name]):
            issues.append({"type": issue_type, "field": fact_name, "reason": "different_package_attribute"})
    if _count_conflict(existing_facts["counts"], candidate_facts["counts"]):
        issues.append({
            "type": "package_size_conflict", "field": "counts",
            "reason": "different_package_attribute",
        })

    if existing_facts["formats"] and candidate_facts["formats"]:
        existing_groups = {
            index for index, group in enumerate(_FORMAT_GROUPS)
            if existing_facts["formats"] & group
        }
        candidate_groups = {
            index for index, group in enumerate(_FORMAT_GROUPS)
            if candidate_facts["formats"] & group
        }
        if existing_groups and candidate_groups and existing_groups.isdisjoint(candidate_groups):
            issues.append({"type": "format_conflict", "field": "dosage_form", "reason": "different_format"})

    if (
        source_type not in {"store_catalog", "manual"}
        and _name_conflicts(existing.get("name"), candidate.get("name"))
    ):
        issues.append({"type": "product_name_conflict", "field": "name", "reason": "no_meaningful_name_overlap"})

    hard_conflict = bool(issues)
    trusted = source_type in AUTO_APPLY_SOURCE_TYPES
    accepted = exact_identifier and not hard_conflict
    auto_apply = accepted and trusted
    verification_status = "verified" if auto_apply else ("requires_review" if accepted or hard_conflict else "unverified")
    confidence = 1.0 if auto_apply else (0.75 if accepted else 0.0)
    return CandidateAssessment(
        accepted=accepted,
        auto_apply=auto_apply,
        verification_status=verification_status,
        source_type=source_type,
        source_priority=source_priority,
        confidence=confidence,
        issues=issues,
    )


def upsert_product_identifier(
    db, product_id, identifier_type, value, *, authority="", source="",
    source_url="", source_record_id="", match_method="imported",
    confidence=1.0, verification_status="unverified", is_primary=False,
    package_level="sellable_unit", imported_at="", last_verified_at="",
):
    identifier_type = str(identifier_type or "").strip().upper().replace("-", "_")
    if identifier_type not in IDENTIFIER_TYPES:
        return False
    authority = str(authority or "").strip()
    normalized = normalize_identifier(identifier_type, value, authority)
    if not normalized:
        return False
    status = verification_status if verification_status in VERIFICATION_STATUSES else "unverified"
    try:
        db.execute(
            """INSERT INTO product_identifiers
               (product_id, identifier_type, identifier_value, normalized_value,
                authority, is_primary, package_level, source, source_url,
                source_record_id, match_method, confidence, verification_status,
                imported_at, last_verified_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(product_id, identifier_type, normalized_value, authority)
               DO UPDATE SET
                 identifier_value=excluded.identifier_value,
                 is_primary=CASE WHEN excluded.is_primary=1 THEN 1 ELSE product_identifiers.is_primary END,
                 source=CASE WHEN excluded.confidence >= product_identifiers.confidence THEN excluded.source ELSE product_identifiers.source END,
                 source_url=CASE WHEN excluded.confidence >= product_identifiers.confidence THEN excluded.source_url ELSE product_identifiers.source_url END,
                 source_record_id=CASE WHEN excluded.confidence >= product_identifiers.confidence THEN excluded.source_record_id ELSE product_identifiers.source_record_id END,
                 match_method=CASE WHEN excluded.confidence >= product_identifiers.confidence THEN excluded.match_method ELSE product_identifiers.match_method END,
                 confidence=CASE WHEN excluded.confidence > product_identifiers.confidence THEN excluded.confidence ELSE product_identifiers.confidence END,
                 verification_status=CASE
                   WHEN excluded.verification_status='verified' THEN 'verified'
                   WHEN product_identifiers.verification_status='verified' THEN 'verified'
                   WHEN product_identifiers.verification_status='rejected' THEN 'rejected'
                   ELSE excluded.verification_status END,
                 last_verified_at=CASE WHEN excluded.last_verified_at<>'' THEN excluded.last_verified_at ELSE product_identifiers.last_verified_at END""",
            (
                int(product_id), identifier_type, str(value or "").strip(), normalized,
                authority, 1 if is_primary else 0, str(package_level or "sellable_unit")[:40],
                str(source or "")[:160], str(source_url or "")[:2048],
                str(source_record_id or "")[:240], str(match_method or "")[:80],
                float(max(0.0, min(float(confidence), 1.0))), status,
                str(imported_at or "")[:64], str(last_verified_at or "")[:64],
            ),
        )
        return True
    except Exception:
        return False


def upsert_reference_identifier(
    db, barcode, identifier_type, value, *, authority="", source="",
    source_url="", source_record_id="", match_method="exact_gtin",
    confidence=0.0, verification_status="unverified", imported_at="",
    last_verified_at="",
):
    """Store an identifier against an exact package before it is placed.

    Verified package identifiers can then follow the UPC into every planogram
    location without relying on names or repeating a network lookup.
    """
    identifier_type = str(identifier_type or "").strip().upper().replace("-", "_")
    if identifier_type not in IDENTIFIER_TYPES:
        return False
    authority = str(authority or "").strip()
    normalized = normalize_identifier(identifier_type, value, authority)
    key = gtin_identity_key(barcode)
    if not key or not normalized:
        return False
    status = verification_status if verification_status in VERIFICATION_STATUSES else "unverified"
    try:
        db.execute(
            """INSERT INTO product_reference_identifiers
               (gtin_key, barcode, identifier_type, identifier_value,
                normalized_value, authority, source, source_url,
                source_record_id, match_method, confidence,
                verification_status, imported_at, last_verified_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(gtin_key, identifier_type, normalized_value, authority, source_record_id)
               DO UPDATE SET
                 barcode=excluded.barcode,
                 source=CASE WHEN excluded.confidence >= product_reference_identifiers.confidence THEN excluded.source ELSE product_reference_identifiers.source END,
                 source_url=CASE WHEN excluded.confidence >= product_reference_identifiers.confidence THEN excluded.source_url ELSE product_reference_identifiers.source_url END,
                 match_method=CASE WHEN excluded.confidence >= product_reference_identifiers.confidence THEN excluded.match_method ELSE product_reference_identifiers.match_method END,
                 confidence=CASE WHEN excluded.confidence > product_reference_identifiers.confidence THEN excluded.confidence ELSE product_reference_identifiers.confidence END,
                 verification_status=CASE
                   WHEN excluded.verification_status='verified' THEN 'verified'
                   WHEN product_reference_identifiers.verification_status='verified' THEN 'verified'
                   WHEN product_reference_identifiers.verification_status='rejected' THEN 'rejected'
                   ELSE excluded.verification_status END,
                 last_verified_at=CASE WHEN excluded.last_verified_at<>'' THEN excluded.last_verified_at ELSE product_reference_identifiers.last_verified_at END""",
            (
                key, str(barcode or "").strip(), identifier_type,
                str(value or "").strip(), normalized, authority,
                str(source or "")[:160], str(source_url or "")[:2048],
                str(source_record_id or "")[:240], str(match_method or "")[:80],
                float(max(0.0, min(float(confidence), 1.0))), status,
                str(imported_at or "")[:64], str(last_verified_at or "")[:64],
            ),
        )
        return True
    except Exception:
        return False


def reference_identifiers_for_barcode(db, barcode, statuses=("verified",)):
    key = gtin_identity_key(barcode)
    allowed = tuple(
        status for status in statuses if status in VERIFICATION_STATUSES
    )
    if not key or not allowed:
        return []
    placeholders = ",".join("?" for _ in allowed)
    try:
        return [dict(row) for row in db.execute(
            f"""SELECT * FROM product_reference_identifiers
                WHERE gtin_key=? AND verification_status IN ({placeholders})
                ORDER BY confidence DESC, id""",
            (key,) + allowed,
        ).fetchall()]
    except Exception:
        return []


def sync_reference_identifiers_to_product(db, product, *, imported_at=""):
    """Copy useful identifiers and clearly flagged candidates to a product.

    Official matches remain ``verified``.  An explicitly labelled DIN/NPN/
    DIN-HM found on a page for the exact UPC is also copied as
    ``requires_review``. A real Health Canada name candidate may also be copied
    at lower confidence. Employees can search both immediately, while the UI
    and AI continue to distinguish candidates from confirmed regulatory facts.
    """
    item = dict(product or {})
    product_id = item.get("id")
    barcode = str(item.get("barcode", "") or "").strip()
    if not product_id or not barcode:
        return 0
    copied = 0
    for reference in reference_identifiers_for_barcode(
        db, barcode, statuses=("verified", "requires_review")
    ):
        status = str(reference.get("verification_status", "") or "")
        identifier_type = str(reference.get("identifier_type", "") or "")
        match_method = str(reference.get("match_method", "") or "")
        if status == "requires_review":
            confidence = float(reference.get("confidence", 0) or 0)
            allowed_candidate = (
                identifier_type in {"DIN", "NPN", "DIN_HM"}
                and (
                    (match_method in {
                        "exact_gtin_labeled_source", "imported_typed_identifier",
                    } and confidence >= 0.7)
                    or (
                        match_method == "health_canada_name_candidate"
                        and confidence >= 0.25
                    )
                )
            )
            if not allowed_candidate:
                continue
        if upsert_product_identifier(
            db, product_id, identifier_type,
            reference.get("identifier_value", ""),
            authority=reference.get("authority", ""),
            source=reference.get("source", ""),
            source_url=reference.get("source_url", ""),
            source_record_id=reference.get("source_record_id", ""),
            match_method=reference.get("match_method", "exact_gtin"),
            confidence=reference.get("confidence", 0),
            verification_status=status,
            imported_at=imported_at,
            last_verified_at=(
                reference.get("last_verified_at", "") or imported_at
                if status == "verified" else ""
            ),
            package_level=(
                "regulated_product" if reference.get("identifier_type")
                in {"DIN", "NPN", "DIN_HM", "HEALTH_CANADA_ID"}
                else "sellable_unit"
            ),
        ):
            copied += 1
    return copied


def record_field_evidence(
    db, product_id, field_name, field_value, *, source="", source_url="",
    source_record_id="", match_method="", confidence=0.0,
    verification_status="unverified", imported_at="", last_verified_at="",
    active=False,
):
    field_name = str(field_name or "").strip()
    value = str(field_value or "").strip()
    if field_name not in FIELD_NAMES or not value:
        return False
    source_type, priority = classify_source(source, source_url)
    status = verification_status if verification_status in VERIFICATION_STATUSES else "unverified"
    try:
        if active and str(match_method or "") != "manual_review":
            current = db.execute(
                """SELECT source_priority FROM product_field_evidence
                   WHERE product_id=? AND field_name=? AND active=1
                     AND verification_status='verified'
                   ORDER BY source_priority DESC, confidence DESC, id DESC LIMIT 1""",
                (int(product_id), field_name),
            ).fetchone()
            current_priority = (
                int(current["source_priority"] if isinstance(current, dict) else current[0])
                if current else -1
            )
            if current_priority > priority:
                active = False
        if active:
            db.execute(
                "UPDATE product_field_evidence SET active=0 WHERE product_id=? AND field_name=?",
                (int(product_id), field_name),
            )
        db.execute(
            """INSERT INTO product_field_evidence
               (product_id, field_name, field_value, source, source_type,
                source_priority, source_url, source_record_id, match_method,
                confidence, verification_status, imported_at, last_verified_at, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(product_id, field_name, field_value, source, source_record_id)
               DO UPDATE SET source_priority=excluded.source_priority,
                 source_url=excluded.source_url, match_method=excluded.match_method,
                 confidence=CASE WHEN excluded.confidence > product_field_evidence.confidence THEN excluded.confidence ELSE product_field_evidence.confidence END,
                 verification_status=CASE WHEN product_field_evidence.verification_status IN ('verified','rejected') THEN product_field_evidence.verification_status ELSE excluded.verification_status END,
                 last_verified_at=CASE WHEN excluded.last_verified_at<>'' THEN excluded.last_verified_at ELSE product_field_evidence.last_verified_at END,
                 active=CASE WHEN product_field_evidence.verification_status='rejected' THEN 0 WHEN excluded.active=1 THEN 1 ELSE product_field_evidence.active END""",
            (
                int(product_id), field_name, value, str(source or "")[:160], source_type,
                priority, str(source_url or "")[:2048], str(source_record_id or "")[:240],
                str(match_method or "")[:80], float(max(0.0, min(float(confidence), 1.0))),
                status, str(imported_at or "")[:64], str(last_verified_at or "")[:64],
                1 if active else 0,
            ),
        )
        return True
    except Exception:
        return False


def record_reference_evidence(
    db, barcode, field_name, field_value, *, source="", source_url="",
    source_record_id="", match_method="exact_gtin", confidence=0.0,
    verification_status="unverified", imported_at="", last_verified_at="",
    active=False,
):
    key = gtin_identity_key(barcode)
    value = str(field_value or "").strip()
    if not key or field_name not in REFERENCE_FIELDS or not value:
        return False
    source_type, priority = classify_source(source, source_url)
    status = verification_status if verification_status in VERIFICATION_STATUSES else "unverified"
    try:
        if active and str(match_method or "") != "manual_review":
            current = db.execute(
                """SELECT source_priority FROM product_reference_evidence
                   WHERE gtin_key=? AND field_name=? AND active=1
                     AND verification_status='verified'
                   ORDER BY source_priority DESC, confidence DESC, id DESC LIMIT 1""",
                (key, field_name),
            ).fetchone()
            current_priority = (
                int(current["source_priority"] if isinstance(current, dict) else current[0])
                if current else -1
            )
            if current_priority > priority:
                active = False
        if active:
            db.execute(
                "UPDATE product_reference_evidence SET active=0 WHERE gtin_key=? AND field_name=?",
                (key, field_name),
            )
        db.execute(
            """INSERT INTO product_reference_evidence
               (gtin_key, barcode, field_name, field_value, source, source_type,
                source_priority, source_url, source_record_id, match_method,
                confidence, verification_status, imported_at, last_verified_at, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(gtin_key, field_name, field_value, source, source_record_id)
               DO UPDATE SET source_priority=excluded.source_priority,
                 source_url=excluded.source_url, match_method=excluded.match_method,
                 confidence=CASE WHEN excluded.confidence > product_reference_evidence.confidence THEN excluded.confidence ELSE product_reference_evidence.confidence END,
                 verification_status=CASE WHEN product_reference_evidence.verification_status IN ('verified','rejected') THEN product_reference_evidence.verification_status ELSE excluded.verification_status END,
                 last_verified_at=CASE WHEN excluded.last_verified_at<>'' THEN excluded.last_verified_at ELSE product_reference_evidence.last_verified_at END,
                 active=CASE WHEN product_reference_evidence.verification_status='rejected' THEN 0 WHEN excluded.active=1 THEN 1 ELSE product_reference_evidence.active END""",
            (
                key, str(barcode or "").strip(), field_name, value,
                str(source or "")[:160], source_type, priority,
                str(source_url or "")[:2048], str(source_record_id or "")[:240],
                str(match_method or "")[:80], float(max(0.0, min(float(confidence), 1.0))),
                status, str(imported_at or "")[:64], str(last_verified_at or "")[:64],
                1 if active else 0,
            ),
        )
        return True
    except Exception:
        return False


def upsert_reference_candidate(db, candidate, *, imported_at=""):
    """Store one exact-package catalogue candidate without field guessing.

    Conflicting values are retained as evidence and returned to the caller for
    review. Existing non-empty values are never overwritten automatically.
    """
    item = dict(candidate or {})
    barcode = str(item.get("barcode", "") or "").strip()
    key = gtin_identity_key(barcode)
    if not key:
        return {"stored": False, "reason": "missing_identifier", "issues": []}
    source = str(item.get("source", "") or "")
    source_url = str(item.get("source_url", "") or "")
    source_type, source_priority = classify_source(source, source_url)
    rows = []
    for value in exact_gtin_variants(barcode):
        row = db.execute("SELECT * FROM product_reference WHERE barcode=?", (value,)).fetchone()
        if row:
            rows.append(dict(row))
    existing = next((row for row in rows if str(row.get("barcode", "")) == barcode), None)
    if existing is None and len(rows) == 1:
        existing = rows[0]
    anchor = existing or {"barcode": barcode, "name": item.get("name", ""), "brand": item.get("brand", "")}
    assessment = assess_metadata_candidate(anchor, item, match_method="exact_gtin")
    valid = bool(canonical_gtin(barcode, require_valid=True))
    can_verify = valid and assessment.auto_apply
    evidence_status = "verified" if can_verify else "requires_review"
    source_record_id = str(item.get("source_record_id", "") or item.get("product_code", "") or barcode)

    conflicts = []
    for field in REFERENCE_FIELDS:
        incoming = str(item.get(field, "") or "").strip()
        if not incoming:
            continue
        current = str((existing or {}).get(field, "") or "").strip()
        active = bool(can_verify and (not current or current == incoming))
        record_reference_evidence(
            db, barcode, field, incoming, source=source, source_url=source_url,
            source_record_id=source_record_id, match_method="exact_gtin",
            confidence=assessment.confidence, verification_status=evidence_status,
            imported_at=imported_at, last_verified_at=imported_at if can_verify else "",
            active=active,
        )
        if current and current != incoming:
            conflicts.append({
                "type": "multiple_possible_matches", "field": field,
                "reason": "conflicting_reference_value", "existing": current,
                "candidate": incoming,
            })

    all_issues = list(assessment.issues) + conflicts
    verification = "verified" if can_verify and not all_issues else "requires_review"
    confidence = assessment.confidence if valid else min(assessment.confidence, 0.5)
    if existing:
        updates = {}
        for field in REFERENCE_FIELDS:
            incoming = str(item.get(field, "") or "").strip()
            current = str(existing.get(field, "") or "").strip()
            if incoming and not current and can_verify:
                updates[field] = incoming
        updates.update({
            "gtin_key": key,
            "match_method": "exact_gtin",
            "verification_status": verification,
            "source_priority": max(int(existing.get("source_priority") or 0), source_priority),
            "confidence": max(float(existing.get("confidence") or 0), confidence),
            "updated_at": imported_at,
        })
        presence = str(item.get("store_presence_status", "") or "").strip()
        if presence:
            updates["store_presence_status"] = presence
        if can_verify and not all_issues and source_priority >= int(existing.get("source_priority") or 0):
            updates["source"] = source
            updates["source_url"] = source_url
            updates["last_verified_at"] = imported_at
        assignments = ", ".join(f"{field}=?" for field in updates)
        db.execute(
            f"UPDATE product_reference SET {assignments} WHERE barcode=?",
            tuple(updates.values()) + (existing["barcode"],),
        )
        stored_barcode = existing["barcode"]
    else:
        fields = list(REFERENCE_FIELDS)
        values = [str(item.get(field, "") or "").strip() for field in fields]
        columns = ["barcode"] + fields + [
            "source", "source_url", "updated_at", "gtin_key", "match_method",
            "verification_status", "source_priority", "confidence", "last_verified_at",
            "store_presence_status",
        ]
        values = [barcode] + values + [
            source, source_url, imported_at, key, "exact_gtin", verification,
            source_priority, confidence, imported_at if verification == "verified" else "",
            str(item.get("store_presence_status", "") or ""),
        ]
        placeholders = ",".join("?" for _ in columns)
        db.execute(
            f"INSERT INTO product_reference ({','.join(columns)}) VALUES ({placeholders})",
            tuple(values),
        )
        stored_barcode = barcode
    return {
        "stored": True, "barcode": stored_barcode, "gtin_key": key,
        "verification_status": verification, "source_type": source_type,
        "source_priority": source_priority, "confidence": confidence,
        "issues": all_issues,
    }


def active_field_evidence(db, product_id, field_name):
    try:
        row = db.execute(
            """SELECT * FROM product_field_evidence
               WHERE product_id=? AND field_name=? AND active=1
               ORDER BY source_priority DESC, confidence DESC, id DESC LIMIT 1""",
            (int(product_id), str(field_name)),
        ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


def field_evidence_for_value(db, product_id, field_name, field_value):
    try:
        row = db.execute(
            """SELECT * FROM product_field_evidence
               WHERE product_id=? AND field_name=? AND field_value=?
               ORDER BY source_priority DESC, confidence DESC, id DESC LIMIT 1""",
            (int(product_id), str(field_name), str(field_value or "").strip()),
        ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


def create_review_issue(
    db, product_id, issue_type, *, field_name="", existing_value="",
    candidate_value="", source="", source_url="", match_method="",
    confidence=0.0, details=None, created_at="",
):
    try:
        existing = db.execute(
            """SELECT id FROM product_data_issues
               WHERE product_id=? AND issue_type=? AND field_name=? AND status='open'
               LIMIT 1""",
            (int(product_id), str(issue_type)[:80], str(field_name)[:80]),
        ).fetchone()
        payload = json.dumps(details or {}, ensure_ascii=True, separators=(",", ":"))[:4000]
        if existing:
            issue_id = existing["id"] if isinstance(existing, dict) else existing[0]
            db.execute(
                """UPDATE product_data_issues SET existing_value=?, candidate_value=?,
                   source=?, source_url=?, match_method=?, confidence=?, details_json=?
                   WHERE id=?""",
                (
                    str(existing_value or "")[:6000], str(candidate_value or "")[:6000],
                    str(source or "")[:160], str(source_url or "")[:2048],
                    str(match_method or "")[:80], float(max(0.0, min(float(confidence), 1.0))),
                    payload, issue_id,
                ),
            )
            return int(issue_id)
        cursor = db.execute(
            """INSERT INTO product_data_issues
               (product_id, issue_type, field_name, existing_value, candidate_value,
                source, source_url, match_method, confidence, status, details_json,
                created_at, resolved_at, resolved_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, '', '')""",
            (
                int(product_id), str(issue_type)[:80], str(field_name)[:80],
                str(existing_value or "")[:6000], str(candidate_value or "")[:6000],
                str(source or "")[:160], str(source_url or "")[:2048],
                str(match_method or "")[:80], float(max(0.0, min(float(confidence), 1.0))),
                payload, str(created_at or "")[:64],
            ),
        )
        return int(getattr(cursor, "lastrowid", 0) or 0)
    except Exception:
        return 0


def sync_basic_aliases(db, product_id, product, source="planogram", verified=False):
    aliases = []
    for alias_type, key, language in (
        ("official_name", "name", "fr"),
        ("brand", "brand", ""),
        ("french_name", "name_fr", "fr"),
        ("english_name", "name_en", "en"),
        ("employee_short_name", "short_name", "fr"),
    ):
        value = str((product or {}).get(key, "") or "").strip()
        if value:
            aliases.append((alias_type, value, language))
    for key, alias_type in (("aliases", "common_name"), ("misspellings", "misspelling"), ("keywords", "keyword")):
        raw = (product or {}).get(key, [])
        values = raw if isinstance(raw, list) else re.split(r"[,;|]", str(raw or ""))
        aliases.extend((alias_type, str(value).strip(), "") for value in values if str(value).strip())
    inserted = 0
    for alias_type, value, language in aliases:
        normalized = normalize_text(value)
        if not normalized:
            continue
        try:
            db.execute(
                """INSERT INTO product_aliases
                   (product_id, alias_type, alias_value, normalized_value, language,
                    source, confidence, verification_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(product_id, alias_type, normalized_value)
                   DO UPDATE SET source=excluded.source,
                     confidence=CASE WHEN excluded.confidence > product_aliases.confidence THEN excluded.confidence ELSE product_aliases.confidence END,
                     verification_status=CASE WHEN product_aliases.verification_status IN ('verified','rejected') THEN product_aliases.verification_status ELSE excluded.verification_status END""",
                (
                    int(product_id), alias_type, value[:500], normalized[:500], language,
                    str(source or "")[:160], 1.0 if verified else 0.7,
                    "verified" if verified else "unverified",
                ),
            )
            inserted += 1
        except Exception:
            return inserted
    return inserted
