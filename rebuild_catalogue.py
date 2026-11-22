"""Prepare a reversible, UPC-preserving clean catalogue from an exported backup.

No database connection and no network access. The original bytes, cleaned import,
quarantined values and checksums are written to a NEW directory. Manual verified
field evidence is retained; automatic media and generated metadata are reviewed
again rather than carried into the rebuilt catalogue as facts.
"""

import argparse
import copy
import hashlib
import json
from pathlib import Path

from product_data import FIELD_NAMES, gtin_identity_key


RESET_FIELDS = (FIELD_NAMES - {"name"}) | {
    "search_terms", "usage_notes", "alternative_suggestions", "source_url",
}


def rebuild(payload):
    if not isinstance(payload, dict) or payload.get("export_version") not in {1, 2}:
        raise ValueError("A version 1 or 2 Familiprix JSON export is required")
    if not isinstance(payload.get("products"), list):
        raise ValueError("The export must contain a products array")
    clean = copy.deepcopy(payload)
    data = clean.setdefault("product_data", {})
    if not isinstance(data, dict):
        raise ValueError("Invalid product_data")
    quarantine = []
    manual = set()
    for table, id_field in (("product_field_evidence", "product_id"),
                            ("product_reference_evidence", "gtin_key")):
        for evidence in data.get(table, []):
            if (evidence.get("source_type") == "manual"
                    and evidence.get("verification_status") == "verified"
                    and evidence.get("active")):
                manual.add((id_field, str(evidence.get(id_field, "")),
                            evidence.get("field_name"), evidence.get("field_value")))
            elif evidence.get("field_name") in RESET_FIELDS and evidence.get("verification_status") != "rejected":
                evidence["verification_status"] = "requires_review"
                evidence["active"] = 0

    def clear_fields(row, id_field, identity, table):
        for field in sorted(RESET_FIELDS):
            value = row.get(field)
            if value in (None, ""):
                continue
            if (id_field, str(identity), field, value) in manual:
                continue
            quarantine.append({"table": table, "id": identity,
                               "barcode": row.get("barcode", ""),
                               "field": field, "value": value,
                               "reason": "automatic_or_unproven_metadata"})
            row[field] = ""
            if field in {"description", "image_url"}:
                row["description_status" if field == "description" else "image_status"] = "missing"
        row["data_status"] = "requires_review"
        row["quality_checked_at"] = ""

    for row in clean["products"]:
        if not isinstance(row, dict) or not isinstance(row.get("barcode", ""), str):
            raise ValueError("UPCs must be strings: numeric exports can lose leading zeros")
        clear_fields(row, "product_id", row.get("id"), "products")
    for row in data.get("product_reference", []):
        if not isinstance(row.get("barcode", ""), str):
            raise ValueError("Reference UPCs must be strings")
        clear_fields(row, "gtin_key", row.get("gtin_key") or gtin_identity_key(row.get("barcode")), "product_reference")
        row["verification_status"] = "requires_review"
        row["enrich_status"] = ""
    for alias in data.get("product_aliases", []):
        if alias.get("source") not in {"planogram", "manual"}:
            alias["verification_status"] = "requires_review"
    # Images duplicated into expiry cards must not reintroduce old associations.
    for row in data.get("product_expiry_status", []):
        if row.get("image_url"):
            quarantine.append({"table": "product_expiry_status", "barcode": row.get("barcode", ""),
                               "field": "image_url", "value": row["image_url"], "reason": "legacy_copy"})
            row["image_url"] = ""
    return clean, quarantine


def prepare(source, destination):
    source, destination = Path(source), Path(destination)
    original = source.read_bytes()
    cleaned, quarantine = rebuild(json.loads(original))
    # Exclusive creation prevents overwriting a backup or an earlier review.
    destination.mkdir(parents=True, exist_ok=False)
    outputs = {
        "original.json": original,
        "clean-catalogue.json": json.dumps(cleaned, ensure_ascii=False, indent=2).encode(),
        "quarantine.json": json.dumps(quarantine, ensure_ascii=False, indent=2).encode(),
    }
    for name, content in outputs.items():
        (destination / name).write_bytes(content)
    manifest = {
        "products": len(cleaned["products"]),
        "quarantined_fields": len(quarantine),
        "sha256": {name: hashlib.sha256(content).hexdigest() for name, content in outputs.items()},
        "note": "Validate the cleaned export in an isolated staging database. The production database was not accessed.",
    }
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backup", help="JSON export from the existing app")
    parser.add_argument("new_directory", help="New private directory for review artifacts")
    args = parser.parse_args()
    print(json.dumps(prepare(args.backup, args.new_directory), indent=2))
