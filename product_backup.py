"""Backup and restore helpers for product identity and provenance records."""

from __future__ import annotations


PRODUCT_DATA_TABLE_COLUMNS = {
    "product_reference": (
        "barcode", "name", "brand", "description", "image_url", "source",
        "source_url", "product_code", "enrich_status", "updated_at", "gtin_key",
        "match_method", "verification_status", "last_verified_at",
        "store_presence_status", "package_size", "package_unit", "variant",
        "flavour", "colour", "strength", "dosage_form", "manufacturer",
        "category", "ingredients", "compatibility", "official_name_fr",
        "official_name_en", "source_priority", "confidence",
    ),
    "product_reference_evidence": (
        "gtin_key", "barcode", "field_name", "field_value", "source",
        "source_type", "source_priority", "source_url", "source_record_id",
        "match_method", "confidence", "verification_status", "imported_at",
        "last_verified_at", "active",
    ),
    "product_identifiers": (
        "product_id", "identifier_type", "identifier_value", "normalized_value",
        "authority", "is_primary", "package_level", "source", "source_url",
        "source_record_id", "match_method", "confidence", "verification_status",
        "imported_at", "last_verified_at",
    ),
    "product_field_evidence": (
        "product_id", "field_name", "field_value", "source", "source_type",
        "source_priority", "source_url", "source_record_id", "match_method",
        "confidence", "verification_status", "imported_at", "last_verified_at",
        "active",
    ),
    "product_data_issues": (
        "product_id", "issue_type", "field_name", "existing_value",
        "candidate_value", "source", "source_url", "match_method", "confidence",
        "status", "details_json", "created_at", "resolved_at", "resolved_by",
    ),
    "product_aliases": (
        "product_id", "alias_type", "alias_value", "normalized_value", "language",
        "source", "confidence", "verification_status",
    ),
    "product_relationships": (
        "source_product_id", "target_product_id", "relationship_type", "source",
        "source_url", "confidence", "verification_status", "approved_by",
        "approved_role", "created_at", "last_verified_at",
    ),
}

PRODUCT_ID_TABLES = {
    "product_identifiers", "product_field_evidence", "product_data_issues",
    "product_aliases",
}

PRODUCT_RESTORE_COLUMNS = (
    "name", "brand", "description", "image_url", "source_url", "search_terms",
    "usage_notes", "alternative_suggestions", "barcode", "product_code",
    "facings", "aisle", "side", "section", "shelf", "position", "is_plano",
    "in_stock", "linked_position", "flipped_label", "underneath_label",
    "created_by", "created_at", "modified_by", "modified_at", "gtin_key",
    "data_status", "identity_status", "name_status", "description_status",
    "image_status", "quality_checked_at", "primary_source",
    "primary_source_url", "category", "package_size", "package_unit",
    "variant", "flavour", "colour", "strength", "dosage_form",
    "manufacturer", "ingredients", "compatibility", "official_name_fr",
    "official_name_en", "quality_issue_count",
)

_PRODUCT_FLAG_COLUMNS = {"is_plano", "in_stock", "flipped_label"}
_PRODUCT_RESTORE_DEFAULTS = {
    "data_status": "complete_unverified",
    "identity_status": "unverified",
    "name_status": "unverified",
    "description_status": "unverified",
    "image_status": "unverified",
}


def build_product_data_backup(db):
    return {
        table: [dict(row) for row in db.execute(
            f"SELECT * FROM {table} ORDER BY {'barcode' if table == 'product_reference' else 'id'}"
        ).fetchall()]
        for table in PRODUCT_DATA_TABLE_COLUMNS
    }


def _clean_value(value):
    if value is None or isinstance(value, (int, float)):
        return value
    return str(value)[:12000]


def _map_product_id(value, product_id_map):
    try:
        old_id = int(value)
    except (TypeError, ValueError):
        return 0
    return int(product_id_map.get(old_id, 0) or 0)


def restore_product_backup_row(db, product, actor, now):
    """Restore one validated product and return its current database id."""
    item = dict(product or {})
    item["created_by"] = str(item.get("created_by") or actor)[:200]
    item["created_at"] = str(item.get("created_at") or now)[:64]
    item["modified_by"] = str(actor or item.get("modified_by") or "restore")[:200]
    item["modified_at"] = str(now)[:64]

    values = []
    for column in PRODUCT_RESTORE_COLUMNS:
        value = item.get(column, _PRODUCT_RESTORE_DEFAULTS.get(column, ""))
        if column in _PRODUCT_FLAG_COLUMNS:
            try:
                value = 1 if int(value) else 0
            except (TypeError, ValueError, OverflowError):
                value = 1 if column == "in_stock" else 0
        elif column == "facings":
            try:
                value = min(max(int(value), 1), 1000)
            except (TypeError, ValueError, OverflowError):
                value = 1
        elif column == "quality_issue_count":
            try:
                value = min(max(int(value), 0), 1_000_000)
            except (TypeError, ValueError, OverflowError):
                value = 0
        else:
            value = _clean_value(value)
        values.append(value)

    location_columns = ("aisle", "side", "section", "shelf", "position")
    location_values = tuple(item.get(column, "") for column in location_columns)
    row = db.execute(
        """
        SELECT id FROM products
        WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?
        """,
        location_values,
    ).fetchone()
    if row:
        immutable = set(location_columns)
        update_columns = [
            column for column in PRODUCT_RESTORE_COLUMNS if column not in immutable
        ]
        update_values = [values[PRODUCT_RESTORE_COLUMNS.index(column)] for column in update_columns]
        product_id = int(row["id"] if isinstance(row, dict) else row[0])
        db.execute(
            f"UPDATE products SET {','.join(f'{column}=?' for column in update_columns)} WHERE id=?",
            tuple(update_values) + (product_id,),
        )
        return product_id

    placeholders = ",".join("?" for _ in PRODUCT_RESTORE_COLUMNS)
    db.execute(
        f"INSERT INTO products ({','.join(PRODUCT_RESTORE_COLUMNS)}) VALUES ({placeholders})",
        tuple(values),
    )
    row = db.execute(
        """
        SELECT id FROM products
        WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?
        """,
        location_values,
    ).fetchone()
    return int(row["id"] if isinstance(row, dict) else row[0]) if row else 0


def restore_product_data_backup(db, payload, product_id_map):
    if not isinstance(payload, dict):
        return {"restored": 0, "skipped": 0}
    restored = skipped = 0
    for table, columns in PRODUCT_DATA_TABLE_COLUMNS.items():
        rows = payload.get(table) or []
        if not isinstance(rows, list):
            continue
        if len(rows) > 250_000:
            skipped += len(rows)
            continue
        for raw in rows:
            if not isinstance(raw, dict):
                skipped += 1
                continue
            item = dict(raw)
            if table in PRODUCT_ID_TABLES:
                mapped = _map_product_id(item.get("product_id"), product_id_map)
                if not mapped:
                    skipped += 1
                    continue
                item["product_id"] = mapped
            elif table == "product_relationships":
                source_id = _map_product_id(item.get("source_product_id"), product_id_map)
                target_id = _map_product_id(item.get("target_product_id"), product_id_map)
                if not source_id or not target_id or source_id == target_id:
                    skipped += 1
                    continue
                item["source_product_id"] = source_id
                item["target_product_id"] = target_id
            if table == "product_reference" and not str(item.get("barcode", "") or "").strip():
                skipped += 1
                continue
            values = tuple(_clean_value(item.get(column, "")) for column in columns)
            placeholders = ",".join("?" for _ in columns)
            if table == "product_reference":
                update_columns = [column for column in columns if column != "barcode"]
                conflict = "ON CONFLICT(barcode) DO UPDATE SET " + ",".join(
                    f"{column}=excluded.{column}" for column in update_columns
                )
            elif table == "product_data_issues":
                existing = db.execute(
                    """
                    SELECT id FROM product_data_issues
                    WHERE product_id=? AND issue_type=? AND field_name=?
                      AND COALESCE(existing_value, '')=?
                      AND COALESCE(candidate_value, '')=? AND status=?
                    LIMIT 1
                    """,
                    (
                        item.get("product_id"), item.get("issue_type", ""),
                        item.get("field_name", ""), item.get("existing_value", "") or "",
                        item.get("candidate_value", "") or "", item.get("status", "") or "",
                    ),
                ).fetchone()
                if existing:
                    skipped += 1
                    continue
                conflict = ""
            else:
                conflict = "ON CONFLICT DO NOTHING"
            db.execute(
                f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) {conflict}",
                values,
            )
            restored += 1
    return {"restored": restored, "skipped": skipped}
