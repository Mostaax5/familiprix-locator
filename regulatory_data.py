"""Authoritative Canadian regulatory product-data helpers.

DIN matching uses Health Canada's DPD packaging extract because it contains the
exact commercial UPC and the related drug record. NPN and DIN-HM do not have an
equivalent public UPC table; those identifiers are accepted only when an
exact-UPC source explicitly labels the number and Health Canada confirms the
licence and product identity.
"""

from __future__ import annotations

import csv
import html
import json
import os
import re
import tempfile
import time
import zipfile
from collections import defaultdict
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from product_data import gtin_identity_key, normalize_text, text_digits


HEALTH_CANADA_DPD_SOURCE = "Health Canada DPD"
HEALTH_CANADA_LNHPD_SOURCE = "Health Canada LNHPD"
HEALTH_CANADA_AUTHORITY = "Health Canada"
HEALTH_CANADA_DPD_PAGE = (
    "https://www.canada.ca/en/health-canada/services/drugs-health-products/"
    "drug-products/drug-product-database/what-data-extract-drug-product-database.html"
)
HEALTH_CANADA_LNHPD_PAGE = (
    "https://health-products.canada.ca/lnhpd-bdpsnh/"
)
_DPD_PACKAGE_URL = (
    "https://www.canada.ca/content/dam/hc-sc/documents/services/"
    "drug-product-database/package.zip"
)
_DPD_DRUG_URL = (
    "https://www.canada.ca/content/dam/hc-sc/documents/services/"
    "drug-product-database/drug.zip"
)
_DPD_API = "https://health-products.canada.ca/api/drug"
_LNHPD_API = "https://health-products.canada.ca/api/natural-licences"
_MAX_EXTRACT_BYTES = 64 * 1024 * 1024
_MAX_API_BYTES = 2 * 1024 * 1024


_REGULATORY_PATTERNS = (
    (
        "DIN_HM",
        re.compile(
            r"(?i)\b(?:DIN\s*[-_ ]?\s*HM|DINHM)\b\s*(?:no\.?|num(?:e|é)ro|#|:)?\s*"
            r"((?:\d[\s.-]*){8})"
        ),
    ),
    (
        "NPN",
        re.compile(
            r"(?i)\bNPN\b\s*(?:no\.?|num(?:e|é)ro|#|:)?\s*((?:\d[\s.-]*){8})"
        ),
    ),
    (
        "DIN",
        re.compile(
            r"(?i)\b(?:DIN(?!\s*[-_ ]?\s*HM)|drug\s+identification\s+number|"
            r"num(?:e|é)ro\s+d['’]identification\s+d['’]un\s+m(?:e|é)dicament)\b"
            r"\s*(?:no\.?|num(?:e|é)ro|#|:)?\s*((?:\d[\s.-]*){8})"
        ),
    ),
)

_NAME_NOISE = {
    "avec", "and", "aux", "capsule", "capsules", "comprime", "comprimes",
    "the", "pour", "sans", "product", "produit", "format", "regular",
    "regulier", "tablet", "tablets", "unit", "units", "unite", "unites",
    "bottle", "bouteille", "liquid", "liquide", "caps", "caplets",
}


def extract_regulatory_identifiers(*values):
    """Return only explicitly labelled eight-digit Canadian identifiers."""
    text = html.unescape(" ".join(str(value or "") for value in values))
    found = []
    seen = set()
    for identifier_type, pattern in _REGULATORY_PATTERNS:
        for match in pattern.finditer(text):
            value = text_digits(match.group(1))
            key = (identifier_type, value)
            if len(value) != 8 or key in seen:
                continue
            seen.add(key)
            found.append({"type": identifier_type, "value": value})
    return found


def merge_regulatory_candidates(*groups):
    merged = []
    seen = set()
    for group in groups:
        for raw in group or []:
            if not isinstance(raw, dict):
                continue
            identifier_type = str(raw.get("type", "") or "").upper().replace("-", "_")
            value = text_digits(raw.get("value", ""))
            source_url = str(raw.get("source_url", "") or "")
            key = (identifier_type, value, source_url)
            if identifier_type not in {"DIN", "NPN", "DIN_HM"} or len(value) != 8 or key in seen:
                continue
            seen.add(key)
            item = dict(raw)
            item.update({"type": identifier_type, "value": value})
            merged.append(item)
    return merged


def attach_regulatory_candidates(product, *texts):
    """Attach labelled candidates with enough provenance for later validation."""
    if not isinstance(product, dict):
        return product
    source = str(product.get("source", "") or "")
    source_url = str(product.get("source_url", "") or "")
    name = str(product.get("name", "") or "")
    barcode = text_digits(product.get("barcode", ""))
    candidates = []
    for item in extract_regulatory_identifiers(name, product.get("description", ""), *texts):
        candidates.append({
            **item,
            "source": source,
            "source_url": source_url,
            "product_name": name,
            "barcode": barcode,
        })
    product["regulatory_identifiers"] = merge_regulatory_candidates(
        product.get("regulatory_identifiers"), candidates,
    )
    return product


def _zip_rows(path):
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        if not members:
            raise ValueError("Health Canada extract contains no text table")
        with archive.open(members[0]) as raw:
            import io
            with io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="") as stream:
                yield from csv.reader(stream)


def parse_dpd_extracts(package_path, drug_path, wanted_gtin_keys):
    """Join the official DPD package and drug tables for exact wanted GTINs."""
    wanted = {str(key) for key in wanted_gtin_keys or [] if str(key)}
    packages = []
    drug_codes = set()
    for row in _zip_rows(package_path):
        if len(row) < 2:
            continue
        barcode = text_digits(row[1])
        key = gtin_identity_key(barcode)
        if not key or key not in wanted:
            continue
        drug_code = text_digits(row[0])
        if not drug_code:
            continue
        packages.append({
            "gtin_key": key,
            "barcode": barcode,
            "drug_code": drug_code,
            "package_size_unit": str(row[2] if len(row) > 2 else "").strip(),
            "package_type": str(row[3] if len(row) > 3 else "").strip(),
            "package_size": str(row[4] if len(row) > 4 else "").strip(),
            "product_information": str(row[5] if len(row) > 5 else "").strip(),
        })
        drug_codes.add(drug_code)

    products = {}
    for row in _zip_rows(drug_path):
        if len(row) < 6:
            continue
        drug_code = text_digits(row[0])
        if drug_code not in drug_codes:
            continue
        din = text_digits(row[3])
        if len(din) != 8:
            continue
        products[drug_code] = {
            "drug_code": drug_code,
            "din": din,
            "brand_name": str(row[4] or "").strip(),
            "descriptor": str(row[5] or "").strip(),
            "last_update_date": str(row[9] if len(row) > 9 else "").strip(),
            "brand_name_fr": str(row[12] if len(row) > 12 else "").strip(),
            "descriptor_fr": str(row[13] if len(row) > 13 else "").strip(),
        }

    joined = []
    for package in packages:
        drug = products.get(package["drug_code"])
        if drug:
            joined.append({**package, **drug})
    return joined


def _download_to_temp(url, *, timeout=180, max_bytes=_MAX_EXTRACT_BYTES):
    request = Request(url, headers={
        "User-Agent": "FamiliprixLocator/1.0",
        "Accept": "application/zip,application/octet-stream",
    })
    last_error = None
    for attempt in range(3):
        path = ""
        try:
            with urlopen(request, timeout=timeout) as response:
                content_length = int(response.headers.get("Content-Length") or 0)
                if content_length > max_bytes:
                    raise ValueError("Health Canada extract exceeds size limit")
                with tempfile.NamedTemporaryFile(prefix="familiprix-hc-", suffix=".zip", delete=False) as output:
                    path = output.name
                    total = 0
                    while True:
                        chunk = response.read(256 * 1024)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_bytes:
                            raise ValueError("Health Canada extract exceeds size limit")
                        output.write(chunk)
                return path, str(response.headers.get("Last-Modified") or "")
        except (OSError, HTTPError, URLError, TimeoutError, ValueError, zipfile.BadZipFile) as exc:
            last_error = exc
            if path:
                try:
                    os.remove(path)
                except OSError:
                    pass
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Health Canada download failed: {last_error}")


def download_dpd_matches(wanted_gtin_keys, progress=None):
    """Download current marketed extracts and return exact package matches."""
    package_path = drug_path = ""
    versions = []
    try:
        if progress:
            progress("download_packages")
        package_path, version = _download_to_temp(_DPD_PACKAGE_URL)
        versions.append(version)
        if progress:
            progress("download_drugs")
        drug_path, version = _download_to_temp(_DPD_DRUG_URL)
        versions.append(version)
        if progress:
            progress("match_exact_upc")
        return parse_dpd_extracts(package_path, drug_path, wanted_gtin_keys), " | ".join(
            value for value in versions if value
        )
    finally:
        for path in (package_path, drug_path):
            if path:
                try:
                    os.remove(path)
                except OSError:
                    pass


def _read_json_url(url, timeout=8):
    request = Request(url, headers={
        "User-Agent": "FamiliprixLocator/1.0",
        "Accept": "application/json",
    })
    with urlopen(request, timeout=timeout) as response:
        payload = response.read(_MAX_API_BYTES + 1)
    if len(payload) > _MAX_API_BYTES:
        raise ValueError("Health Canada API response exceeds size limit")
    return json.loads(payload.decode("utf-8", errors="replace"))


def _api_records(payload):
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
        return [payload]
    return []


def _name_tokens(value):
    return {
        token for token in normalize_text(value).split()
        if len(token) >= 3 and token not in _NAME_NOISE and not token.isdigit()
    }


def regulatory_name_match(left, right):
    left_tokens = _name_tokens(left)
    right_tokens = _name_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    shared = left_tokens & right_tokens
    if not shared:
        shared = {
            a for a in left_tokens for b in right_tokens
            if len(a) >= 4 and len(b) >= 4 and (a.startswith(b) or b.startswith(a))
        }
    if not shared:
        return False
    return len(shared) >= 2 or len(shared) / max(1, min(len(left_tokens), len(right_tokens))) >= 0.5


def verify_regulatory_candidate(candidate, catalog_name="", fetch_json=None):
    """Verify a labelled exact-UPC candidate against the relevant official API."""
    item = dict(candidate or {})
    identifier_type = str(item.get("type", "") or "").upper().replace("-", "_")
    value = text_digits(item.get("value", ""))
    barcode = text_digits(item.get("barcode", ""))
    product_name = str(item.get("product_name", "") or catalog_name or "")
    fetch = fetch_json or _read_json_url
    if identifier_type not in {"DIN", "NPN", "DIN_HM"} or len(value) != 8:
        return {"verified": False, "reason": "invalid_identifier"}

    try:
        if identifier_type == "DIN":
            url = f"{_DPD_API}/drugproduct/?{urlencode({'din': value, 'lang': 'en', 'type': 'json'})}"
            records = [
                record for record in _api_records(fetch(url))
                if text_digits(record.get("drug_identification_number", "")) == value
            ]
            for record in records:
                code = text_digits(record.get("drug_code", ""))
                if not code:
                    continue
                package_url = f"{_DPD_API}/packaging/?{urlencode({'id': code, 'type': 'json'})}"
                package_match = any(
                    gtin_identity_key(row.get("upc", "")) == gtin_identity_key(barcode)
                    for row in _api_records(fetch(package_url))
                ) if barcode else False
                if package_match:
                    return {
                        "verified": True,
                        "identifier_type": "DIN",
                        "value": value,
                        "source": HEALTH_CANADA_DPD_SOURCE,
                        "source_url": HEALTH_CANADA_DPD_PAGE,
                        "source_record_id": code,
                        "official_name": str(record.get("brand_name", "") or "").strip(),
                        "manufacturer": str(record.get("company_name", "") or "").strip(),
                        "match_method": "exact_gtin_health_canada_packaging",
                        "confidence": 1.0,
                    }
            return {
                "verified": False,
                "reason": "din_package_not_confirmed" if records else "din_not_found",
            }

        url = f"{_LNHPD_API}/productlicence/?{urlencode({'id': value, 'lang': 'en', 'type': 'json'})}"
        records = [
            record for record in _api_records(fetch(url))
            if text_digits(record.get("licence_number", "")) == value
        ]
        active_records = []
        for record in records:
            official_name = str(record.get("product_name", "") or "").strip()
            active = str(record.get("flag_product_status", "1") or "1").lower() not in {
                "0", "false", "inactive", "no",
            }
            if not active:
                continue
            active_records.append(record)
            if not regulatory_name_match(product_name or catalog_name, official_name):
                continue
            lnhpd_id = str(record.get("lnhpd_id", "") or "").strip()
            details = {}
            if lnhpd_id:
                for endpoint in (
                    "medicinalingredient", "productpurpose", "productroute",
                ):
                    detail_url = (
                        f"{_LNHPD_API}/{endpoint}/?"
                        f"{urlencode({'id': lnhpd_id, 'lang': 'en', 'type': 'json'})}"
                    )
                    try:
                        details[endpoint] = _api_records(fetch(detail_url))
                    except Exception:
                        details[endpoint] = []
            ingredient_parts = []
            for ingredient in details.get("medicinalingredient", [])[:12]:
                name = str(ingredient.get("ingredient_name", "") or "").strip()
                amount = str(
                    ingredient.get("quantity", "")
                    or ingredient.get("potency_amount", "") or ""
                ).strip()
                unit = str(
                    ingredient.get("quantity_unit_of_measure", "")
                    or ingredient.get("potency_unit_of_measure", "") or ""
                ).strip()
                if name:
                    ingredient_parts.append(
                        " ".join(part for part in (name, amount, unit) if part)
                    )
            purposes = [
                str(detail.get("purpose", "") or "").strip()
                for detail in details.get("productpurpose", [])[:3]
                if str(detail.get("purpose", "") or "").strip()
            ]
            routes = [
                str(detail.get("route_type_desc", "") or "").strip()
                for detail in details.get("productroute", [])[:4]
                if str(detail.get("route_type_desc", "") or "").strip()
            ]
            dosage_form = str(record.get("dosage_form", "") or "").strip()
            description_parts = [official_name]
            if purposes:
                description_parts.append(" ".join(purposes))
            if dosage_form:
                description_parts.append(f"Dosage form: {dosage_form}")
            if ingredient_parts:
                description_parts.append(
                    f"Medicinal ingredients: {', '.join(ingredient_parts)}"
                )
            if routes:
                description_parts.append(f"Route: {', '.join(routes)}")
            return {
                "verified": True,
                "identifier_type": identifier_type,
                "value": value,
                "source": HEALTH_CANADA_LNHPD_SOURCE,
                "source_url": HEALTH_CANADA_LNHPD_PAGE,
                "source_record_id": lnhpd_id or value,
                "official_name": official_name,
                "manufacturer": str(record.get("company_name", "") or "").strip(),
                "dosage_form": dosage_form,
                "ingredients": ", ".join(ingredient_parts),
                "purpose": " ".join(purposes),
                "route_of_administration": ", ".join(routes),
                "description": ". ".join(
                    part.rstrip(".") for part in description_parts if part
                ) + ".",
                "match_method": "exact_gtin_label_plus_health_canada_licence",
                "confidence": 0.98,
            }
        if active_records:
            # The licence exists and is active, but an abbreviated planogram name
            # was not strong enough for automatic confirmation.  Keep the exact-
            # UPC labelled association useful, explicitly as probable.
            return {
                "verified": False,
                "probable": True,
                "reason": "official_licence_name_unconfirmed",
                "official_name": str(
                    active_records[0].get("product_name", "") or ""
                ).strip(),
            }
        return {
            "verified": False,
            "reason": "licence_inactive" if records else "licence_not_found",
        }
    except (OSError, HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        return {"verified": False, "reason": "official_source_unavailable", "error": str(exc)[:200]}


def group_unambiguous_dpd_matches(matches):
    """Separate exact UPCs with one DIN from source conflicts."""
    grouped = defaultdict(list)
    for match in matches or []:
        grouped[str(match.get("gtin_key", ""))].append(dict(match))
    verified = {}
    conflicts = {}
    for key, rows in grouped.items():
        dins = {str(row.get("din", "")) for row in rows if str(row.get("din", ""))}
        if len(dins) == 1:
            verified[key] = rows
        elif dins:
            conflicts[key] = rows
    return verified, conflicts
