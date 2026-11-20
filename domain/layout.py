"""Layout normalization and limits, independent of Flask and persistence."""
import json
import re

MAX_LAYOUT_SECTIONS = 200
MAX_LAYOUT_SHELVES = 100
MAX_LAYOUT_POSITIONS = 500
MAX_LAYOUT_PRESENTOIRS = 100
MAX_LAYOUT_FACADES = 50
MAX_LAYOUT_LABEL_LENGTH = 160
_AISLE_NAME_RE = re.compile(r"[A-Za-z0-9À-ÿ .\-]+")


# ── Config helpers ─────────────────────────────────────────────────────────────

def clamp_non_negative_int(value, fallback=0, maximum=None):
    try:
        cleaned = max(0, int(str(value)))
        return min(cleaned, maximum) if maximum is not None else cleaned
    except (TypeError, ValueError):
        return fallback


def valid_aisle_name(value):
    aisle = str(value or "").strip()
    return bool(aisle and len(aisle) <= 40 and _AISLE_NAME_RE.fullmatch(aisle))


def _layout_label(value):
    return str(value or "")[:MAX_LAYOUT_LABEL_LENGTH]


def build_default_layout_config(max_section, max_shelf, max_position):
    section_count = clamp_non_negative_int(max_section, maximum=MAX_LAYOUT_SECTIONS)
    shelf_count = clamp_non_negative_int(max_shelf, maximum=MAX_LAYOUT_SHELVES)
    position_count = clamp_non_negative_int(max_position, maximum=MAX_LAYOUT_POSITIONS)
    section_template = [
        {"shelves": [position_count for _ in range(shelf_count)], "labels": ["" for _ in range(shelf_count)]}
        for _ in range(section_count)
    ]
    return {
        "sides": {
            "Gauche": {"sections": json.loads(json.dumps(section_template))},
            "Droite": {"sections": json.loads(json.dumps(section_template))},
        },
        "presentoirs": [],
    }


def normalize_layout_config(config_value, max_section="1", max_shelf="5", max_position="8"):
    if isinstance(config_value, str):
        try:
            config = json.loads(config_value) if config_value.strip() else {}
        except json.JSONDecodeError:
            config = {}
    else:
        config = config_value or {}

    config = config if isinstance(config, dict) else {}
    sides = config.get("sides") if isinstance(config.get("sides"), dict) else {}
    normalized_sides = {}
    default = build_default_layout_config(max_section, max_shelf, max_position)

    for side in ["Gauche", "Droite"]:
        side_value = sides.get(side) if isinstance(sides.get(side), dict) else {}
        has_explicit_sections = isinstance(side_value.get("sections"), list)
        sections = side_value.get("sections") if has_explicit_sections else []
        normalized_sections = []
        for section in sections[:MAX_LAYOUT_SECTIONS]:
            shelves = section.get("shelves") if isinstance(section, dict) else None
            if not isinstance(shelves, list):
                continue
            cleaned_shelves = [
                clamp_non_negative_int(shelf, maximum=MAX_LAYOUT_POSITIONS)
                for shelf in shelves[:MAX_LAYOUT_SHELVES]
            ]
            raw_labels = section.get("labels", []) if isinstance(section, dict) else []
            if not isinstance(raw_labels, list):
                raw_labels = []
            cleaned_labels = [
                _layout_label(raw_labels[i]) if i < len(raw_labels) else ""
                for i in range(len(cleaned_shelves))
            ]
            normalized_sections.append({"shelves": cleaned_shelves, "labels": cleaned_labels})
        if not has_explicit_sections:
            normalized_sections = default["sides"][side]["sections"]
        normalized_sides[side] = {"sections": normalized_sections}

    def norm_fixture(fx):
        if not isinstance(fx, dict):
            return {"shelves": [], "labels": []}
        raw_sh = fx.get("shelves", [])
        if not isinstance(raw_sh, list): raw_sh = []
        shelves = [
            clamp_non_negative_int(v, maximum=MAX_LAYOUT_POSITIONS)
            for v in raw_sh[:MAX_LAYOUT_SHELVES]
        ]
        raw_lab = fx.get("labels", [])
        if not isinstance(raw_lab, list): raw_lab = []
        labels = [_layout_label(raw_lab[i]) if i < len(raw_lab) else "" for i in range(len(shelves))]
        return {"shelves": shelves, "labels": labels}

    normalized_facade_a = norm_fixture(config.get("facade_a") if isinstance(config, dict) else None)
    normalized_facade_b = norm_fixture(config.get("facade_b") if isinstance(config, dict) else None)

    raw_pres = config.get("presentoirs", []) if isinstance(config, dict) else []
    if not isinstance(raw_pres, list): raw_pres = []
    normalized_pres = []
    for p in raw_pres[:MAX_LAYOUT_PRESENTOIRS]:
        if not isinstance(p, dict):
            continue
        name = _layout_label(p.get("name", "Présentoir")).strip() or "Présentoir"
        raw_facades = p.get("facades") if isinstance(p.get("facades"), list) else None
        if raw_facades:
            facades = []
            for i, f in enumerate(raw_facades[:MAX_LAYOUT_FACADES]):
                if not isinstance(f, dict):
                    continue
                fname = _layout_label(f.get("name", f"Façade {i+1}")).strip() or f"Façade {i+1}"
                fx = norm_fixture(f)
                facades.append({"name": fname, "shelves": fx["shelves"], "labels": fx["labels"]})
        else:
            fx = norm_fixture(p)
            facades = [{"name": "Façade 1", "shelves": fx["shelves"], "labels": fx["labels"]}]
        normalized_pres.append({"name": name, "facades": facades})

    return {"sides": normalized_sides, "facade_a": normalized_facade_a, "facade_b": normalized_facade_b, "presentoirs": normalized_pres}


def layout_metrics(config):
    sides = config.get("sides", {})
    max_section = max(len((sides.get(side) or {}).get("sections", [])) for side in ["Gauche", "Droite"])
    max_shelf = 0
    max_position = 0
    for side in ["Gauche", "Droite"]:
        for section in (sides.get(side) or {}).get("sections", []):
            shelves = section.get("shelves", [])
            max_shelf = max(max_shelf, len(shelves))
            if shelves:
                max_position = max(max_position, max(shelves))
    return str(max_section), str(max_shelf), str(max_position)


