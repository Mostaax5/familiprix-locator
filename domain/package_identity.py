"""Comparable package facts, independent of AI and descriptive marketing text."""

import re
import unicodedata
from decimal import Decimal


def identity_text(value):
    text = unicodedata.normalize("NFKD", str(value or "").lower().replace("µ", "u").replace("μ", "u"))
    return "".join(char for char in text if not unicodedata.combining(char))


def number(value):
    return Decimal(str(value).replace(",", "."))


def quantity(value, unit):
    factors = {"mcg": ("mg", ".001"), "ug": ("mg", ".001"),
               "mg": ("mg", "1"), "g": ("mg", "1000"),
               "kg": ("mg", "1000000"), "ml": ("ml", "1"),
               "l": ("ml", "1000"), "%": ("%", "1"), "oz": ("oz", "1")}
    dimension, factor = factors[unit]
    return number(value) * Decimal(factor), dimension


FLAVOURS = (
    ("mint", "menthe"), ("grape", "raisin"), ("strawberry", "fraise"),
    ("cherry", "cerise"), ("orange",), ("lemon", "citron"), ("lime",),
    ("raspberry", "framboise"), ("vanilla", "vanille"), ("chocolate", "chocolat"),
    ("unflavoured", "unflavored", "sans saveur"),
)
COLOURS = (("red", "rouge"), ("blue", "bleu"), ("green", "vert"),
           ("black", "noir"), ("white", "blanc"), ("pink", "rose"),
           ("brown", "brun"), ("grey", "gray", "gris"), ("purple", "violet"),
           ("yellow", "jaune"))
FORMATS = (
    ("shampoo", "shampoing", "shp"),
    ("conditioner", "revitalisant", "apres shampoing"),
    ("refill", "recharge", "replacement", "remplacement"),
    ("device", "appareil", "machine", "brosse electrique"),
    ("liquid", "liquide", "suspension", "sirop", "sir"),
    ("tablet", "tablets", "tablette", "tablettes", "comprime", "comprimes", "caplet", "caplets"),
    ("capsule", "capsules", "gelule", "gelules", "liqui gel", "liquigel", "liquigels"),
    ("cream", "creme"), ("lotion",), ("spray", "vaporisateur", "vapo"),
)

# These are product types, not therapeutic substitutions. Unknown vocabulary
# stays unknown; the matcher never invents a family from a brand alone.
FAMILIES = (
    ("toothpaste", "dentifrice", "dentifrices"),
    ("mouthwash", "rince bouche", "rinse"),
    ("toothbrush", "brosse a dents", "brosses a dents"),
    ("shampoo", "shampoing", "shp"),
    ("conditioner", "revitalisant", "apres shampoing"),
    ("deodorant", "deodorants", "deodorante", "antisudorifique"),
    ("vitamin", "vitamins", "vitamine", "vitamines", "vit"),
    ("tuna", "thon"), ("diaper", "diapers", "couche", "couches"),
    ("makeup remover", "demaquillant", "demaquillants", "demaq"),
    ("sunscreen", "ecran solaire", "protection solaire"),
)


def product_families(value):
    text = identity_text(value).replace("-", " ")
    result = groups(text, FAMILIES)
    if "apres shampoing" in text:
        result.discard("shampoo")
    return result


def groups(text, aliases):
    return {group[0] for group in aliases if any(
        re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", text) for term in group
    )}


def package_facts(*values):
    text = identity_text(" ".join(str(value or "") for value in values))
    facts = {key: set() for key in ("sizes", "counts", "strengths", "formats", "flavours", "colours")}
    numeric = r"\d+(?:[.,]\d+)?"
    # Normalize concentration before standalone measurements. 100mg/5ml and
    # 20mg/ml are the same concentration, regardless of bottle size.
    concentration = re.compile(
        rf"(?<![\w.,])({numeric})\s*(mcg|ug|mg|g)\s*(?:/|par|per)\s*"
        rf"({numeric})?\s*(ml|l|dose)(?!\w)"
    )
    def consume(match):
        amount, mass_unit = quantity(match[1], match[2])
        denominator = number(match[3] or "1")
        unit = match[4]
        if unit == "l":
            denominator *= 1000
            unit = "ml"
        if denominator > 0:
            facts["strengths"].add(f"{(amount / denominator).normalize()}:{mass_unit}/{unit}")
        else:
            facts["strengths"].add("invalid:zero_denominator")
        return " "
    rest = concentration.sub(consume, text)
    for match in re.finditer(rf"(?<![\w.,])({numeric})\s*(mcg|ug|mg|kg|ml|g|l|oz|%)(?!\w)", rest):
        amount, dimension = quantity(match[1], match[2])
        target = "strengths" if match[2] in {"mcg", "ug", "mg", "%"} else "sizes"
        facts[target].add(f"{amount.normalize()}:{dimension}")
    for count in re.findall(r"\b(\d+)\s*(?:comprimes?|tablets?|caplets?|capsules?|gelules?|doses?|unites?|units?|un|sachets?|pieces?|lingettes?|wipes?|ct)\b", text):
        facts["counts"].add(f"{int(count)}:unit")
    for count in re.findall(r"\b(?:ca|co|caps?|capsules?|capt|caplets?|gelcaps?|comprimes?|tablets?|gelules?|ct)\s*(\d{1,4})\b(?![.,]\d)(?!\s*(?:mcg|ug|mg|kg|g|ml|l|oz|%)\b)", text):
        facts["counts"].add(f"{int(count)}:unit")
    for packs, count in re.findall(r"\b(\d+)\s*[x×]\s*(\d+)\b(?!\s*(?:ml|mg|g|l|oz)\b)", text):
        facts["counts"].add(f"{int(packs) * int(count)}:unit")
    facts["formats"] = groups(text, FORMATS)
    # Do not classify "après-shampoing" as both shampoo and conditioner.
    if re.search(r"apres[ -]shampoing", text):
        facts["formats"].discard("shampoo")
        facts["formats"].add("conditioner")
    facts["flavours"] = groups(text, FLAVOURS)
    facts["colours"] = groups(text, COLOURS)
    return facts
