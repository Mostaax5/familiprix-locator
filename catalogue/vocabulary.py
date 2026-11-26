"""One bilingual vocabulary shared by indexing, query parsing and the UI.

Concepts describe retail product types, never therapeutic equivalence. A brand
alone cannot assign an ingredient, indication, age range, or compatibility.
"""
import re
import unicodedata


def normalize(value):
    text = unicodedata.normalize('NFKD', str(value or '').lower().replace('œ', 'oe'))
    return re.sub(r'[^a-z0-9%]+', ' ', ''.join(c for c in text if not unicodedata.combining(c))).strip()


# Expansion is reversible: the original store label remains on the detail card.
# Ambiguous abbreviations (e.g. REG, NT, ACET) are deliberately left unchanged.
ABBREVIATIONS = {
    'CO': 'comprimés', 'CA': 'capsules', 'CAPS': 'capsules', 'CAPT': 'caplets',
    'GELCAP': 'capsules de gel', 'LIQ': 'liquide', 'SIR': 'sirop',
    'SHP': 'shampoing', 'SHAMP': 'shampoing', 'DENT': 'dentifrice',
    'DEMAQ': 'démaquillant', 'HYDR': 'hydratant', 'VAPO': 'vaporisateur',
    'COMP': 'comprimés', 'CP': 'comprimés',
    'VIT': 'vitamine', 'CR': 'crème', 'LOT': 'lotion', 'DEO': 'déodorant',
    'REV': 'revitalisant', 'MENT': 'menthe', 'PANS': 'pansements',
    'LING': 'lingettes', 'SAV': 'savon', 'RECH': 'recharge',
    'ENF': 'enfants', 'BB': 'bébé', 'HYD': 'hydratant', 'NETT': 'nettoyant',
    'GTTS': 'gouttes', 'PDRE': 'poudre', 'ONG': 'onguent', 'SER': 'sérum',
}


def readable_label(value):
    text = re.sub(r'\s+', ' ', str(value or '')).strip()
    text = re.sub(r'\bBR/DENTS?\b', 'brosse à dents', text)
    text = re.sub(r'\bFIL[ /]DENT\b', 'fil dentaire', text)
    text = re.sub(r'\bSOIE DENT\b', 'soie dentaire', text)
    text = re.sub(r'\b(?:NETT APP DENT|NETT DENT)\b', 'nettoyant pour prothèses dentaires', text)
    text = re.sub(r'\b(' + '|'.join(ABBREVIATIONS) + r')(?=\d)', r'\1 ', text)
    text = re.sub(r'\b(?:' + '|'.join(ABBREVIATIONS) + r')\b',
                  lambda m: ABBREVIATIONS[m[0]], text)
    # Whitespace was collapsed above. Bound this match to the single possible
    # space so unit formatting never scans an unbounded whitespace run.
    text = re.sub(r'(?<=\d) ?(MCG|MG|ML|KG|G|L)\b', lambda m: ' ' + m[1].lower(), text)
    # Explicit, unambiguous printed claims; no inference from a missing claim.
    text = re.sub(r'\bS/SUCRE\b', 'sans sucre', text)
    text = re.sub(r'\bS/PARFUM\b', 'sans parfum', text)
    text = re.sub(r'\bS/ALC\b', 'sans alcool', text)
    text = re.sub(r'\bN/PARF\b', 'sans parfum', text)
    return text


CONCEPTS = {
    'oral_toothpaste': ('Dentifrices', ['dentifrice', 'dentifrices', 'toothpaste']),
    'oral_brush': ('Brosses à dents', ['brosse a dents', 'brosses a dents', 'toothbrush', 'toothbrushes']),
    'oral_rinse': ('Rince-bouche', ['rince bouche', 'mouthwash']),
    'oral_floss': ('Soies et fils dentaires', ['fil dentaire', 'soie dentaire', 'dental floss']),
    'denture_cleaner': ('Entretien des prothèses dentaires', ['nettoyant pour protheses dentaires', 'denture cleaner']),
    'hair_shampoo': ('Shampoings', ['shampoing', 'shampoo', 'shampooing']),
    'hair_conditioner': ('Revitalisants', ['revitalisant', 'conditioner', 'apres shampoing']),
    'skin_moisturizer': ('Hydratants', ['hydratant', 'hydratante', 'moisturizer', 'moisturizing']),
    'skin_cleanser': ('Nettoyants', ['nettoyant', 'cleanser', 'demaquillant', 'makeup remover']),
    'sun_care': ('Protection solaire', ['solaire', 'sunscreen', 'sunblock', 'ecran solaire']),
    'deodorant': ('Déodorants', ['deodorant', 'antisudorifique', 'antiperspirant']),
    'baby_diapers': ('Couches', ['couche', 'couches', 'diaper', 'diapers']),
    'wipes': ('Lingettes', ['lingette', 'lingettes', 'wipes']),
    'bandages': ('Pansements', ['pansement', 'pansements', 'bandage', 'bandages']),
    'eye_drops': ('Gouttes pour les yeux', ['gouttes ophtalmiques', 'gouttes yeux', 'eye drops', 'larmes artificielles']),
    'vitamins': ('Vitamines', ['vitamine', 'vitamines', 'vitamin', 'vitamins', 'multivitamine']),
    'batteries': ('Piles', ['pile', 'piles', 'battery', 'batteries']),
    'feminine_care': ('Hygiène féminine', ['tampon', 'tampons', 'serviette hygienique', 'protege dessous']),
    'soap': ('Savons', ['savon', 'soap', 'body wash', 'gel douche']),
    'lip_care': ('Soins des lèvres', ['baume levres', 'lip balm', 'levres']),
    'shaving': ('Rasage', ['rasoir', 'rasoirs', 'razor', 'rasage', 'shaving']),
    'thermometer': ('Thermomètres', ['thermometre', 'thermometer']),
    'creams': ('Crèmes', ['creme', 'cream']),
    'lotions': ('Lotions', ['lotion', 'lotions']),
    'serums': ('Sérums', ['serum', 'serums']),
    'drops': ('Gouttes', ['gouttes', 'drops']),
    'powders': ('Poudres', ['poudre', 'powder']),
    'ointments': ('Onguents', ['onguent', 'ointment']),
}

SYNONYMS = [
    ['comprime', 'comprimes', 'tablet', 'tablets', 'caplet', 'caplets'],
    ['capsule', 'capsules', 'gelule', 'gelules'], ['liquide', 'liquid'],
    ['enfant', 'enfants', 'children', 'kids'], ['bebe', 'baby', 'infant'],
    ['adulte', 'adult'], ['menthe', 'mint'], ['fraise', 'strawberry'],
    ['cerise', 'cherry'], ['raisin', 'grape'], ['vanille', 'vanilla'],
    ['creme', 'cream'], ['recharge', 'refill', 'replacement'],
    ['ibup', 'ibuprofene', 'ibuprofen'], ['acetaminophene', 'acetaminophen'],
    ['sans sucre', 'sugar free'], ['sans parfum', 'fragrance free', 'unscented'],
    ['sans alcool', 'alcohol free', 'without alcohol'], ['sans gluten', 'gluten free', 'without gluten'],
]

STOP = set('je cherche recherche voudrais besoin avez vous svp le la les de des du un une pour a au aux et ou en avec the a an i need looking for do you have please where is are find me some product produit produits'.split())


def contains(text, phrase):
    return (' ' + phrase + ' ') in (' ' + text + ' ')


def concept_ids(text):
    text = normalize(readable_label(text))
    result = [key for key, (_, terms) in CONCEPTS.items() if any(contains(text, t) for t in terms)]
    if contains(text, 'apres shampoing') and 'hair_shampoo' in result:
        result.remove('hair_shampoo')
    if 'vitamins' in result and any(c in result for c in ('creams','lotions','serums','skin_cleanser','hair_shampoo','hair_conditioner')):
        result.remove('vitamins')
    return result


def tokens(text):
    return [t for t in normalize(text).split() if t not in STOP and (len(t) > 1 or t.isdigit() or t in {'b','c','d','e','k'})]


def expand_tokens(text):
    normalized = normalize(readable_label(text))
    output = set(tokens(normalized))
    for group in SYNONYMS:
        if any(contains(normalized, term) for term in group):
            for term in group:
                output.update(tokens(term))
    for key in concept_ids(normalized):
        output.add('concept_' + key)
    return output
