import json
import time
import unittest
from unittest.mock import patch

from app import app
from routes.ai import (
    _outbound_url_allowed,
    _deepseek_json_request,
    build_client_query_plan,
    classify_client_request,
    filter_client_answer_category,
    generate_documented_client_answer,
    health_canada_documents,
    health_canada_nhp_documents,
    normalize_documented_client_answer,
    normalize_verified_client_answer,
    normalize_url,
    retrieve_client_documentation,
    select_client_answer_candidates,
    _unconfirmed_identifier_notice,
)
from routes.products import (
    client_excluded_concept_terms,
    client_required_concept_groups,
    find_existing_image_for_barcode,
    hybrid_client_candidates,
    normalize_search_text,
    product_matches_client_request,
    row_matches_client_concepts,
    tokenize_search_query,
)

app.config.update(TESTING=True, AUTH_TEST_BYPASS=True)


def search_row(name, brand="", description="", barcode=""):
    normalized_name = normalize_search_text(name)
    normalized_brand = normalize_search_text(brand)
    haystack = " ".join([
        normalized_name,
        normalized_brand,
        normalize_search_text(description),
    ])
    return {
        "_bc": barcode,
        "_name": normalized_name,
        "_brand": normalized_brand,
        "_hay": haystack,
        "_tokens": normalized_name.split(),
    }


class ClientRagTests(unittest.TestCase):
    def test_outbound_lookup_urls_cannot_reach_other_or_insecure_hosts(self):
        base = "https://example.com/catalog"
        self.assertEqual(
            normalize_url(base, "/product/123"),
            "https://example.com/product/123",
        )
        self.assertEqual(normalize_url(base, "https://169.254.169.254/latest"), "")
        self.assertEqual(normalize_url(base, "http://example.com/product/123"), "")
        self.assertFalse(_outbound_url_allowed("https://user:pass@example.com/private"))

    def test_hybrid_retrieval_corrects_spoken_french_brand_typo(self):
        advil = {"id": 1, "name": "Advil Extra Fort", "brand": "Advil", "barcode": "111"}
        unrelated = {"id": 2, "name": "Tylenol Regular", "brand": "Tylenol", "barcode": "222"}
        corpus = [
            (advil, search_row(advil["name"], advil["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
        ]
        plan = {
            "corrected_query": "J'ai besoin d'Advil",
            "search_queries": ["Advil"],
            "keywords": ["Advil", "ibuprofen"],
            "must_include": ["Advil"],
            "exclude": [],
        }
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             patch("routes.products._reference_corpus", return_value=[]):
            matches = hybrid_client_candidates("Jai besoin dadvile", plan, limit=10)

        self.assertEqual([item["name"] for item in matches], ["Advil Extra Fort"])

    def test_headache_retrieval_uses_the_full_intent_not_the_word_tete(self):
        question = "Jai male a la tete que prendre"
        products = [
            {
                "id": 1, "name": "BIOMEDIC SOUL M/TETE ULT CO120",
                "brand": "Biomedic", "barcode": "101",
                "description": "Contient de l'acétaminophène.",
            },
            {
                "id": 2, "name": "ADVIL 200MG CO100",
                "brand": "Advil", "barcode": "102",
                "description": "Comprimés d'ibuprofène.",
            },
            {
                "id": 3, "name": "TYLENOL 500MG X/F FAC CO100",
                "brand": "Tylenol", "barcode": "103",
                "description": "Comprimés d'acétaminophène.",
            },
            {
                "id": 4, "name": "ORAL-B IO TETE BR/DENTS BLC 4",
                "brand": "Oral-B", "barcode": "104",
            },
            {
                "id": 5, "name": "JJ NET BB TETE O PIEDS 400ML",
                "brand": "Johnson's", "barcode": "105",
            },
            {
                "id": 6, "name": "LOTUS AROMA H/ESS A/B MX/TETE1",
                "brand": "Lotus", "barcode": "106",
            },
            {
                "id": 7, "name": "AVENE FL/MIN TEINTE 50 + 40ML",
                "brand": "Avene", "barcode": "107",
            },
            {
                "id": 8, "name": "ADVIL ENF 100MG RAISIN 100ML",
                "brand": "Advil", "barcode": "108",
            },
            {
                "id": 9, "name": "TYLENOL RH/SIN JR NT CA20",
                "brand": "Tylenol", "barcode": "109",
            },
            {
                "id": 10, "name": "TYLENOL NUIT X/F CO40",
                "brand": "Tylenol", "barcode": "110",
            },
            {
                "id": 11, "name": "ADVIL GRIPPE CA18",
                "brand": "Advil", "barcode": "111",
            },
            {
                "id": 12, "name": "MOTRIN NOURRISSON S/COLOR 30ML",
                "brand": "Motrin", "barcode": "112",
            },
            {
                "id": 13, "name": "TYLENOL RH/TX/GR CA24",
                "brand": "Tylenol", "barcode": "113",
            },
            {
                "id": 14, "name": "ALEVE AID/SOMM NT CA20",
                "brand": "Aleve", "barcode": "114",
            },
            {
                "id": 15, "name": "LAKOTA EF ANALGESIQUE 57ML",
                "brand": "Lakota", "barcode": "115",
            },
            {
                "id": 16, "name": "ASPIRIN 81MG CROQ ACTION CO100",
                "brand": "Aspirin", "barcode": "116",
            },
            {
                "id": 17, "name": "TYLENOL X/F A/MUCUS 170ML",
                "brand": "Tylenol", "barcode": "117",
            },
        ]
        corpus = [
            (
                product,
                search_row(
                    product["name"], product.get("brand", ""),
                    product.get("description", ""), product["barcode"],
                ),
            )
            for product in products
        ]
        plan = build_client_query_plan(question, "documented")

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(question, plan, limit=100)

        self.assertEqual(plan["intent"], "headache_relief")
        self.assertEqual(
            {product["name"] for product in matches},
            {
                "BIOMEDIC SOUL M/TETE ULT CO120",
                "ADVIL 200MG CO100",
                "TYLENOL 500MG X/F FAC CO100",
            },
        )
        self.assertTrue(product_matches_client_request(
            products[14],
            "Jai mal a la tete et je cherche un analgesique topique",
        ))

    def test_headache_filter_does_not_restore_unrelated_candidates(self):
        candidates = [
            {"id": 1, "name": "ADVIL 200MG CO100", "brand": "Advil"},
            {"id": 2, "name": "ORAL-B IO TETE BR/DENTS BLC 4", "brand": "Oral-B"},
            {"id": 3, "name": "TYLENOL RH/SIN JR NT CA20", "brand": "Tylenol"},
        ]

        filtered = filter_client_answer_category(
            "Jai mal à la tête que prendre", candidates,
        )

        self.assertEqual([product["id"] for product in filtered], [1])

    def test_duplicate_plan_positions_are_one_product_with_all_locations(self):
        first = {
            "id": 1, "name": "Advil", "brand": "Advil", "barcode": "111",
            "aisle": "2", "side": "Gauche", "section": "1", "shelf": "2", "position": "1",
        }
        second = {
            **first, "id": 2, "aisle": "5", "side": "Droite", "shelf": "3", "position": "4",
        }
        corpus = [
            (first, search_row(first["name"], first["brand"], barcode="111")),
            (second, search_row(second["name"], second["brand"], barcode="111")),
        ]
        plan = {
            "corrected_query": "Advil", "search_queries": ["Advil"],
            "keywords": ["Advil"], "must_include": ["Advil"], "exclude": [],
        }
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates("Advil", plan, limit=10)

        self.assertEqual(len(matches), 1)
        self.assertEqual(len(matches[0]["locations"]), 2)

    def test_fast_client_lookup_handles_spoken_brand_typo(self):
        advil = {"id": 1, "name": "Advil Extra Fort", "brand": "Advil", "barcode": "111"}
        unrelated = {"id": 2, "name": "Tylenol Regular", "brand": "Tylenol", "barcode": "222"}
        corpus = [
            (advil, search_row(advil["name"], advil["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=Jai%20besoin%20dadvile")

        self.assertEqual(response.status_code, 200)
        self.assertEqual([item["name"] for item in response.get_json()], ["Advil Extra Fort"])

    def test_fast_client_lookup_can_return_more_than_sixty_matches(self):
        corpus = []
        for index in range(75):
            product = {
                "id": index + 1,
                "name": f"Advil variante {index + 1}",
                "brand": "Advil",
                "barcode": str(100000 + index),
            }
            corpus.append((
                product,
                search_row(product["name"], product["brand"], barcode=product["barcode"]),
            ))
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=Advil&limit=100")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()), 75)

    def test_fast_client_lookup_requires_an_electric_toothbrush_match(self):
        products = [
            {"id": 1, "name": "ORAL-B P100 BR/DENTS ELEC NR 1", "brand": "Oral-B", "barcode": "101"},
            {"id": 2, "name": "ORAL-B D/C BR/DENTS A PILE 1", "brand": "Oral-B", "barcode": "102"},
            {"id": 3, "name": "SONICARE BR/DENT BH1022/04 TE2", "brand": "Sonicare", "barcode": "103"},
            {"id": 4, "name": "SONICARE RECH BROS HX9023/64 3", "brand": "Sonicare", "barcode": "104"},
            {"id": 5, "name": "ORAL-B IO TETE BR/DENTS BLC 4", "brand": "Oral-B", "barcode": "105"},
            {"id": 6, "name": "GUM BR/DENT CRAYOLA MARQ/ELEC1", "brand": "Gum", "barcode": "106"},
            {"id": 11, "name": "PHILIPS SONI RECH HX6012/77 2", "brand": "Philips", "barcode": "111",
             "description": "Compatible avec les manches Sonicare."},
            {"id": 7, "name": "CURAPROX BR/DENT SMART 1", "brand": "Curaprox", "barcode": "107"},
            {"id": 8, "name": "DENTA RINSE PRO .2% MENT 500ML", "brand": "Denta", "barcode": "108"},
            {"id": 9, "name": "SONICARE IRR S/FIL HX3826/23 1", "brand": "Sonicare", "barcode": "109",
             "description": "Utilisé en complément d'une brosse à dents manuelle."},
            {"id": 10, "name": "GUM PROXABRUSH RECH BROS LG 10", "brand": "Gum", "barcode": "110"},
        ]
        corpus = [
            (product, search_row(
                product["name"], product["brand"], product.get("description", ""),
                barcode=product["barcode"],
            ))
            for product in products
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=brosse%20a%20dent%20electric&limit=100")

        self.assertEqual(response.status_code, 200)
        names = {item["name"] for item in response.get_json()}
        self.assertEqual(names, {product["name"] for product in products[:7]})
        self.assertNotIn("CURAPROX BR/DENT SMART 1", names)
        self.assertNotIn("DENTA RINSE PRO .2% MENT 500ML", names)
        self.assertNotIn("SONICARE IRR S/FIL HX3826/23 1", names)
        self.assertNotIn("GUM PROXABRUSH RECH BROS LG 10", names)

    def test_electric_toothbrush_concept_filter_is_precompiled_and_fast(self):
        query = "brosse a dent electric"
        groups = client_required_concept_groups(query)
        excluded = client_excluded_concept_terms(query)
        description = "rince bouche menthe protection fraicheur quotidienne " * 12
        rows = [
            search_row(f"DENTA RINSE MENTHE {index}", description=description)
            for index in range(12000)
        ]
        rows.append(search_row("ORAL-B P100 BR/DENTS ELEC NR 1"))

        started = time.perf_counter()
        matches = [row for row in rows if row_matches_client_concepts(row, groups, excluded)]
        elapsed = time.perf_counter() - started

        self.assertEqual(len(matches), 1)
        self.assertLess(elapsed, 0.8)

    def test_fast_client_lookup_understands_transparent_dressing_language(self):
        transparent = {
            "id": 1, "name": "PARAMEDIC PANS TRANSP 5CMX1M", "brand": "Paramedic",
            "barcode": "111",
        }
        unrelated = {"id": 2, "name": "BANDAGE ELASTIQUE", "brand": "Test", "barcode": "222"}
        nasal_strip = {
            "id": 3, "name": "BREATHE RIGHT BDE NAS TRANSP 30", "brand": "Breathe Right",
            "barcode": "333",
        }
        cup = {"id": 4, "name": "TASSE TRANSPARENTE", "brand": "Test", "barcode": "444"}
        corpus = [
            (transparent, search_row(transparent["name"], transparent["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
            (nasal_strip, search_row(nasal_strip["name"], nasal_strip["brand"], barcode="333")),
            (cup, search_row(cup["name"], cup["brand"], barcode="444")),
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get(
                    "/api/client/find?q=membrane%20transparente%20pour%20blessure"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["PARAMEDIC PANS TRANSP 5CMX1M"],
        )

    def test_fast_client_lookup_understands_watte_as_cotton_balls(self):
        cotton = {
            "id": 1, "name": "PERSONNEL OUATE BOULES 100", "brand": "Personnelle",
            "barcode": "111",
        }
        unrelated = {"id": 2, "name": "MOUCHOIRS 100", "brand": "Test", "barcode": "222"}
        cotton_swabs = {"id": 3, "name": "Q-TIPS COTONS-TIGES 400", "brand": "Q-Tips", "barcode": "333"}
        laxative = {"id": 4, "name": "CARTER PETITES PILULES LAX", "brand": "Carter", "barcode": "444"}
        bowls = {"id": 5, "name": "MUNCHKIN PETITS BOLS", "brand": "Munchkin", "barcode": "555"}
        corpus = [
            (cotton, search_row(cotton["name"], cotton["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
            (cotton_swabs, search_row(cotton_swabs["name"], cotton_swabs["brand"], barcode="333")),
            (laxative, search_row(laxative["name"], laxative["brand"], barcode="444")),
            (bowls, search_row(bowls["name"], bowls["brand"], barcode="555")),
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get(
                    "/api/client/find?q=je%20cherche%20de%20la%20watte%20des%20petites%20boules"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["PERSONNEL OUATE BOULES 100"],
        )

    def test_product_can_reuse_reference_catalogue_image_by_upc(self):
        class Result:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeDb:
            def __init__(self):
                self.calls = 0

            def execute(self, _query, _params):
                self.calls += 1
                if "product_reference_evidence" in _query:
                    return Result(rows=[{"field_value": "https://example.test/advil.jpg"}])
                return Result(None)

        image = find_existing_image_for_barcode(FakeDb(), "12345678")
        self.assertEqual(image, "https://example.test/advil.jpg")

    def test_assortment_retrieval_keeps_matching_flavours_only(self):
        strawberry = {"id": 1, "name": "Milk Strawberry", "brand": "Test", "barcode": "111"}
        banana = {"id": 2, "name": "Milk Banana", "brand": "Test", "barcode": "222"}
        medicine = {"id": 3, "name": "Advil", "brand": "Advil", "barcode": "333"}
        corpus = [
            (strawberry, search_row(strawberry["name"], description="fruit flavoured milk", barcode="111")),
            (banana, search_row(banana["name"], description="fruit flavoured milk", barcode="222")),
            (medicine, search_row(medicine["name"], medicine["brand"], barcode="333")),
        ]
        plan = {
            "corrected_query": "all fruit flavoured milk",
            "search_queries": ["fruit flavoured milk", "milk strawberry", "milk banana"],
            "keywords": ["milk", "fruit", "strawberry", "banana"],
            "must_include": ["milk", "fruit"],
            "exclude": [],
        }
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             patch("routes.products._reference_corpus", return_value=[]):
            matches = hybrid_client_candidates("Give all the fruit flavor milk", plan, limit=10)

        self.assertEqual({item["name"] for item in matches}, {"Milk Strawberry", "Milk Banana"})

    def test_long_melatonin_comparison_retrieves_the_product_not_answer_words(self):
        question = (
            "Peux tu me dire tout les type de melatonine les saveurs qu'on a en magasin "
            "et les difference de context dans lequel les utiliser ?"
        )
        self.assertEqual(tokenize_search_query(question), ["melatonine"])
        melatonin = [
            {
                "id": index, "name": f"WEBBER MELATON {index}MG CO60",
                "brand": "Webber", "barcode": str(1000 + index),
            }
            for index in range(1, 31)
        ]
        unrelated = [
            {
                "id": 1000 + index, "name": f"PRODUIT DIVERS {index}",
                "description": "Types, saveurs et contextes d'utilisation en magasin.",
                "barcode": str(5000 + index),
            }
            for index in range(120)
        ]
        corpus = [
            (
                product,
                search_row(
                    product["name"], product.get("brand", ""),
                    product.get("description", ""), product["barcode"],
                ),
            )
            for product in melatonin + unrelated
        ]
        plan = build_client_query_plan(question, "documented")

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(question, plan, limit=100)

        self.assertEqual(len(matches), 30)
        self.assertTrue(all("MELATON" in product["name"] for product in matches))

    def test_melatonin_comparison_excludes_bath_and_generic_sleep_products(self):
        candidates = [
            {"id": 1, "name": "WEBBER MELATON 5MG CO120"},
            {"id": 2, "name": "DR TEALS B/MOUS MELATON 1000ML"},
            {"id": 3, "name": "DR TEALS S/EPSOM MELATON 1.36KG"},
            {"id": 4, "name": "NERVIVE SOUL NERFS SOIR CO30", "description": "Avec mélatonine"},
        ]

        supplements = filter_client_answer_category(
            "Quels types de mélatonine avons-nous?", candidates,
        )
        bath_search = filter_client_answer_category(
            "Quels produits de bain Dr Teals avec mélatonine avons-nous?", candidates,
        )

        self.assertEqual([product["id"] for product in supplements], [1])
        self.assertIn(2, [product["id"] for product in bath_search])

    def test_verifier_can_only_return_real_candidate_ids(self):
        parsed = {
            "answer": "Advil",
            "selected_product_ids": ["product:1", "invented:99", "product:1"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
        }
        result = normalize_verified_client_answer(parsed, ["product:1"])
        self.assertEqual(result["selected_product_ids"], ["product:1"])

    def test_documented_answer_rejects_invented_products_and_sources(self):
        documents = [{"source_id": "store-plan"}, {"source_id": "health-canada:12"}]
        parsed = {
            "answer": "Réponse documentée.",
            "selected_product_ids": ["product:1", "invented:9"],
            "key_points": [{
                "heading": "Ingrédient", "detail": "Ibuprofène 200 mg.",
                "source_ids": ["health-canada:12", "invented-source"],
            }],
            "comparisons": [{
                "candidate_id": "invented:9", "difference": "Fausse différence",
                "practical_note": "", "source_ids": ["invented-source"],
            }],
            "useful_guidance": [], "important_checks": [],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
            "source_ids": ["health-canada:12", "invented-source"],
        }

        result = normalize_documented_client_answer(parsed, ["product:1"], documents)

        self.assertEqual(result["selected_product_ids"], ["product:1"])
        self.assertEqual(result["key_points"][0]["source_ids"], ["health-canada:12"])
        self.assertEqual(result["comparisons"], [])
        self.assertEqual(result["source_ids"], ["health-canada:12"])

    def test_documented_answer_keeps_products_when_ai_is_unavailable(self):
        product = {
            "id": 7,
            "client_id": "product:7",
            "name": "MELATONINE FRAISE 5 MG CO60",
            "description": "Comprimés à saveur de fraise.",
            "usage_notes": "Lire l'étiquette avant utilisation.",
            "aisle": "4", "side": "A", "section": "2", "shelf": "3", "position": "5",
        }
        documents = [{
            "source_id": "store-plan",
            "title": "Plan actuel du magasin",
            "candidate_ids": ["product:7"],
        }, {
            "source_id": "catalog:1",
            "title": "Fiche produit",
            "candidate_ids": ["product:7"],
        }]

        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                "Quelles saveurs de mélatonine avons-nous?",
                {"medical": False}, [product], documents,
            )

        self.assertTrue(result["degraded"])
        self.assertEqual(result["selected_product_ids"], ["product:7"])
        self.assertIn("comprimés", result["answer"])
        self.assertIn("5 mg", result["answer"])
        self.assertIn("fraise", result["answer"])
        self.assertEqual(result["comparisons"][0]["source_ids"], ["catalog:1"])
        self.assertLessEqual(len(result["comparisons"][0]["difference"]), 420)
        self.assertTrue(result["pharmacist_referral"])

    def test_documented_headache_fallback_remains_useful_when_ai_times_out(self):
        products = [{
            "id": 1, "client_id": "product:1",
            "name": "BIOMEDIC SOUL M/TETE ULT CO120", "brand": "Biomedic",
            "description": "Contient de l'acétaminophène.",
            "aisle": "Labo", "side": "A", "section": "2", "shelf": "3", "position": "4",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "ADVIL 200MG CO100", "brand": "Advil",
            "description": "Comprimés d'ibuprofène.",
            "aisle": "Labo", "side": "A", "section": "2", "shelf": "4", "position": "1",
        }]
        query_plan = build_client_query_plan(
            "Jai male a la tete que prendre", "documented",
        )
        documents = retrieve_client_documentation(products, query_plan)

        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                query_plan["corrected_query"], query_plan, products, documents,
            )

        self.assertTrue(result["degraded"])
        self.assertIn("mal de tête", result["answer"])
        self.assertIn("acétaminophène", result["answer"])
        self.assertEqual(
            [point["heading"] for point in result["key_points"]],
            ["Avant de choisir", "Ingrédients repérés", "Éviter les doublons", "Besoin d'un avis"],
        )
        self.assertEqual(len(result["follow_up_questions"]), 3)
        self.assertTrue(result["pharmacist_referral"])
        self.assertIn(
            "health-canada:acetaminophen-safe-use",
            result["source_ids"],
        )

    def test_documented_toothbrush_fallback_compares_power_types(self):
        products = [{
            "client_id": "product:1", "name": "ORAL-B BR/DENTS A PILE 1",
            "description": "Brosse à dents électrique à pile.",
            "aisle": "3", "side": "A", "section": "5", "shelf": "2", "position": "1",
        }, {
            "client_id": "product:2", "name": "PHILIPS ONE BR/DENTS RECH NR 1",
            "description": "Brosse à dents rechargeable.",
            "aisle": "3", "side": "A", "section": "5", "shelf": "2", "position": "2",
        }, {
            "client_id": "product:3", "name": "ORAL-B TETE BR/DENTS S/G SPL 3",
            "description": "Têtes de remplacement.",
            "aisle": "3", "side": "A", "section": "5", "shelf": "2", "position": "3",
        }]
        documents = [{
            "source_id": "store-plan", "title": "Plan actuel du magasin",
            "candidate_ids": [product["client_id"] for product in products],
        }]
        query_plan = {
            "corrected_query": (
                "Quelle est la différence entre les brosses à dents électriques à pile "
                "et rechargeables?"
            ),
            "medical": False,
        }

        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                query_plan["corrected_query"], query_plan, products, documents,
            )

        self.assertTrue(result["degraded"])
        self.assertIn("brosse à pile", result["answer"])
        self.assertIn("brosse rechargeable", result["answer"])
        self.assertIn("tête de remplacement", result["answer"])
        self.assertNotIn("saveur", result["answer"].lower())
        self.assertNotIn("concentration", result["answer"].lower())
        self.assertFalse(result["pharmacist_referral"])
        self.assertEqual(
            [point["heading"] for point in result["key_points"]],
            ["Modèles à pile", "Modèles rechargeables", "Têtes de remplacement"],
        )

    def test_melatonin_documentation_skips_the_inapplicable_drug_database(self):
        products = [
            {"client_id": "product:1", "name": "A GAGNON MELATON 5MG GUM 120"},
            {"client_id": "product:2", "name": "WEBBER MELATON 10MG CO 120"},
        ]
        with patch("routes.ai._health_canada_json") as lookup:
            documents = health_canada_documents(products)

        self.assertEqual(documents, [])
        lookup.assert_not_called()

    def test_unconfirmed_identifiers_never_attach_regulatory_documents(self):
        products = [{
            "client_id": "product:1",
            "name": "Possible regulated product",
            "_identifiers": [
                {
                    "type": "DIN", "value": "01234567",
                    "verification_status": "requires_review",
                },
                {
                    "type": "NPN", "value": "80123456",
                    "verification_status": "requires_review",
                },
            ],
        }]
        with patch("routes.ai._health_canada_json") as drug_lookup, \
             patch("routes.ai._health_canada_nhp_json") as nhp_lookup:
            self.assertEqual(health_canada_documents(products), [])
            self.assertEqual(health_canada_nhp_documents(products), [])

        drug_lookup.assert_not_called()
        nhp_lookup.assert_not_called()

    def test_unconfirmed_identifier_notice_is_mandatory_only_when_relevant(self):
        uncertain_product = {
            "name": "Possible regulated product",
            "_identifiers": [{
                "type": "DIN", "value": "01234567",
                "verification_status": "requires_review",
            }],
        }
        notice = _unconfirmed_identifier_notice(
            "Je cherche le DIN 01234567",
            "Voici le produit possible.",
            [uncertain_product],
        )
        self.assertIn("DIN 01234567", notice)
        self.assertIn("non confirmée", notice)
        self.assertIn("peut être incorrecte", notice)
        self.assertIn("emballage", notice)
        self.assertEqual(
            _unconfirmed_identifier_notice(
                "Montre-moi ce produit",
                "Voici le produit possible.",
                [uncertain_product],
            ),
            "",
        )
        self.assertEqual(
            _unconfirmed_identifier_notice(
                "Je cherche le DIN 01234567",
                "Voici le produit.",
                [{
                    "name": "Verified product",
                    "_identifiers": [{
                        "type": "DIN", "value": "01234567",
                        "verification_status": "verified",
                    }],
                }],
            ),
            "",
        )

    def test_documented_deepseek_timeout_retries_fast_model_without_thinking(self):
        response_payload = {
            "usage": {"prompt_tokens": 12, "completion_tokens": 6},
            "choices": [{"message": {"content": json.dumps({"answer": "ok"})}}],
        }

        class StubResponse:
            def __enter__(self):
                return self

            def __exit__(self, _type, _value, _traceback):
                return False

            def read(self, _size=-1):
                return json.dumps(response_payload).encode("utf-8")

        with patch("routes.ai.DEEPSEEK_DOCUMENTED_MODEL", "deepseek-v4-pro"), \
             patch("routes.ai.DEEPSEEK_MODEL", "deepseek-v4-flash"), \
             patch("routes.ai._DEEPSEEK_DOCUMENTED_THINKING", False), \
             patch("routes.ai._safe_urlopen", side_effect=[TimeoutError("slow"), StubResponse()]) as opener, \
             patch("routes.ai._log_ai_usage"):
            result = _deepseek_json_request(
                [{"role": "user", "content": "test"}],
                max_tokens=3200, quality_mode=True,
            )

        self.assertEqual(result, {"answer": "ok"})
        self.assertEqual(opener.call_count, 2)
        first_payload = json.loads(opener.call_args_list[0].args[0].data.decode("utf-8"))
        second_payload = json.loads(opener.call_args_list[1].args[0].data.decode("utf-8"))
        self.assertEqual(first_payload["model"], "deepseek-v4-pro")
        self.assertEqual(first_payload["thinking"], {"type": "disabled"})
        self.assertEqual(second_payload["model"], "deepseek-v4-flash")
        self.assertEqual(second_payload["thinking"], {"type": "disabled"})

    def test_small_ai_context_keeps_different_product_forms(self):
        candidates = [
            {"id": index, "name": f"Advil 200 mg liqui-gel {index}"}
            for index in range(1, 12)
        ] + [
            {"id": 20, "name": "Advil enfants suspension liquide"},
            {"id": 21, "name": "Advil comprimes 200 mg"},
        ]

        selected = select_client_answer_candidates(candidates, limit=4)

        self.assertIn(20, [item["id"] for item in selected])
        self.assertIn(21, [item["id"] for item in selected])

    def test_request_router_separates_fast_lookup_from_detailed_advice(self):
        self.assertEqual(classify_client_request("Advil"), "lookup")
        self.assertEqual(classify_client_request("Montre-moi les Advil"), "lookup")
        self.assertEqual(
            classify_client_request("Quelle est la différence entre liquide et comprimés?"),
            "detailed",
        )
        self.assertEqual(classify_client_request("Que prendre pour la fièvre?"), "detailed")
        self.assertEqual(classify_client_request("Quel Advil pour enfant?"), "detailed")
        self.assertEqual(classify_client_request("Et le liquide?", follow_up=True), "detailed")

    def test_client_retrieval_never_uses_reference_catalogue(self):
        placed = {"id": 1, "name": "Tylenol", "brand": "Tylenol", "barcode": "111"}
        corpus = [(placed, search_row(placed["name"], placed["brand"], barcode="111"))]
        plan = {
            "corrected_query": "Advil", "search_queries": ["Advil"],
            "keywords": ["Advil"], "must_include": ["Advil"], "exclude": [],
        }
        reference_mock = patch("routes.products._reference_corpus")
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             reference_mock as reference:
            matches = hybrid_client_candidates("Advil", plan, limit=10)

        self.assertEqual(matches, [])
        reference.assert_not_called()

    def test_client_endpoint_returns_only_ai_verified_products(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        second_candidate = {
            "id": 2, "client_id": "product:2", "name": "Advil Enfants", "brand": "Advil",
            "barcode": "222", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "3", "in_stock": 1,
        }
        plan = {
            "intent": "specific_product", "corrected_query": "Advil",
            "search_queries": ["Advil"], "keywords": ["Advil"],
            "must_include": ["Advil"], "exclude": [], "wants_all": False,
            "needs_comparison": False, "answer_language": "fr", "medical": True,
        }
        verified = {
            "answer": "Advil est le produit trouve.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek", "label": "DeepSeek", "model": "test"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.generate_client_query_plan") as old_planner, \
             patch("routes.products.hybrid_client_candidates", return_value=[candidate, second_candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.generate_verified_client_answer", return_value=verified) as verifier, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Et pour les enfants?",
                    "history": [{"role": "user", "content": "Jai besoin dadvile"}],
                    "follow_up": True,
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["response_mode"], "detailed")
        self.assertEqual([item["client_id"] for item in payload["products"]], ["product:1"])
        self.assertEqual(payload["highlighted_product_ids"], ["product:1"])
        old_planner.assert_not_called()
        verifier.assert_called_once()

    def test_simple_product_lookup_does_not_call_ai(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai.generate_verified_client_answer") as verifier:
            with app.test_client() as client:
                response = client.post("/api/client/help", json={"question": "Advil"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["response_mode"], "lookup")
        self.assertEqual(payload["products"][0]["name"], "Advil")
        provider.assert_not_called()
        verifier.assert_not_called()

    def test_explicit_ai_mode_forces_grounded_answer_for_simple_name(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        verified = {
            "answer": "Advil est disponible dans le plan.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.generate_verified_client_answer", return_value=verified) as verifier, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Advil", "mode": "ai",
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["response_mode"], "detailed")
        self.assertEqual([item["client_id"] for item in payload["products"]], ["product:1"])
        verifier.assert_called_once()

    def test_client_endpoint_forces_uncertain_identifier_warning(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Possible product",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
            "_identifiers": [{
                "type": "DIN", "value": "01234567",
                "verification_status": "requires_review",
            }],
        }
        verified = {
            "answer": "Voici le produit possiblement associé.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.generate_verified_client_answer", return_value=verified), \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Je cherche le DIN 01234567", "mode": "ai",
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("DIN 01234567", payload["answer"])
        self.assertIn("confirmer le numéro", payload["answer"])
        self.assertIn(
            "DIN 01234567",
            payload["advice"]["safety_flags"][0],
        )

    def test_explicit_fast_mode_never_calls_ai_for_detailed_question(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai.generate_verified_client_answer") as verifier:
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Quelle est la différence?", "mode": "fast",
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["response_mode"], "lookup")
        provider.assert_not_called()
        verifier.assert_not_called()

    def test_explicit_documented_mode_returns_structured_sources(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil 200 mg",
            "brand": "Advil", "description": "Comprimés", "barcode": "111",
            "aisle": "2", "side": "Gauche", "section": "1", "shelf": "3",
            "position": "2", "in_stock": 1,
        }
        documents = [{
            "source_id": "health-canada:12", "title": "Santé Canada - ADVIL",
            "publisher": "Santé Canada", "url": "https://example.test/source",
            "evidence": "Ibuprofène 200 mg", "candidate_ids": ["product:1"],
        }]
        documented = {
            "answer": "Ce format contient 200 mg d'ibuprofène.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": ["Quel âge a la personne?"],
            "safety_flags": [], "pharmacist_referral": False,
            "pharmacist_reason": "", "source_ids": ["health-canada:12"],
            "key_points": [{
                "heading": "Ingrédient", "detail": "Ibuprofène 200 mg",
                "source_ids": ["health-canada:12"],
            }],
            "comparisons": [], "useful_guidance": [], "important_checks": [],
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents) as retriever, \
             patch("routes.ai.generate_documented_client_answer", return_value=documented) as generator, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Explique-moi cet Advil en détail", "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["response_mode"], "documented")
        self.assertEqual(payload["advice"]["documentation"]["key_points"][0]["heading"], "Ingrédient")
        self.assertEqual(payload["advice"]["documentation"]["sources"][0]["publisher"], "Santé Canada")
        retriever.assert_called_once()
        generator.assert_called_once()

    def test_documented_melatonin_assortment_uses_immediate_grounded_summary(self):
        names = [
            "WEBBER MELATON 5MG CO120",
            "A GAGNON MELATON 10MG CA90",
            "JAMI MELATON 2.5MG GUM60",
            "LANDART MELATON 3MG LIQ50ML",
        ]
        candidates = [{
            "id": index,
            "client_id": f"product:{index}",
            "name": names[(index - 1) % len(names)],
            "barcode": str(1000 + index), "aisle": "1", "side": "B",
            "section": "7", "shelf": "2", "position": str(index), "in_stock": 1,
        } for index in range(1, 13)]
        documents = [{
            "source_id": "store-plan", "title": "Plan actuel",
            "publisher": "Familiprix Locator", "url": "", "evidence": "",
            "candidate_ids": [product["client_id"] for product in candidates],
        }]
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents) as retriever, \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Montre tous les types et saveurs de mélatonine",
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["degraded"])
        self.assertIn("comprimés", payload["answer"])
        self.assertIn("2.5 mg, 3 mg, 5 mg, 10 mg", payload["answer"])
        generator.assert_not_called()
        provider.assert_not_called()
        rate_limit.assert_not_called()
        retriever.assert_called_once_with(
            unittest.mock.ANY,
            unittest.mock.ANY,
            include_live_regulatory=False,
        )

    def test_documented_toothbrush_power_comparison_skips_ai_delay(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "ORAL-B BR/DENTS A PILE 1", "barcode": "1001",
            "aisle": "3", "side": "A", "section": "5", "shelf": "2", "position": "1",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "PHILIPS ONE BR/DENTS RECH NR 1", "barcode": "1002",
            "aisle": "3", "side": "A", "section": "5", "shelf": "2", "position": "2",
        }]
        documents = [{
            "source_id": "store-plan", "title": "Plan actuel",
            "publisher": "Familiprix Locator", "url": "", "evidence": "",
            "candidate_ids": [product["client_id"] for product in candidates],
        }]
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents) as retriever, \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": (
                        "Quelle est la différence entre les brosses à dents électriques "
                        "à pile et rechargeables?"
                    ),
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["degraded"])
        self.assertIn("brosse à pile", payload["answer"])
        self.assertIn("brosse rechargeable", payload["answer"])
        generator.assert_not_called()
        provider.assert_not_called()
        rate_limit.assert_not_called()
        retriever.assert_called_once_with(
            unittest.mock.ANY,
            unittest.mock.ANY,
            include_live_regulatory=False,
        )

    def test_documented_headache_question_uses_immediate_grounded_summary(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "BIOMEDIC SOUL M/TETE ULT CO120", "brand": "Biomedic",
            "description": "Contient de l'acétaminophène.",
            "barcode": "1001", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "3", "position": "4",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "ADVIL 200MG CO100", "brand": "Advil",
            "description": "Comprimés d'ibuprofène.",
            "barcode": "1002", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "4", "position": "1",
        }]
        documents = [{
            "source_id": "store-plan", "title": "Plan actuel",
            "publisher": "Familiprix Locator", "url": "", "evidence": "",
            "candidate_ids": [product["client_id"] for product in candidates],
        }, {
            "source_id": "health-canada:acetaminophen-safe-use",
            "title": "Acétaminophène", "publisher": "Santé Canada",
            "url": "https://www.canada.ca/",
            "evidence": "Lire l'étiquette et éviter les doublons.",
            "candidate_ids": [],
        }]
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents) as retriever, \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Jai male a la tete que prendre",
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["degraded"])
        self.assertEqual(payload["warning"], "")
        self.assertIn("mal de tête", payload["answer"])
        self.assertIn("acétaminophène", payload["answer"])
        self.assertEqual(
            [point["heading"] for point in payload["advice"]["documentation"]["key_points"]],
            ["Avant de choisir", "Ingrédients repérés", "Éviter les doublons", "Besoin d'un avis"],
        )
        generator.assert_not_called()
        provider.assert_not_called()
        rate_limit.assert_not_called()
        retriever.assert_called_once_with(
            unittest.mock.ANY,
            unittest.mock.ANY,
            include_live_regulatory=False,
        )


if __name__ == "__main__":
    unittest.main()
