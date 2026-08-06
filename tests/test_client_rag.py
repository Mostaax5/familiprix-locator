import json
import time
import unittest
from unittest.mock import MagicMock, patch

from app import app
from routes import ai as ai_module
from routes import products as products_module
from routes.ai import (
    _compact_documented_product_context,
    _description_excerpt_for_ai,
    _guard_documented_inventory_contradiction,
    _outbound_url_allowed,
    _deepseek_json_request,
    _kimi_json_request,
    _read_kimi_stream,
    build_client_query_plan,
    classify_client_request,
    filter_client_answer_category,
    generate_documented_client_answer,
    health_canada_documents,
    health_canada_nhp_documents,
    lookup_familiprix_product,
    normalize_documented_client_answer,
    normalize_verified_client_answer,
    normalize_url,
    parse_familiprix_product_page,
    retrieve_client_documentation,
    select_client_answer_candidates,
    structured_catalog_description,
    _unconfirmed_identifier_notice,
)
from routes.products import (
    classify_client_result_roles,
    client_candidates_need_semantic_retry,
    client_exact_identifier_queries,
    client_excluded_concept_terms,
    client_required_concept_groups,
    filter_client_request_products,
    find_existing_image_for_barcode,
    hybrid_client_candidates,
    normalize_search_text,
    product_matches_client_request,
    resolve_client_exact_identifiers,
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
    def setUp(self):
        products_module._PROD_CACHE.update(
            key=None, rows=[], built_at=0.0, database_token=None,
            generation=-1, state_checked_at=0.0,
            statistics_rows_id=0,
            token_postings={}, token_prefixes={},
            name_token_postings={}, name_tokens_by_initial={},
            mapped_indices_by_key={}, document_in_stock={},
            representative_indices=(), document_barcodes=(),
            identifier_postings={}, product_id_to_key={},
            category_postings={},
        )
        with ai_module._DOCUMENTED_ANSWER_CACHE_LOCK:
            ai_module._DOCUMENTED_ANSWER_CACHE.clear()
            ai_module._DOCUMENTED_JOBS.clear()
        with ai_module._CLIENT_QUERY_PLAN_CACHE_LOCK:
            ai_module._CLIENT_QUERY_PLAN_CACHE.clear()

    def test_outbound_lookup_urls_cannot_reach_other_or_insecure_hosts(self):
        base = "https://example.com/catalog"
        self.assertEqual(
            normalize_url(base, "/product/123"),
            "https://example.com/product/123",
        )
        self.assertEqual(normalize_url(base, "https://169.254.169.254/latest"), "")
        self.assertEqual(normalize_url(base, "http://example.com/product/123"), "")
        self.assertFalse(_outbound_url_allowed("https://user:pass@example.com/private"))

    def test_search_normalization_preserves_french_and_special_latin_words(self):
        self.assertEqual(
            normalize_search_text("c\u0153ur lev\u00e9, cr\u00e8me et go\u00fbt m\u00fbre"),
            "coeur leve creme et gout mure",
        )
        self.assertEqual(
            normalize_search_text("B\u00d8RN \u00c6sir"), "born aesir",
        )

    def test_local_query_plan_does_not_search_conversation_words_separately(self):
        question = (
            "Quels types de melatonine avons-nous, quelles differences et "
            "comment choisir entre les formats?"
        )
        plan = build_client_query_plan(question, "detailed")
        self.assertEqual(plan["search_queries"], [question])
        self.assertEqual(plan["keywords"], [])

    def test_semantic_plan_separates_product_family_from_choice_constraints(self):
        question = "Quel type de chips choisir si je ne veux pas de sucre?"
        plan = ai_module.normalize_client_query_plan({
            "intent": "food_comparison",
            "answer_goal": "Comparer les croustilles du magasin selon leur sucre.",
            "product_family": "croustilles",
            "corrected_query": "croustilles avec le moins de sucre",
            "search_queries": ["croustilles", "chips", "potato chips"],
            "keywords": ["croustilles", "chips", "sucre"],
            "must_include": ["croustilles"],
            "constraints": ["le moins de sucre possible"],
            "evidence_fields": ["nutrition", "sucres"],
            "exclude": [],
            "wants_all": False,
            "needs_comparison": True,
            "answer_language": "fr",
            "medical": False,
            "retrieval_scope": "store",
        }, question)

        self.assertEqual(plan["product_family"], "croustilles")
        self.assertEqual(plan["must_include"], ["croustilles"])
        self.assertEqual(plan["constraints"], ["le moins de sucre possible"])
        self.assertEqual(plan["evidence_fields"], ["nutrition", "sucres"])

    def test_compact_semantic_plan_derives_safe_retrieval_defaults(self):
        question = "Montre-moi toutes les options et explique leurs différences"
        plan = ai_module.normalize_client_query_plan({
            "product_family": "crèmes hydratantes pour le visage",
            "answer_goal": "Comparer les crèmes hydratantes disponibles.",
            "corrected_query": "crèmes hydratantes visage",
            "search_queries": ["crème hydratante visage", "face moisturizer"],
            "constraints": ["pour peau sensible"],
            "evidence_fields": ["type de peau", "parfum", "format"],
            "exclude": ["crème pour le corps"],
            "answer_language": "fr",
            "medical": False,
            "retrieval_scope": "store",
        }, question)

        self.assertEqual(
            plan["must_include"], ["crèmes hydratantes pour le visage"]
        )
        self.assertEqual(
            plan["search_queries"][0], "crèmes hydratantes pour le visage"
        )
        self.assertTrue(plan["wants_all"])
        self.assertTrue(plan["needs_comparison"])

    def test_semantic_family_retrieval_does_not_let_constraint_words_take_over(self):
        products = [{
            "id": 1, "name": "ESSENTIEL CROUST NAT 16X150G",
            "brand": "Essentiel", "barcode": "101",
            "description": "Croustilles nature.",
        }, {
            "id": 2, "name": "ESSENTIEL CROUST BBQ 16X150G",
            "brand": "Essentiel", "barcode": "102",
            "description": "Croustilles saveur barbecue.",
        }, {
            "id": 3, "name": "ESSENTIEL CROUST KETCH 16X150G",
            "brand": "Essentiel", "barcode": "103",
            "description": "Croustilles saveur ketchup.",
        }, {
            "id": 4, "name": "METAMUCIL ORA S/SUCRE 662G",
            "brand": "Metamucil", "barcode": "104",
            "description": "Poudre de fibres sans sucre.",
        }, {
            "id": 5, "name": "A GAGNON MELATON S/SUCRE GUM45",
            "brand": "Adrien Gagnon", "barcode": "105",
            "description": "Mélatonine en gommes sans sucre.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in products]
        plan = {
            "intent": "food_comparison",
            "answer_goal": "Comparer les croustilles selon leur sucre.",
            "product_family": "croustilles",
            "corrected_query": "croustilles avec le moins de sucre",
            "search_queries": ["croustilles", "chips", "potato chips"],
            "keywords": ["croustilles", "chips", "sucre"],
            "must_include": ["croustilles"],
            "constraints": ["le moins de sucre possible"],
            "evidence_fields": ["nutrition", "sucres"],
            "exclude": [], "wants_all": False,
            "needs_comparison": True, "answer_language": "fr",
            "medical": False, "retrieval_scope": "store",
        }

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(
                plan["corrected_query"], plan, limit=20,
            )

        self.assertEqual(
            {product["name"] for product in matches},
            {
                "ESSENTIEL CROUST NAT 16X150G",
                "ESSENTIEL CROUST BBQ 16X150G",
                "ESSENTIEL CROUST KETCH 16X150G",
            },
        )

    def test_real_chips_wording_is_recovered_by_independent_category_embedding(self):
        category = "Aliments et breuvages > Collations > Croustilles"
        products = [{
            "id": 1, "client_id": "product:1",
            "name": "ESSENTIEL CROUST NAT 16X150G",
            "brand": "Essentiel", "barcode": "101",
            "description": "Croustilles nature.",
            "_catalogue_category": category,
        }, {
            "id": 2, "client_id": "product:2",
            "name": "ESSENTIEL CROUST BBQ 16X150G",
            "brand": "Essentiel", "barcode": "102",
            "description": "Croustilles saveur barbecue.",
            "_catalogue_category": category,
        }, {
            "id": 3, "client_id": "product:3",
            "name": "ESSENTIEL CROUST KETCH 16X150G",
            "brand": "Essentiel", "barcode": "103",
            "description": "Croustilles saveur ketchup.",
            "_catalogue_category": category,
        }, {
            "id": 4, "client_id": "product:4",
            "name": "METAMUCIL ORA S/SUCRE 662G",
            "brand": "Metamucil", "barcode": "104",
            "description": "Poudre de fibres sans sucre.",
            "_catalogue_category": "Sante > Fibres",
        }, {
            "id": 5, "client_id": "product:5",
            "name": "MELATONINE S/SUCRE GUM45",
            "brand": "Test", "barcode": "105",
            "description": "Melatonine en gommes sans sucre.",
            "_catalogue_category": "Sante > Sommeil",
        }, {
            "id": 6, "client_id": "product:6",
            "name": "IMPERIAL M/SOUFFLE BBQ 16X300G",
            "brand": "Imperial", "barcode": "106",
            "description": "Mais souffle saveur barbecue.",
            # A real catalogue can group sibling snacks under a coarse leaf.
            "_catalogue_category": category,
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in products]
        question = "quelle chips sont les moins sucre"
        plan = build_client_query_plan(question, "documented")
        plan["semantic_search"] = True
        semantic_hits = [{
            "kind": "category", "rank": 1, "product_id": 0,
            "barcode": "", "category": category, "similarity": 0.82,
        }]

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             patch("routes.products.semantic_product_hits", return_value=semantic_hits):
            matches = hybrid_client_candidates(question, plan, limit=20)

        self.assertEqual(
            {product["name"] for product in matches[:3]},
            {
                "ESSENTIEL CROUST NAT 16X150G",
                "ESSENTIEL CROUST BBQ 16X150G",
                "ESSENTIEL CROUST KETCH 16X150G",
            },
        )
        self.assertEqual(matches[3]["name"], "IMPERIAL M/SOUFFLE BBQ 16X300G")
        fallback = ai_module.grounded_documented_fallback(
            plan, matches, [],
        )
        self.assertEqual(
            fallback["selected_product_ids"],
            ["product:1", "product:2", "product:3"],
        )

    def test_documented_timeout_keeps_semantic_family_and_drops_constraint_noise(self):
        products = [{
            "id": 1, "client_id": "product:1",
            "name": "ESSENTIEL CROUST NAT 16X150G",
            "description": "Croustilles nature.",
            "_retrieval_sources": ["semantic_category", "lexical"],
        }, {
            "id": 2, "client_id": "product:2",
            "name": "ESSENTIEL CROUST BBQ 16X150G",
            "description": "Croustilles saveur barbecue.",
            "_retrieval_sources": ["semantic_category"],
        }, {
            "id": 3, "client_id": "product:3",
            "name": "METAMUCIL ORA S/SUCRE 662G",
            "description": "Poudre de fibres sans sucre.",
            "_retrieval_sources": ["lexical"],
        }]
        question = "quelle chips sont les moins sucre"
        fallback = ai_module.grounded_documented_fallback(
            build_client_query_plan(question, "documented"), products, [],
        )

        self.assertEqual(
            fallback["selected_product_ids"],
            ["product:1", "product:2"],
        )
        self.assertIn("ESSENTIEL CROUST NAT", fallback["answer"])
        self.assertNotIn("METAMUCIL", fallback["answer"])

    def test_exact_identifier_extraction_accepts_label_before_or_after_code(self):
        self.assertEqual(
            client_exact_identifier_queries(
                "063848907665 c'est le UPC du produit en question"
            ),
            [{"field": "upc", "value": "063848907665"}],
        )
        self.assertEqual(
            client_exact_identifier_queries("DIN-HM 80012345"),
            [{"field": "din_hm", "value": "80012345"}],
        )
        self.assertEqual(
            client_exact_identifier_queries("code pharmacie 146962"),
            [{"field": "familiprix_code", "value": "146962"}],
        )

    def test_exact_upc_resolution_cannot_be_erased_by_question_words(self):
        exact = {
            "id": 1884, "name": "BIOMEDIC GEL ANALG GLACE 255G",
            "brand": "Biomedic", "barcode": "063848907665",
            "product_code": "146962",
            "description": "Gel for temporary muscular and joint pain relief.",
            "aisle": "Labo", "side": "Gauche", "section": "3",
            "shelf": "2", "position": "5", "in_stock": 1,
            "_identifiers": [], "_verified_fields": ["description"],
        }
        noise = {
            "id": 2, "name": "EDGE GEL RASER PEAU SENSIBLE",
            "brand": "Edge", "barcode": "841058005209",
            "description": "Shaving gel for sensitive skin.",
            "aisle": "5", "side": "Gauche", "section": "1",
            "shelf": "1", "position": "1", "in_stock": 1,
            "_identifiers": [], "_verified_fields": [],
        }
        question = (
            "063848907665 c'est le UPC. Est-ce que le produit est liquide "
            "ou en gel?"
        )
        with patch(
            "routes.products.get_db", return_value=MagicMock(),
        ), patch(
            "routes.products._direct_identifier_products",
            return_value=[noise, exact],
        ):
            matches = resolve_client_exact_identifiers(question)

        self.assertEqual([item["id"] for item in matches], [1884])
        self.assertEqual(
            matches[0]["_exact_identifier_matches"],
            [{"field": "upc", "value": "063848907665"}],
        )
        context = _compact_documented_product_context(
            matches[0], include_identifiers=True,
        )
        self.assertEqual(
            context["catalogue_description"]["text"],
            exact["description"],
        )
        self.assertEqual(
            context["catalogue_identifiers"]["upc_or_gtin"],
            "063848907665",
        )
        self.assertEqual(
            context["matched_identifiers"][0]["match"], "exact",
        )
        fallback = ai_module.grounded_documented_fallback(
            build_client_query_plan(question, "documented"), matches, [],
        )
        self.assertIn("BIOMEDIC GEL ANALG GLACE 255G", fallback["answer"])
        self.assertIn("gel topique", fallback["answer"])
        self.assertNotIn("aucun produit", fallback["answer"].lower())

    def test_unconfirmed_din_is_exactly_searchable_without_becoming_verified(self):
        product = {
            "id": 5, "name": "TEST PACKAGE", "brand": "Test",
            "barcode": "012345678905", "description": "Test description.",
            "aisle": "1", "side": "Gauche", "section": "1",
            "shelf": "1", "position": "1", "in_stock": 1,
            "_verified_fields": [],
            "_identifiers": [{
                "type": "DIN", "value": "00559407",
                "verification_status": "requires_review",
            }],
        }
        with patch(
            "routes.products.get_db", return_value=MagicMock(),
        ), patch(
            "routes.products._direct_identifier_products",
            return_value=[product],
        ):
            matches = resolve_client_exact_identifiers(
                "Le DIN 00559407 correspond a quoi?"
            )

        self.assertEqual([item["id"] for item in matches], [5])
        self.assertEqual(
            matches[0]["_identifiers"][0]["verification_status"],
            "requires_review",
        )

    def test_semantic_retry_ignores_conversational_comparison_words(self):
        question = (
            "Quels types de melatonine avons-nous en magasin, quelles sont "
            "leurs differences et comment choisir entre les formats?"
        )
        candidates = [{
            "name": "BIOMEDIC MELATON 5MG CO100",
            "description": "Melatonine 5 mg en comprimes.",
        }]
        self.assertFalse(
            client_candidates_need_semantic_retry(question, candidates)
        )

    def test_structured_description_uses_only_known_package_fields(self):
        description = structured_catalog_description({
            "name": "ACME VITAMINE C 500MG CO100",
            "brand": "Acme",
            "package_size": "100",
            "package_unit": "comprimés",
            "strength": "500 mg",
            "flavour": "orange",
        })
        self.assertIn("ACME VITAMINE C 500MG CO100", description)
        self.assertIn("Saveur ou parfum: orange", description)
        self.assertNotIn("prévient", description.lower())
        self.assertNotIn("traite", description.lower())

    def test_structured_description_marks_sparse_identity_for_confirmation(self):
        description = structured_catalog_description({
            "name": "PRODUIT TEST",
        })
        self.assertIn("nom exact", description)
        self.assertIn("confirmer", description)

    def test_electric_toothbrush_results_group_heads_after_brushes(self):
        products = classify_client_result_roles([
            {"name": "ORAL-B IO TETE BR/DENTS BLC 4", "in_stock": 1},
            {"name": "ORAL-B PRO BROSSE A DENTS ELECTRIQUE", "in_stock": 1},
        ], "brosse à dents électrique")
        self.assertEqual(
            [product["result_role"] for product in products],
            ["primary", "replacement"],
        )
        self.assertIn("remplacement", products[1]["result_role_label"].lower())

    def test_abbreviated_replacement_brushes_are_not_main_toothbrushes(self):
        products = classify_client_result_roles([{
            "name": "SONICARE BR/DENTS HX3681/03 1",
        }, {
            "name": "SONICARE RECH BROS HX9023/64 3",
        }, {
            "name": "PHILIPS SONI RECH HX6012/77 2",
        }], "brosse a dents electrique")
        self.assertEqual(
            [product["result_role"] for product in products],
            ["primary", "replacement", "replacement"],
        )

    def test_fast_inventory_filter_honours_rechargeable_without_heads(self):
        products = [{
            "name": "SONICARE BR/DENTS HX3681/03 1",
            "description": "Brosse a dents electrique rechargeable avec USB.",
            "_verified_fields": ["description"],
        }, {
            "name": "ORAL-B P100 BR/DENTS ELEC NR 1",
            "description": "Brosse a dents a pile.",
            "_verified_fields": ["description"],
        }, {
            "name": "SONICARE RECH BROS HX9023/64 3",
            "description": "Tetes de rechange pour brosse rechargeable.",
            "_verified_fields": ["description"],
        }]
        filtered = filter_client_request_products(
            products,
            "brosse a dents electrique rechargeable, pas des tetes de remplacement",
        )
        self.assertEqual(
            [product["name"] for product in filtered],
            ["SONICARE BR/DENTS HX3681/03 1"],
        )

    def test_explicit_replacement_head_query_promotes_heads(self):
        products = classify_client_result_roles([
            {"name": "ORAL-B PRO BROSSE A DENTS ELECTRIQUE", "in_stock": 1},
            {"name": "ORAL-B TETE DE RECHANGE", "in_stock": 1},
        ], "tête de rechange Oral-B")
        self.assertEqual(products[0]["name"], "ORAL-B TETE DE RECHANGE")
        self.assertEqual(products[0]["result_role"], "primary")
        self.assertEqual(products[1]["result_role"], "related")

    def test_main_product_is_not_demoted_by_accessories_in_its_description(self):
        products = classify_client_result_roles([{
            "name": "ORAL-B PRO BROSSE A DENTS ELECTRIQUE",
            "category": "Brosse à dents électrique",
            "description": (
                "Inclut un chargeur et accepte plusieurs têtes de remplacement."
            ),
        }], "brosse à dents électrique")
        self.assertEqual(products[0]["result_role"], "primary")

    def test_rechargeable_brush_request_excludes_battery_models_and_heads(self):
        products = [{
            "name": "SONICARE BROSSE A DENTS ELECTRIQUE",
            "description": "Brosse rechargeable avec cable USB.",
        }, {
            "name": "ORAL-B BROSSE A DENTS A PILE",
            "description": "Brosse electrique alimentee par piles.",
        }, {
            "name": "ORAL-B TETE DE REMPLACEMENT",
            "description": "Brossette compatible avec un manche rechargeable.",
        }]

        filtered = filter_client_answer_category(
            "brosse a dents electrique rechargeable, pas des tetes de remplacement",
            products,
        )

        self.assertEqual(
            [product["name"] for product in filtered],
            ["SONICARE BROSSE A DENTS ELECTRIQUE"],
        )

    def test_documented_answer_cache_reuses_identical_evidence(self):
        with ai_module._DOCUMENTED_ANSWER_CACHE_LOCK:
            ai_module._DOCUMENTED_ANSWER_CACHE.clear()
        query_plan = build_client_query_plan(
            "Compare le produit cache test", "documented"
        )
        candidates = [{
            "client_id": "product:cache",
            "name": "PRODUIT CACHE TEST",
            "description": "Description exacte du produit.",
        }]
        documents = [{
            "source_id": "catalog:test",
            "evidence": "Fait exact du catalogue.",
            "candidate_ids": ["product:cache"],
        }]
        generated = {
            "answer": "Voici une réponse documentée assez complète pour le test.",
            "selected_product_ids": ["product:cache"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
            "degraded": False,
        }
        with patch(
            "routes.ai._generate_documented_client_answer_sync",
            return_value=generated,
        ) as generator:
            first = generate_documented_client_answer(
                "Compare le produit cache test",
                query_plan, candidates, documents,
            )
            second = generate_documented_client_answer(
                "Compare le produit cache test",
                query_plan, candidates, documents,
            )
        self.assertEqual(generator.call_count, 1)
        self.assertFalse(first["_cache_hit"])
        self.assertTrue(second["_cache_hit"])

    def test_familiprix_lookup_uses_internal_code_and_validates_exact_upc(self):
        product_url = (
            "https://magasiner.familiprix.com/fr/sante/produit/"
            "p/000000000000120407"
        )
        search_html = (
            '<a href="/fr/sante/produit/p/000000000000120407">Produit</a>'
        )
        product_html = """
            <script type="application/ld+json">
            {
              "@type": "Product",
              "name": "BENYLIN SIROP GORGE ET TOUX 250 ML",
              "brand": {"name": "Benylin"},
              "sku": "120407",
              "gtin12": "062600264206",
              "description": "Soulage le mal de gorge et la toux.",
              "image": "https://images.example/benylin.jpg"
            }
            </script>
        """
        requested_urls = []

        def fake_fetch(url):
            requested_urls.append(url)
            if "/p/" in url:
                return product_html, product_url
            return search_html, url

        with patch("routes.ai.fetch_text", side_effect=fake_fetch):
            product = lookup_familiprix_product(
                "062600264206", product_code="120407"
            )

        self.assertIsNotNone(product)
        self.assertEqual(product["product_code"], "120407")
        self.assertEqual(product["source"], "Familiprix")
        self.assertEqual(
            product["description"],
            "BENYLIN SIROP GORGE ET TOUX 250 ML. "
            "Soulage le mal de gorge et la toux.",
        )
        self.assertEqual(
            requested_urls[0],
            "https://magasiner.familiprix.com/fr/p/000000000000120407",
        )

        with patch("routes.ai.fetch_text", side_effect=fake_fetch):
            mismatch = lookup_familiprix_product(
                "063000000000", product_code="120407"
            )
        self.assertIsNone(mismatch)

        requested_urls.clear()
        with patch("routes.ai.fetch_text", side_effect=fake_fetch):
            direct_mismatch = lookup_familiprix_product(
                "063000000000", product_code="120407", direct_only=True
            )
        self.assertIsNone(direct_mismatch)
        self.assertEqual(len(requested_urls), 1)

    def test_familiprix_page_imports_readable_description_and_exact_facts(self):
        html = """
          <script type="application/ld+json">
          {
            "@type": "BreadcrumbList",
            "itemListElement": [
              {"item": {"name": "Accueil"}},
              {"item": {"name": "Santé"}},
              {"item": {"name": "Rhume et toux"}},
              {"item": {"name": "Sirops"}},
              {"item": {"name": "Benylin Mal de Gorge 250ml"}}
            ]
          }
          </script>
          <script type="application/ld+json">
          {
            "@type": "Product",
            "name": "Benylin Mal de Gorge 250ml",
            "brand": {"name": "Benylin"},
            "sku": "120407",
            "gtin12": "062600264206",
            "description": "Soulage rapidement :<br>- mal de gorge<br>- toux grasse",
            "image": "https://images.example/benylin.jpg"
          }
          </script>
          <div class="product-specification-item">
            <span><b>Forme</b></span><span>SIROP</span>
          </div>
          <div class="product-specification-item">
            <span><b>#DIN</b></span><span>02479869</span>
          </div>
          <button class="product-information-section-btn">
            Avertissements et allégations sur le produit
          </button>
          <div class="product-information-section-text">
            MISES EN GARDE<br>Usages<br>- toux grasse<br>- mal de gorge
          </div>
          <button class="product-information-section-btn">Ingrédients</button>
          <div class="product-information-section-text">
            Glycérine<br>- eau purifiée
          </div>
        """
        product = parse_familiprix_product_page(
            html,
            "https://magasiner.familiprix.com/fr/product/p/000000000000120407",
            "062600264206",
            product_code="120407",
        )

        self.assertIsNotNone(product)
        self.assertEqual(product["category"], "Santé > Rhume et toux > Sirops")
        self.assertEqual(product["dosage_form"], "SIROP")
        self.assertEqual(product["package_size"], "250")
        self.assertEqual(product["package_unit"], "ml")
        self.assertIn("mal de gorge; toux grasse", product["description"].lower())
        self.assertIn("toux grasse; mal de gorge", product["purpose"].lower())
        self.assertIn("glycérine; eau purifiée", product["ingredients"].lower())
        self.assertTrue(
            any(
                identifier.get("type") == "DIN"
                and identifier.get("value") == "02479869"
                and identifier.get("source") == "Familiprix"
                for identifier in product["regulatory_identifiers"]
            )
        )

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

    def test_explicit_brand_comparison_excludes_unrelated_liquid_products(self):
        products = [{
            "id": 1, "name": "ADVIL 200MG CO100",
            "brand": "Advil", "barcode": "101",
            "description": "Comprimes d'ibuprofene 200 mg.",
        }, {
            "id": 2, "name": "ADVIL 200MG LIQ/GEL CA40",
            "brand": "Advil", "barcode": "102",
            "description": "Capsules liqui-gels d'ibuprofene 200 mg.",
        }, {
            "id": 3, "name": "SKINTIMATE GEL RASAGE 198G",
            "brand": "Skintimate", "barcode": "103",
            "description": "Gel liquide pour le rasage.",
        }, {
            "id": 4, "name": "JJ NET BB TETE O PIEDS 400ML",
            "brand": "Johnson's", "barcode": "104",
            "description": "Nettoyant liquide pour bebe.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in products]
        question = (
            "Quelle est la difference entre les comprimes et les "
            "liqui-gels Advil, et dans quel contexte choisir chacun?"
        )

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(
                question, build_client_query_plan(question, "documented"),
                limit=20,
            )

        self.assertEqual(
            {product["id"] for product in matches},
            {1, 2},
        )

    def test_form_comparison_excludes_unrequested_contextual_variants(self):
        products = [{
            "id": 1, "name": "ADVIL 200MG CO100",
            "brand": "Advil", "barcode": "101",
            "description": "Comprimes d'ibuprofene 200 mg.",
        }, {
            "id": 2, "name": "ADVIL 200MG LIQ/GEL CA40",
            "brand": "Advil", "barcode": "102",
            "description": "Capsules liqui-gels d'ibuprofene 200 mg.",
        }, {
            "id": 3, "name": "ADVIL ENF 100MG RAISIN 100ML",
            "brand": "Advil", "barcode": "103",
            "description": "Suspension orale pour enfants.",
        }, {
            "id": 4, "name": "ADVIL GTTS FRUITS 24ML",
            "brand": "Advil", "barcode": "104",
            "description": "Gouttes pour nourrissons.",
        }, {
            "id": 5, "name": "ADVIL RHUME SINUS CA18",
            "brand": "Advil", "barcode": "105",
            "description": "Comprimes combines contre le rhume et les sinus.",
        }, {
            "id": 6, "name": "ADVIL NUIT CA20",
            "brand": "Advil", "barcode": "106",
            "description": "Comprimes de nuit.",
        }, {
            "id": 7, "name": "ADVIL NT LIQ/GEL CA20",
            "brand": "Advil", "barcode": "107",
            "description": "Liqui-gels de nuit.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in products]
        question = (
            "Quelle est la difference entre les comprimes Advil et les "
            "Liqui-Gels, et dans quel contexte choisir chaque forme?"
        )

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(
                question, build_client_query_plan(question, "documented"),
                limit=20,
            )

        self.assertEqual(
            {product["id"] for product in matches},
            {1, 2},
        )

    def test_full_question_words_do_not_retrieve_unrelated_products(self):
        products = [{
            "id": 1, "name": "BENYLIN SIROP GORGE TOUX 250ML",
            "brand": "Benylin", "barcode": "111",
            "description": "Sirop pour la gorge et la toux.",
        }, {
            "id": 2, "name": "RESTORALAX POUDRE 510G",
            "brand": "RestoraLAX", "barcode": "222",
            "description": "Produit facile à prendre et meilleur format familial.",
        }, {
            "id": 3, "name": "CANESTEN CREME 15G",
            "brand": "Canesten", "barcode": "333",
            "description": "Demandez pourquoi ce produit convient à la peau.",
        }, {
            "id": 4, "name": "TYLENOL MAL DOS CA18",
            "brand": "Tylenol", "barcode": "444",
            "description": "Soulagement du mal de dos.",
        }, {
            "id": 5, "name": "LAKOTA ANALG BIL MAL DOS 88ML",
            "brand": "Lakota", "barcode": "555",
            "description": "Analgésique pour le mal de dos.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in products]
        question = (
            "Quel est le meilleur produit pour le mal de gorge et la toux, "
            "et pourquoi?"
        )

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(
                question, build_client_query_plan(question, "documented"),
                limit=20,
            )

        self.assertEqual(
            [product["name"] for product in matches],
            ["BENYLIN SIROP GORGE TOUX 250ML"],
        )

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
            {
                "id": 18, "name": "AIRWICK E/MIST LAV FL AMD 20ML",
                "brand": "Airwick", "barcode": "118",
                "description": "Contient de l'acétaminophène.",
            },
            {
                "id": 19, "name": "BENYLIN T/E/U S/APAIS 250ML",
                "brand": "Benylin", "barcode": "119",
                "description": "Soulage la douleur et contient un analgésique.",
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

    def test_headache_search_normalizes_the_query_not_every_catalogue_term(self):
        question = "Jai mal a la tete que prendre"
        corpus = []
        for index in range(1200):
            product = {
                "id": index + 1,
                "name": (
                    "TYLENOL 500MG CO100"
                    if index == 0 else f"PRODUIT DIVERS {index}"
                ),
                "brand": "Tylenol" if index == 0 else "Marque",
                "barcode": str(600000000000 + index),
                "aisle": "1", "side": "A", "section": "1",
                "shelf": "1", "position": str(index + 1),
                "in_stock": 1, "is_plano": 1,
            }
            corpus.append((
                product,
                search_row(
                    product["name"], product["brand"],
                    barcode=product["barcode"],
                ),
            ))
        plan = build_client_query_plan(question, "documented")

        with patch(
            "routes.products._employee_product_corpus", return_value=corpus,
        ), patch(
            "routes.products.get_db", return_value=object(),
        ), patch(
            "routes.products.normalize_search_text",
            wraps=products_module.normalize_search_text,
        ) as normalize:
            matches = hybrid_client_candidates(question, plan, limit=8)

        self.assertEqual(matches[0]["name"], "TYLENOL 500MG CO100")
        self.assertLess(normalize.call_count, 200)

    def test_headache_filter_does_not_restore_unrelated_candidates(self):
        candidates = [
            {"id": 1, "name": "ADVIL 200MG CO100", "brand": "Advil"},
            {"id": 2, "name": "ORAL-B IO TETE BR/DENTS BLC 4", "brand": "Oral-B"},
            {"id": 3, "name": "TYLENOL RH/SIN JR NT CA20", "brand": "Tylenol"},
            {
                "id": 4, "name": "AIRWICK E/MIST LAV FL AMD 20ML",
                "brand": "Airwick", "description": "Contient de l'acétaminophène.",
            },
            {
                "id": 5, "name": "BENYLIN T/E/U S/APAIS 250ML",
                "brand": "Benylin", "description": "Soulage la douleur.",
            },
        ]

        filtered = filter_client_answer_category(
            "Jai mal à la tête que prendre", candidates,
        )

        self.assertEqual([product["id"] for product in filtered], [1])

    def test_fever_retrieval_keeps_only_named_fever_relief_products(self):
        question = "Jai de la fievre que prendre"
        products = [{
            "id": 1, "name": "TYLENOL 500MG X/F CO100",
            "brand": "Tylenol", "barcode": "101",
            "description": "Contient de l'acétaminophène.",
        }, {
            "id": 2, "name": "ADVIL 200MG CO100",
            "brand": "Advil", "barcode": "102",
            "description": "Contient de l'ibuprofène.",
        }, {
            "id": 3, "name": "AIRWICK E/MIST LAV 20ML",
            "brand": "Airwick", "barcode": "103",
            "description": "Description erronée: contient de l'acétaminophène.",
        }, {
            "id": 4, "name": "BENYLIN T/E/U 250ML",
            "brand": "Benylin", "barcode": "104",
            "description": "Formule avec analgésique contre la fièvre.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"], product["description"],
                barcode=product["barcode"],
            ),
        ) for product in products]
        plan = build_client_query_plan(question, "documented")

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(question, plan, limit=20)

        self.assertEqual(plan["intent"], "fever_relief")
        self.assertEqual({product["id"] for product in matches}, {1, 2})

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
        response_products = response.get_json()
        names = {item["name"] for item in response_products}
        self.assertEqual(names, {product["name"] for product in products[:7]})
        self.assertNotIn("TETE", response_products[0]["name"])
        self.assertNotIn("CURAPROX BR/DENT SMART 1", names)
        self.assertNotIn("DENTA RINSE PRO .2% MENT 500ML", names)
        self.assertNotIn("SONICARE IRR S/FIL HX3826/23 1", names)
        self.assertNotIn("GUM PROXABRUSH RECH BROS LG 10", names)

    def test_toothpaste_comparison_excludes_brushes_and_floss(self):
        products = [{
            "id": 1, "name": "SENSODYNE BLANCHISSANT 135ML",
            "brand": "Sensodyne", "barcode": "101",
            "description": "Dentifrice blanchissant pour dents sensibles.",
        }, {
            "id": 2, "name": "COLGATE TT BLANCHISSANT 120ML",
            "brand": "Colgate", "barcode": "102",
            "description": "Dentifrice qui aide à enlever les taches de surface.",
        }, {
            "id": 3, "name": "ORAL-B GLIDE SOIE DENT 40M",
            "brand": "Oral-B", "barcode": "103",
            "description": "Soie pour les espaces serrés et les dents sensibles.",
        }, {
            "id": 4, "name": "ORAL-B BR/DENTS BLANCHISSANT 1",
            "brand": "Oral-B", "barcode": "104",
            "description": "Brosse pour nettoyer les dents et aider au blanchiment.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"], product["description"],
                barcode=product["barcode"],
            ),
        ) for product in products]
        query = (
            "différence entre un dentifrice pour dents sensibles "
            "et un dentifrice blanchissant"
        )

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates(
                query, build_client_query_plan(query, "documented"), limit=20,
            )

        self.assertEqual(
            {product["id"] for product in matches},
            {1, 2},
        )

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
                employee_response = client.get(
                    "/api/products/search?q=je%20cherche%20de%20la%20watte%20des%20petites%20boules"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["PERSONNEL OUATE BOULES 100"],
        )
        self.assertEqual(
            [item["name"] for item in employee_response.get_json()],
            ["PERSONNEL OUATE BOULES 100"],
        )

    def test_charcoal_pill_lookup_keeps_exact_store_products_only(self):
        biomedic = {
            "id": 1, "name": "BIOMEDIC CHARB ACT 225MG CA75",
            "brand": "Biomedic", "barcode": "063848908532",
            "description": "Capsules de charbon active",
        }
        leo = {
            "id": 2, "name": "LEO DESILETS CHARBON ACT CA 75",
            "brand": "Leo Desilets", "barcode": "622049105074",
            "description": "Capsules de charbon active",
        }
        unrelated = [
            {
                "id": 3, "name": "BIOMEDIC ECRASE COUPE PILULE 1",
                "brand": "Biomedic", "barcode": "063848960677",
                "description": "Broyeur de comprimes avec contenant.",
            },
            {
                "id": 4, "name": "CARTER PETITES PILULES LAX 25",
                "brand": "Carter", "barcode": "400",
                "description": "Laxatif en pilules.",
            },
            {
                "id": 5, "name": "CREST 3DW CHARBON FLUOR 135ML",
                "brand": "Crest", "barcode": "500",
                "description": "Dentifrice au charbon.",
            },
            {
                "id": 6, "name": "BIORE NETT CHARB PORES 200ML",
                "brand": "Biore", "barcode": "600",
                "description": "Nettoyant facial au charbon.",
            },
        ]
        corpus = [(
            product,
            search_row(
                product["name"], product.get("brand", ""),
                product.get("description", ""), product["barcode"],
            ),
        ) for product in [biomedic, leo, *unrelated]]

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                client_response = client.get(
                    "/api/client/find?q=pilule%20de%20charbon"
                )
                employee_response = client.get(
                    "/api/products/search?q=pilule%20de%20charbon"
                )

        expected = {
            "BIOMEDIC CHARB ACT 225MG CA75",
            "LEO DESILETS CHARBON ACT CA 75",
        }
        self.assertEqual(client_response.status_code, 200)
        self.assertEqual(
            {item["name"] for item in client_response.get_json()}, expected,
        )
        self.assertEqual(
            {item["name"] for item in employee_response.get_json()}, expected,
        )

    def test_charcoal_identity_ignores_contaminated_cosmetic_enrichment(self):
        charcoal = {
            "id": 1, "name": "BIOMEDIC CHARB ACT 225MG CA75",
            "brand": "Biomedic", "barcode": "101",
            "description": "Capsules de charbon active.",
        }
        cosmetic = {
            "id": 2, "name": "LOREAL MEN NETT CHARBON 100ML",
            "brand": "Loreal", "barcode": "102",
            # Simulates a stale alias/description attached by enrichment.
            "description": "Capsules de charbon active.",
        }
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in (charcoal, cosmetic)]

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=pilule%20de%20charbon")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["BIOMEDIC CHARB ACT 225MG CA75"],
        )

    def test_ambiguous_four_letter_shelf_fragment_is_not_a_product_concept(self):
        bath_foam = {
            "id": 1, "name": "ATTITUDE B/L B/MOUS 473ML",
            "brand": "Attitude", "barcode": "101",
            "description": "Bain moussant.",
        }
        antiperspirant = {
            "id": 2, "name": "DOVE MEN ANTI VAPO 107G",
            "brand": "Dove", "barcode": "102",
            "description": "Antisudorifique en vaporisateur.",
        }
        after_bite = {
            "id": 3, "name": "AFTER BITE G/TRAIT 20G",
            "brand": "After Bite", "barcode": "103",
            "description": "Traitement des piqures de moustiques.",
        }
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in (bath_foam, antiperspirant, after_bite)]

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                fast = client.get("/api/client/find?q=spray%20anti%20moustique")
                search = client.get("/api/products/search?q=spray%20anti%20moustique")

        self.assertEqual(fast.get_json(), [])
        self.assertEqual(search.get_json(), [])

    def test_five_letter_planogram_abbreviation_still_finds_requested_object(self):
        repellent = {
            "id": 1, "name": "OFF CHASSE MOUST VAPO 142G",
            "brand": "Off", "barcode": "101",
            "description": "Vaporisateur chasse-moustiques.",
        }
        corpus = [(
            repellent,
            search_row(
                repellent["name"], repellent["brand"],
                repellent["description"], repellent["barcode"],
            ),
        )]

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=spray%20anti%20moustique")

        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["OFF CHASSE MOUST VAPO 142G"],
        )

    def test_distinguishing_concepts_outrank_generic_forms_across_categories(self):
        products = [{
            "id": 1, "name": "WEBBER CANNEB 10000MG CA90",
            "brand": "Webber", "barcode": "101",
            "description": "Capsules de canneberge.",
        }, {
            "id": 2, "name": "CAFE CAPSULE INTENSE 10",
            "brand": "Test", "barcode": "102",
            "description": "Capsules de cafe.",
        }, {
            "id": 3, "name": "DICLOFENAC GEL TOPIQUE 100G",
            "brand": "Test", "barcode": "103",
            "description": "Gel topique au diclofenac.",
        }, {
            "id": 4, "name": "GEL COIFFANT TENUE FORTE 100G",
            "brand": "Test", "barcode": "104",
            "description": "Gel pour les cheveux.",
        }, {
            "id": 5, "name": "OFF CHASSE MOUST VAPO 142G",
            "brand": "Off", "barcode": "105",
            "description": "Vaporisateur chasse-moustiques.",
        }, {
            "id": 6, "name": "SPRAY COIFFANT 200ML",
            "brand": "Test", "barcode": "106",
            "description": "Fixatif en vaporisateur.",
        }, {
            "id": 7, "name": "WEBBER VIT D3 1000UI GEL90",
            "brand": "Webber", "barcode": "107",
            "description": "Produit de la famille des supplements de canneberge.",
        }]
        corpus = [(
            product,
            search_row(
                product["name"], product["brand"],
                product["description"], product["barcode"],
            ),
        ) for product in products]
        cases = [
            ("capsules de canneberge", 1),
            ("gel de diclofenac", 3),
            ("spray anti moustique", 5),
        ]

        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            for query, expected_id in cases:
                with self.subTest(query=query):
                    matches = hybrid_client_candidates(
                        query, build_client_query_plan(query, "documented"),
                        limit=20,
                    )
                    self.assertEqual(
                        [product["id"] for product in matches], [expected_id],
                    )

    def test_candidate_quality_rejects_generic_word_only_results(self):
        pill_crusher = {
            "id": 1, "name": "BIOMEDIC ECRASE COUPE PILULE 1",
            "brand": "Biomedic", "barcode": "101",
            "description": "Broyeur de comprimes.",
        }
        charcoal = {
            "id": 2, "name": "BIOMEDIC CHARB ACT 225MG CA75",
            "brand": "Biomedic", "barcode": "102",
            "description": "Capsules de charbon active.",
        }
        transparent_cup = {
            "id": 3, "name": "TASSE TRANSPARENTE 1",
            "brand": "Test", "barcode": "103",
            "description": "Tasse en plastique transparent.",
        }

        self.assertTrue(client_candidates_need_semantic_retry(
            "pilule de charbon", [pill_crusher],
        ))
        self.assertFalse(client_candidates_need_semantic_retry(
            "pilule de charbon", [charcoal],
        ))
        self.assertTrue(client_candidates_need_semantic_retry(
            "objet transparent pour proteger une blessure", [transparent_cup],
        ))

    def test_documented_contradiction_is_replaced_with_grounded_inventory(self):
        candidate = {
            "id": 1, "client_id": "product:1",
            "name": "BIOMEDIC CHARB ACT 225MG CA75",
            "brand": "Biomedic", "barcode": "063848908532",
            "description": "Capsules de charbon active.",
            "aisle": "1", "side": "B", "section": "8",
            "shelf": "2", "position": "15",
        }
        contradictory = {
            "answer": "Je n'ai trouve aucune capsule de charbon dans le magasin.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
            "key_points": [], "comparisons": [], "useful_guidance": [],
            "important_checks": [], "source_ids": ["store-plan"],
        }
        plan = build_client_query_plan("pilule de charbon", "documented")
        guarded = _guard_documented_inventory_contradiction(
            contradictory, plan, [candidate], [{
                "source_id": "store-plan", "candidate_ids": ["product:1"],
            }],
        )

        self.assertNotIn("aucune capsule", guarded["answer"].lower())
        self.assertEqual(guarded["selected_product_ids"], ["product:1"])
        self.assertIn(candidate["name"], guarded["answer"])

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
                if "verification_status='rejected'" in _query:
                    return Result(rows=[])
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

    def test_partial_ai_answer_never_attaches_the_first_unrelated_candidates(self):
        candidates = [{
            "client_id": "product:1", "name": "TASSE TRANSPARENTE",
        }, {
            "client_id": "product:2", "name": "PARAMEDIC PANS TRANSP 5CMX1M",
        }]
        with patch(
            "routes.ai._provider_structured_request",
            return_value={
                "_partial_answer": (
                    "Le produit pertinent est PARAMEDIC PANS TRANSP 5CMX1M."
                ),
                "_partial_selected_product_ids": ["product:2"],
            },
        ):
            result = ai_module.generate_verified_client_answer(
                "Je cherche un pansement transparent", {}, candidates,
            )

        self.assertEqual(result["selected_product_ids"], ["product:2"])

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

    def test_documented_model_only_generates_the_answer_layer(self):
        product = {
            "id": 7, "client_id": "product:7",
            "name": "ADVIL 200MG CO100", "brand": "Advil",
            "description": "Comprimés d'ibuprofène.",
            "_verified_fields": ["description"],
            "aisle": "Labo", "side": "A", "section": "2",
            "shelf": "4", "position": "1",
        }
        documents = [{
            "source_id": "store-plan", "title": "Plan actuel",
            "candidate_ids": ["product:7"],
        }]
        parsed = {
            "answer": "L'ibuprofène est une option contre la douleur.",
            "key_points": [],
            "selected_product_ids": ["product:7"],
            "source_ids": ["store-plan"],
        }

        with patch(
            "routes.ai._provider_structured_request", return_value=parsed
        ) as provider:
            result = generate_documented_client_answer(
                "Que prendre pour un mal de tête?",
                {"medical": True}, [product], documents,
            )

        call = provider.call_args
        self.assertEqual(call.kwargs["max_tokens"], 520)
        self.assertEqual(call.kwargs["timeout_seconds"], 9)
        self.assertTrue(call.kwargs["realtime_model"])
        self.assertEqual(
            set(call.kwargs["schema"]["properties"]),
            {"selected_product_ids", "answer"},
        )
        compact_payload = call.args[1]
        self.assertNotIn("required_schema", compact_payload)
        self.assertNotIn("locations", compact_payload["candidates"][0])
        self.assertNotIn("field_sources", compact_payload["candidates"][0])
        self.assertTrue(
            compact_payload["candidates"][0]["catalogue_description"]["verified"]
        )
        self.assertEqual(result["selected_product_ids"], ["product:7"])
        self.assertEqual(result["comparisons"][0]["candidate_id"], "product:7")
        self.assertTrue(result["safety_flags"])

    def test_documented_answer_keeps_products_when_ai_is_unavailable(self):
        product = {
            "id": 7,
            "client_id": "product:7",
            "name": "MELATONINE FRAISE 5 MG CO60",
            "description": "Comprimés à saveur de fraise.",
            "usage_notes": "Lire l'étiquette avant utilisation.",
            "aisle": "4", "side": "A", "section": "2", "shelf": "3", "position": "5",
        }
        query_plan = build_client_query_plan(
            "Quelles saveurs de mélatonine avons-nous et dans quel contexte les utiliser?",
            "documented",
        )
        documents = retrieve_client_documentation(
            [product], query_plan, include_live_regulatory=False,
        ) + [{
            "source_id": "catalog:1",
            "title": "Fiche produit",
            "candidate_ids": ["product:7"],
        }]

        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                query_plan["corrected_query"], query_plan, [product], documents,
            )

        self.assertTrue(result["degraded"])
        self.assertEqual(result["selected_product_ids"], ["product:7"])
        self.assertIn("comprimés", result["answer"])
        self.assertIn("5 mg", result["answer"])
        self.assertIn("fraise", result["answer"])
        self.assertIn("horaire décalé", result["answer"])
        self.assertNotIn("j'ai trouvé", result["answer"].lower())
        self.assertEqual(result["key_points"][0]["heading"], "Choisir selon le besoin")
        self.assertEqual(
            result["comparisons"][0]["source_ids"],
            ["catalog-description:1", "catalog:1"],
        )
        self.assertLessEqual(len(result["comparisons"][0]["difference"]), 420)
        self.assertTrue(result["pharmacist_referral"])

    def test_documented_answer_returns_fast_model_before_deep_upgrade(self):
        product = {
            "id": 1, "client_id": "product:1", "name": "ADVIL 200MG CO100",
            "brand": "Advil", "description": "Comprimes d'ibuprofene.",
        }
        query_plan = build_client_query_plan(
            "Quelle difference entre comprimes et liqui-gels Advil?",
            "documented",
        )

        quick = {
            "answer": "Réponse rapide fondée sur le produit en magasin.",
            "selected_product_ids": ["product:1"], "degraded": False,
        }
        deep = {
            "answer": "Réponse approfondie fondée sur le produit en magasin.",
            "selected_product_ids": ["product:1"], "degraded": False,
        }

        def answer(*_args, **kwargs):
            if kwargs.get("quality_mode"):
                time.sleep(0.08)
                return dict(deep)
            return dict(quick)

        started = time.perf_counter()
        with patch(
            "routes.ai.configured_ai_provider", return_value={"name": "kimi"},
        ), patch(
            "routes.ai.KIMI_REALTIME_MODEL", "kimi-k2.6",
        ), patch(
            "routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k3",
        ), patch(
            "routes.ai._generate_documented_client_answer_sync", side_effect=answer,
        ):
            result = generate_documented_client_answer(
                query_plan["corrected_query"], query_plan, [product], [],
            )
        elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 0.06)
        self.assertEqual(result["answer"], quick["answer"])
        self.assertTrue(result["_ai_pending"])
        self.assertTrue(result["_documented_job_id"])
        self.assertIn("product:1", result["selected_product_ids"])
        time.sleep(0.1)

    def test_documented_exact_code_returns_grounded_answer_before_ai_upgrade(self):
        product = {
            "id": 1884, "client_id": "product:1884",
            "name": "BIOMEDIC GEL ANALG GLACE 255G", "brand": "Biomedic",
            "barcode": "063848907665",
            "description": "Gel for temporary muscular and joint pain relief.",
            "dosage_form": "GEL", "_verified_fields": ["description", "dosage_form"],
            "_exact_identifier_matches": [{
                "field": "upc", "value": "063848907665",
            }],
        }
        question = "UPC 063848907665 : est-ce liquide ou en gel?"
        query_plan = build_client_query_plan(question, "documented")
        deep = {
            "answer": "La fiche approfondie confirme qu'il s'agit d'un gel.",
            "selected_product_ids": ["product:1884"],
            "degraded": False,
        }

        def answer(*_args, **kwargs):
            self.assertFalse(kwargs.get("quality_mode"))
            time.sleep(0.08)
            return dict(deep)

        started = time.perf_counter()
        with patch(
            "routes.ai.configured_ai_provider", return_value={"name": "kimi"},
        ), patch(
            "routes.ai.KIMI_REALTIME_MODEL", "kimi-k2.6",
        ), patch(
            "routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k3",
        ), patch(
            "routes.ai._generate_documented_client_answer_sync",
            side_effect=answer,
        ) as generator:
            result = generate_documented_client_answer(
                question, query_plan, [product], [],
            )
            elapsed = time.perf_counter() - started
            for _attempt in range(50):
                if generator.call_count:
                    break
                time.sleep(0.02)

        self.assertLess(elapsed, 0.06)
        self.assertIn("BIOMEDIC GEL ANALG GLACE 255G", result["answer"])
        self.assertIn("gel topique", result["answer"])
        self.assertTrue(result["_ai_pending"])
        self.assertTrue(result["_documented_job_id"])
        self.assertEqual(generator.call_count, 1)
        time.sleep(0.1)

    def test_documented_background_answer_becomes_pollable_and_cached(self):
        question = "Compare le produit asynchrone unique"
        query_plan = build_client_query_plan(question, "documented")
        candidates = [{
            "client_id": "product:async",
            "name": "PRODUIT ASYNCHRONE",
            "description": "Description exacte et suffisamment précise.",
        }]
        documents = [{
            "source_id": "catalog:async",
            "evidence": "Fiche exacte du produit asynchrone.",
            "candidate_ids": ["product:async"],
        }]
        generated = {
            "answer": (
                "Réponse approfondie terminée avec les différences "
                "exactement documentées pour ce produit."
            ),
            "selected_product_ids": ["product:async"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
            "key_points": [],
            "comparisons": [],
            "useful_guidance": [],
            "important_checks": [],
            "source_ids": ["catalog:async"],
            "degraded": False,
            "warning": "",
        }

        immediate_answer = {
            **generated,
            "answer": "Réponse documentée rapide et utile pour ce produit.",
        }

        def staged_answer(*_args, **kwargs):
            if kwargs.get("quality_mode"):
                time.sleep(0.08)
                return dict(generated)
            return dict(immediate_answer)

        with patch(
            "routes.ai.configured_ai_provider", return_value={"name": "kimi"},
        ), patch(
            "routes.ai.KIMI_REALTIME_MODEL", "kimi-k2.6",
        ), patch(
            "routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k3",
        ), patch(
            "routes.ai._generate_documented_client_answer_sync",
            side_effect=staged_answer,
        ) as generator:
            immediate = generate_documented_client_answer(
                question, query_plan, candidates, documents,
            )
            self.assertTrue(immediate["_ai_pending"])
            job_id = immediate["_documented_job_id"]
            with app.test_client() as client:
                pending_response = client.get(
                    f"/api/client/help/documented/{job_id}"
                )
            self.assertEqual(pending_response.status_code, 202)
            time.sleep(0.12)
            job = ai_module._documented_job_status(job_id)
            with app.test_client() as client:
                ready_response = client.get(
                    f"/api/client/help/documented/{job_id}"
                )
            cached = generate_documented_client_answer(
                question, query_plan, candidates, documents,
            )

        self.assertEqual(generator.call_count, 2)
        self.assertEqual(job["status"], "ready")
        self.assertEqual(job["result"]["answer"], generated["answer"])
        self.assertEqual(ready_response.status_code, 200)
        self.assertTrue(ready_response.get_json()["ready"])
        self.assertTrue(cached["_cache_hit"])

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
        self.assertNotIn("j'ai trouvé", result["answer"].lower())
        self.assertEqual(
            [point["heading"] for point in result["key_points"]],
            ["Choix rapide", "Avant de proposer", "Ne pas combiner", "Quand référer"],
        )
        self.assertEqual(len(result["follow_up_questions"]), 3)
        self.assertTrue(result["pharmacist_referral"])
        self.assertIn(
            "health-canada:acetaminophen-safe-use",
            result["source_ids"],
        )

    def test_documented_wound_dressing_fallback_answers_the_comparison(self):
        products = [{
            "id": 1, "client_id": "product:1",
            "name": "PARAMEDIC PANS HYDRO 10X10CM 1",
            "description": (
                "Pansement hydrocolloïde qui absorbe l'exsudat et maintient "
                "un milieu humide."
            ),
            "aisle": "2", "side": "B", "section": "6",
            "shelf": "7", "position": "3",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "PARAMEDIC PANS TRANSP 5CMX1M 1",
            "description": (
                "Film transparent imperméable qui permet de voir la plaie."
            ),
            "aisle": "2", "side": "B", "section": "6",
            "shelf": "6", "position": "4",
        }]
        query_plan = build_client_query_plan(
            (
                "Quelle est la différence entre un pansement hydrocolloïde "
                "et un pansement transparent?"
            ),
            "documented",
        )
        documents = retrieve_client_documentation(
            products, query_plan, include_live_regulatory=False,
        )

        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                query_plan["corrected_query"], query_plan, products, documents,
            )

        self.assertIn("forme un gel", result["answer"])
        self.assertIn("zone visible", result["answer"])
        self.assertEqual(
            [point["heading"] for point in result["key_points"]],
            ["Hydrocolloïde", "Film transparent", "Choix rapide", "Avant d'appliquer"],
        )
        self.assertIn("nhs:wound-hydrocolloid", result["source_ids"])
        self.assertIn("nhs:wound-transparent-film", result["source_ids"])
        self.assertEqual(len(result["follow_up_questions"]), 3)
        self.assertTrue(result["pharmacist_referral"])

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

    def test_documented_form_comparison_explains_how_to_choose(self):
        products = [{
            "client_id": "product:1", "name": "ADVIL 200MG CO100",
            "description": "Comprimés d'ibuprofène 200 mg.",
            "aisle": "Labo", "side": "A", "shelf": "4",
        }, {
            "client_id": "product:2", "name": "ADVIL 200MG LIQ/GEL CA115",
            "description": "Capsules liqui-gels d'ibuprofène 200 mg.",
            "aisle": "Labo", "side": "A", "shelf": "4",
        }, {
            "client_id": "product:3", "name": "ADVIL 200MG MINI GEL CA110",
            "description": "Mini-gels d'ibuprofène 200 mg.",
            "aisle": "Labo", "side": "A", "shelf": "4",
        }, {
            "client_id": "product:4", "name": "ADVIL ENF 100MG SUSP LIQ100ML",
            "description": "Suspension liquide d'ibuprofène.",
            "aisle": "Labo", "side": "A", "shelf": "4",
        }]
        plan = build_client_query_plan(
            "Quelle est la différence entre les formes d'Advil et laquelle choisir?",
            "documented",
        )
        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                plan["corrected_query"], plan, products, [{
                    "source_id": "store-plan",
                    "candidate_ids": [product["client_id"] for product in products],
                }],
            )

        self.assertIn("façon de les prendre", result["answer"])
        self.assertIn("liqui-gels", result["answer"])
        self.assertIn("ne prouve pas une action plus rapide", result["answer"])
        self.assertIn("mesure avec le dispositif", result["answer"])
        self.assertIn("100 mg, 200 mg", result["answer"])
        self.assertEqual(len(result["follow_up_questions"]), 3)
        self.assertTrue(result["pharmacist_referral"])

    def test_documented_form_summary_uses_verified_rapid_relief_evidence(self):
        products = [{
            "client_id": "product:1",
            "name": "ADVIL 200MG CO100",
            "description": "Comprimés d'ibuprofène 200 mg.",
            "description_status": "verified",
            "_verified_fields": ["description"],
        }, {
            "client_id": "product:2",
            "name": "ADVIL 200MG LIQ/GEL CA40",
            "description": (
                "Capsules liqui-gels d'ibuprofène 200 mg présentées pour "
                "un soulagement rapide."
            ),
            "description_status": "verified",
            "_verified_fields": ["description"],
        }]
        plan = build_client_query_plan(
            "Compare les comprimés Advil et les Liqui-Gels.",
            "documented",
        )
        documents = [{
            "source_id": "store-plan",
            "candidate_ids": ["product:1", "product:2"],
        }, {
            "source_id": "catalog:liqui-gel",
            "candidate_ids": ["product:2"],
            "evidence": products[1]["description"],
        }]

        with patch("routes.ai._provider_structured_request", return_value=None):
            result = generate_documented_client_answer(
                plan["corrected_query"], plan, products, documents,
            )

        liqui_point = next(
            point for point in result["key_points"]
            if point["heading"] == "Liqui-gels"
        )
        self.assertIn("soulagement rapide", liqui_point["detail"])
        self.assertEqual(
            liqui_point["source_ids"], ["catalog:liqui-gel"]
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
        query_plan = build_client_query_plan(
            "Quels types de mélatonine et dans quel contexte?", "documented",
        )
        intent_documents = retrieve_client_documentation(
            products, query_plan, include_live_regulatory=False,
        )
        source_ids = {
            document["source_id"] for document in intent_documents
        }
        self.assertIn("health-canada:melatonin-uses", source_ids)
        self.assertIn("health-canada:melatonin-safety", source_ids)
        self.assertIn("health-canada:melatonin-pediatric", source_ids)
        for document in intent_documents:
            if document["source_id"].startswith("health-canada:"):
                self.assertEqual(document["source_class"], "official_regulator")
                self.assertEqual(document["trust_level"], 100)
                self.assertTrue(document["medical_claims_allowed"])

    def test_unverified_catalogue_text_cannot_support_medical_claims(self):
        documents = retrieve_client_documentation([{
            "id": 1,
            "client_id": "product:1",
            "name": "PRODUIT TEST",
            "description": "Description non vérifiée du catalogue interne.",
            "description_status": "requires_review",
        }], {
            "corrected_query": "Que prendre pour un mal de tête?",
            "medical": True,
        }, include_live_regulatory=False)
        description_document = next(
            document for document in documents
            if document["source_id"].startswith("catalog-description:")
        )
        self.assertEqual(
            description_document["source_class"], "unverified_catalogue"
        )
        self.assertFalse(description_document["medical_claims_allowed"])

    def test_medical_answer_must_cite_trusted_medical_evidence(self):
        query_plan = {
            "corrected_query": "Que prendre pour un mal de tête?",
            "medical": True,
            "needs_comparison": False,
        }
        result = {
            "answer": (
                "Cette réponse donne une recommandation suffisamment longue "
                "pour répondre à la demande du client."
            ),
            "selected_product_ids": ["product:1"],
            "source_ids": ["store-plan"],
            "key_points": [],
        }
        documents = [{
            "source_id": "health-canada:headache",
            "trust_level": 100,
            "medical_claims_allowed": True,
        }]
        self.assertFalse(ai_module._documented_answer_covers_request(
            result, query_plan, [{"client_id": "product:1"}], documents,
        ))
        result["source_ids"] = ["health-canada:headache"]
        self.assertTrue(ai_module._documented_answer_covers_request(
            result, query_plan, [{"client_id": "product:1"}], documents,
        ))

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

    def test_documented_deepseek_timeout_falls_back_without_a_second_charge(self):
        with patch("routes.ai.DEEPSEEK_DOCUMENTED_MODEL", "deepseek-v4-pro"), \
             patch("routes.ai.DEEPSEEK_MODEL", "deepseek-v4-flash"), \
             patch("routes.ai._DEEPSEEK_DOCUMENTED_THINKING", False), \
             patch("routes.ai._safe_urlopen", side_effect=TimeoutError("slow")) as opener, \
             patch("routes.ai._log_ai_usage"):
            result = _deepseek_json_request(
                [{"role": "user", "content": "test"}],
                max_tokens=3200, quality_mode=True,
            )

        self.assertIsNone(result)
        self.assertEqual(opener.call_count, 1)
        first_payload = json.loads(opener.call_args_list[0].args[0].data.decode("utf-8"))
        self.assertEqual(first_payload["model"], "deepseek-v4-pro")
        self.assertEqual(first_payload["thinking"], {"type": "disabled"})

    def test_documented_kimi_request_uses_k3_reasoning_and_structured_output(self):
        model_result = {
            "answer": "Réponse documentée.",
            "key_points": [],
            "selected_product_ids": [],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
            "source_ids": [],
        }
        api_response = {
            "choices": [{
                "message": {"content": json.dumps(model_result, ensure_ascii=False)},
            }],
            "usage": {"prompt_tokens": 120, "completion_tokens": 60},
        }
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            api_response, ensure_ascii=False,
        ).encode("utf-8")

        with patch("routes.ai.KIMI_API_KEY", "secret"), \
             patch("routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k3"), \
             patch("routes.ai.KIMI_DOCUMENTED_REASONING_EFFORT", "high"), \
             patch("routes.ai._safe_urlopen", return_value=response) as opener, \
             patch("routes.ai._log_ai_usage"):
            result = _kimi_json_request(
                [{"role": "user", "content": "test"}],
                max_tokens=1100,
                quality_mode=True,
                schema_name="documented_answer",
                schema={
                    "type": "object",
                    "properties": {"answer": {"type": "string"}},
                    "required": ["answer"],
                    "additionalProperties": False,
                },
            )

        self.assertEqual(result, model_result)
        request_obj = opener.call_args.args[0]
        payload = json.loads(request_obj.data.decode("utf-8"))
        self.assertEqual(request_obj.full_url, "https://api.moonshot.ai/v1/chat/completions")
        self.assertEqual(payload["model"], "kimi-k3")
        self.assertEqual(payload["reasoning_effort"], "high")
        self.assertEqual(payload["response_format"]["type"], "json_schema")
        self.assertEqual(
            payload["response_format"]["json_schema"]["name"],
            "documented_answer",
        )
        self.assertTrue(payload["response_format"]["json_schema"]["strict"])
        self.assertEqual(payload["max_completion_tokens"], 1100)
        self.assertNotIn("max_tokens", payload)

    def test_kimi_conversational_request_uses_k3_low_reasoning(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({
            "choices": [{
                "message": {"content": json.dumps({"answer": "ok"})},
            }],
            "usage": {},
        }).encode("utf-8")

        with patch("routes.ai.KIMI_MODEL", "kimi-k3"), \
             patch("routes.ai.KIMI_REALTIME_REASONING_EFFORT", "low"), \
             patch("routes.ai._safe_urlopen", return_value=response) as opener, \
             patch("routes.ai._log_ai_usage"):
            result = _kimi_json_request(
                [{"role": "user", "content": "question"}],
                max_tokens=900,
            )

        payload = json.loads(opener.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(result, {"answer": "ok"})
        self.assertEqual(payload["model"], "kimi-k3")
        self.assertEqual(payload["reasoning_effort"], "low")
        self.assertEqual(payload["max_completion_tokens"], 900)
        self.assertNotIn("thinking", payload)

    def test_kimi_stream_recovers_answer_if_response_ends_mid_json(self):
        answer = (
            "Les comprimes conviennent a un usage courant; les capsules "
            "peuvent etre plus faciles a avaler selon la personne."
        )
        event = {
            "choices": [{
                "delta": {
                    "content": json.dumps(
                        {"answer": answer}, ensure_ascii=False,
                    )[:-1],
                },
            }],
        }

        class StreamResponse:
            fp = None

            def __init__(self):
                self.lines = iter([
                    (
                        "data: "
                        + json.dumps(event, ensure_ascii=False)
                        + "\n\n"
                    ).encode("utf-8"),
                ])

            def readline(self):
                return next(self.lines, b"")

        parsed, usage = _read_kimi_stream(
            StreamResponse(), time.monotonic() + 5,
        )

        self.assertEqual(parsed, {
            "_partial_answer": answer,
            "_partial_selected_product_ids": [],
            "_partial_source_ids": [],
        })
        self.assertEqual(usage, {})

    def test_kimi_stream_waits_for_product_ids_after_answer_field(self):
        payload = json.dumps({
            "answer": "Réponse utile qui cite PRODUIT EXACT.",
            "selected_product_ids": ["product:42"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
        }, ensure_ascii=False)
        split_at = payload.index(', "selected_product_ids"')
        chunks = (payload[:split_at], payload[split_at:])

        class StreamResponse:
            fp = None

            def __init__(self):
                self.lines = iter([
                    (
                        "data: " + json.dumps({
                            "choices": [{"delta": {"content": chunk}}],
                        }, ensure_ascii=False) + "\n\n"
                    ).encode("utf-8")
                    for chunk in chunks
                ] + [b"data: [DONE]\n\n"])

            def readline(self):
                return next(self.lines, b"")

        parsed, _usage = _read_kimi_stream(
            StreamResponse(), time.monotonic() + 5,
        )

        self.assertEqual(parsed["selected_product_ids"], ["product:42"])

    def test_kimi_stream_recovers_product_family_from_partial_query_plan(self):
        partial = (
            '{"intent":"food_comparison",'
            '"answer_goal":"Comparer les croustilles selon leur sucre",'
            '"product_family":"croustilles",'
            '"corrected_query":"croustilles avec le moins de sucre",'
            '"search_queries":["croustilles","chips"],'
            '"keywords":['
        )

        class StreamResponse:
            fp = None

            def __init__(self):
                self.lines = iter([
                    (
                        "data: " + json.dumps({
                            "choices": [{"delta": {"content": partial}}],
                        }, ensure_ascii=False) + "\n\n"
                    ).encode("utf-8"),
                    b"",
                ])

            def readline(self):
                return next(self.lines, b"")

        parsed, usage = _read_kimi_stream(
            StreamResponse(), time.monotonic() + 5,
            schema_name="client_query_plan",
        )

        self.assertEqual(parsed["product_family"], "croustilles")
        self.assertEqual(parsed["search_queries"], ["croustilles", "chips"])
        self.assertEqual(parsed["retrieval_scope"], "store")
        self.assertEqual(usage, {})

    def test_kimi_stream_deadline_includes_connection_time(self):
        events = []
        captured = {}
        response = MagicMock()
        response.__enter__.return_value = response

        def clock():
            events.append("clock")
            return 100.0

        def opener(_request, timeout):
            events.append("open")
            self.assertEqual(timeout, 15)
            return response

        def reader(_response, deadline, schema_name=""):
            events.append("read")
            captured["deadline"] = deadline
            captured["schema_name"] = schema_name
            return {"answer": "ok"}, {}

        with patch("routes.ai.KIMI_API_KEY", "secret"), \
             patch("routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k3"), \
             patch("routes.ai.time.monotonic", side_effect=clock), \
             patch("routes.ai._safe_urlopen", side_effect=opener), \
             patch("routes.ai._read_kimi_stream", side_effect=reader), \
             patch("routes.ai._log_ai_usage"):
            result = _kimi_json_request(
                [{"role": "user", "content": "question"}],
                max_tokens=500,
                quality_mode=True,
                timeout_seconds=15,
                realtime_model=True,
            )

        self.assertEqual(result, {"answer": "ok"})
        self.assertEqual(events, ["clock", "open", "read"])
        self.assertEqual(captured["deadline"], 115.0)

    def test_documented_context_keeps_unverified_description_as_flagged_evidence(self):
        context = _compact_documented_product_context({
            "id": 9,
            "client_id": "product:9",
            "name": "PRODUIT EXEMPLE 100ML",
            "brand": "Exemple",
            "description": (
                "Description catalogue utile mais encore a confirmer pour "
                "ce paquet exact."
            ),
            "description_status": "complete_unverified",
            "purpose": "Usage catalogue provisoire",
            "usage_notes": "Indice de recherche seulement",
            "_verified_fields": [],
            "_field_sources": {
                "description": {"source": "Familiprix import"},
            },
        })

        self.assertEqual(context["brand"], "Exemple")
        self.assertFalse(context["catalogue_description"]["verified"])
        self.assertEqual(
            context["catalogue_description"]["source"],
            "Familiprix import",
        )
        self.assertEqual(
            context["catalogue_clues_to_confirm"]["purpose"],
            "Usage catalogue provisoire",
        )
        self.assertIn(
            "usage_notes", context["search_clues_not_product_facts"]
        )

    def test_documented_context_prefers_exact_facts_over_marketing_preamble(self):
        description = (
            "Il y a des moments ou chaque seconde compte. Dans ces instants, "
            "le bien-etre merite une reponse moderne. Chaque capsule contient "
            "200 mg d'ibuprofene. Ce format comprend 40 Liqui-Gels et soulage "
            "temporairement les maux de tete et la fievre."
        )

        excerpt = _description_excerpt_for_ai(description, max_chars=150)
        context = _compact_documented_product_context({
            "id": 10,
            "client_id": "product:10",
            "name": "ADVIL 200MG LIQ/GEL CA40",
            "description": description,
            "description_status": "verified",
            "strength": "200.0 MG / 0.0",
            "_verified_fields": ["description", "strength"],
        })

        self.assertIn("200 mg", excerpt)
        self.assertIn("40 Liqui-Gels", excerpt)
        self.assertNotIn("chaque seconde compte", excerpt.lower())
        self.assertEqual(context["verified_facts"]["strength"], "200 MG")
        self.assertIn(
            "200 mg", context["catalogue_description"]["text"]
        )
        self.assertNotIn(
            "chaque seconde compte",
            context["catalogue_description"]["text"].lower(),
        )
        documents = retrieve_client_documentation([{
            "id": 10,
            "client_id": "product:10",
            "name": "ADVIL 200MG LIQ/GEL CA40",
            "description": description,
            "description_status": "verified",
            "_verified_fields": ["description"],
            "_field_sources": {
                "description": {
                    "source": "Familiprix",
                    "source_url": "https://example.test/product",
                },
            },
        }])
        verified_document = next(
            item for item in documents
            if item["source_id"].startswith("catalog:")
        )
        self.assertIn("200 mg", verified_document["evidence"])
        self.assertNotIn(
            "chaque seconde compte",
            verified_document["evidence"].lower(),
        )

    def test_kimi_k26_uses_bounded_non_thinking_mode_by_default(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({
            "choices": [{
                "message": {"content": json.dumps({"answer": "ok"})},
            }],
            "usage": {},
        }).encode("utf-8")

        with patch("routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k2.6"), \
             patch("routes.ai.KIMI_MODEL", "kimi-k2.6"), \
             patch("routes.ai._safe_urlopen", return_value=response) as opener, \
             patch("routes.ai._log_ai_usage"):
            _kimi_json_request(
                [{"role": "user", "content": "documented"}],
                max_tokens=1800, quality_mode=True,
            )
            documented_payload = json.loads(
                opener.call_args.args[0].data.decode("utf-8")
            )
            _kimi_json_request(
                [{"role": "user", "content": "plan"}],
                max_tokens=450, quality_mode=False,
                schema_name="query_plan",
                schema={
                    "type": "object",
                    "properties": {"answer": {"type": "string"}},
                    "required": ["answer"],
                    "additionalProperties": False,
                },
            )
            plan_payload = json.loads(
                opener.call_args.args[0].data.decode("utf-8")
            )

        self.assertEqual(
            documented_payload["thinking"], {"type": "disabled"}
        )
        self.assertEqual(plan_payload["thinking"], {"type": "disabled"})
        self.assertEqual(plan_payload["response_format"]["type"], "json_schema")
        self.assertEqual(
            plan_payload["response_format"]["json_schema"]["name"],
            "query_plan",
        )

    def test_kimi_planner_override_uses_small_non_thinking_json_request(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({
            "choices": [{
                "message": {"content": json.dumps({
                    "product_family": "savon pour les mains",
                    "search_queries": ["savon mains", "hand soap"],
                }, ensure_ascii=False)},
            }],
            "usage": {},
        }, ensure_ascii=False).encode("utf-8")

        with patch("routes.ai._safe_urlopen", return_value=response) as opener, \
             patch("routes.ai._log_ai_usage"):
            result = _kimi_json_request(
                [{"role": "user", "content": "savon"}],
                max_tokens=320,
                realtime_model=True,
                schema_name="client_query_plan",
                schema=ai_module._CLIENT_QUERY_PLAN_SCHEMA,
                model_override="moonshot-v1-8k",
            )

        payload = json.loads(opener.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(result["product_family"], "savon pour les mains")
        self.assertEqual(payload["model"], "moonshot-v1-8k")
        self.assertEqual(payload["max_tokens"], 320)
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertNotIn("thinking", payload)
        self.assertNotIn("stream", payload)

    def test_kimi_documented_request_is_not_downgraded_by_realtime_flag(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({
            "choices": [{
                "message": {"content": json.dumps({"answer": "ok"})},
            }],
            "usage": {},
        }).encode("utf-8")

        with patch("routes.ai.KIMI_REALTIME_MODEL", "moonshot-v1-8k"), \
             patch("routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k3"), \
             patch("routes.ai._available_kimi_models") as available, \
             patch("routes.ai._safe_urlopen", return_value=response) as opener, \
             patch(
                 "routes.ai._read_kimi_stream",
                 return_value=({"answer": "ok"}, {}),
             ), \
             patch("routes.ai._log_ai_usage"):
            result = _kimi_json_request(
                [{"role": "user", "content": "question courte"}],
                max_tokens=500,
                quality_mode=True,
                realtime_model=True,
            )

        payload = json.loads(opener.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(result, {"answer": "ok"})
        self.assertEqual(payload["model"], "kimi-k3")
        self.assertEqual(payload["reasoning_effort"], "low")
        self.assertTrue(payload["stream"])
        self.assertNotIn("thinking", payload)
        available.assert_not_called()

    def test_kimi_realtime_k26_reads_bounded_stream(self):
        expected = {
            "answer": "Réponse IA directe.",
            "key_points": [],
            "selected_product_ids": ["product:1"],
            "source_ids": ["store-plan"],
        }
        event = json.dumps({
            "choices": [{
                "delta": {
                    "content": json.dumps(expected, ensure_ascii=False),
                },
            }],
            "usage": {"prompt_tokens": 20, "completion_tokens": 10},
        }, ensure_ascii=False)

        class StreamResponse:
            def __init__(self):
                self.lines = iter([
                    f"data: {event}\n\n".encode("utf-8"),
                    b"data: [DONE]\n\n",
                ])

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def readline(self):
                return next(self.lines, b"")

        with patch(
            "routes.ai.KIMI_DOCUMENTED_MODEL", "kimi-k2.6"
        ), patch(
            "routes.ai._safe_urlopen", return_value=StreamResponse()
        ) as opener, patch("routes.ai._log_ai_usage"):
            result = _kimi_json_request(
                [{"role": "user", "content": "question"}],
                max_tokens=700,
                quality_mode=True,
                realtime_model=True,
                timeout_seconds=8,
            )

        payload = json.loads(opener.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(result, expected)
        self.assertEqual(payload["model"], "kimi-k2.6")
        self.assertTrue(payload["stream"])
        self.assertEqual(payload["thinking"], {"type": "disabled"})

    def test_documented_kimi_timeout_makes_only_one_paid_request(self):
        with patch("routes.ai.KIMI_API_KEY", "secret"), \
             patch("routes.ai._safe_urlopen", side_effect=TimeoutError("slow")) as opener:
            result = _kimi_json_request(
                [{"role": "user", "content": "test"}],
                max_tokens=1100,
                quality_mode=True,
            )

        self.assertIsNone(result)
        self.assertEqual(opener.call_count, 1)

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

    def test_comparison_context_covers_each_requested_product_type(self):
        candidates = [{
            "id": index,
            "name": f"Pansement transparent {index}",
            "description": "Film transparent imperméable.",
        } for index in range(1, 10)] + [{
            "id": 20,
            "name": "Pansement hydrocolloïde",
            "description": "Coussinet hydrocolloïde absorbant.",
        }]

        selected = select_client_answer_candidates(
            candidates,
            limit=4,
            question=(
                "Quelle est la différence entre un pansement hydrocolloïde "
                "et un pansement transparent?"
            ),
        )

        self.assertIn(20, [item["id"] for item in selected])
        self.assertTrue(any("transparent" in item["name"].lower() for item in selected))

    def test_comparison_context_prefers_distinct_sides_over_combined_product(self):
        candidates = [{
            "id": 1,
            "name": "Sensodyne blanchissant",
            "description": "Dentifrice blanchissant pour dents sensibles.",
        }, {
            "id": 2,
            "name": "Sensodyne sensibilité",
            "description": "Dentifrice pour dents sensibles.",
        }, {
            "id": 3,
            "name": "Crest blanchissant",
            "description": "Dentifrice blanchissant pour retirer les taches.",
        }]

        selected = select_client_answer_candidates(
            candidates,
            limit=2,
            question=(
                "Différence entre un dentifrice pour dents sensibles "
                "et un dentifrice blanchissant"
            ),
        )

        self.assertEqual({item["id"] for item in selected}, {2, 3})

    def test_small_context_avoids_duplicate_visible_names(self):
        candidates = [{
            "id": 1, "name": "ADVIL 200MG CO100",
            "description": "Comprimés.",
        }, {
            "id": 2, "name": "ADVIL 200MG CO100",
            "description": "Comprimés avec autre description.",
        }, {
            "id": 3, "name": "TYLENOL 500MG CO100",
            "description": "Comprimés d'acétaminophène.",
        }, {
            "id": 4, "name": "ALEVE 220MG CO24",
            "description": "Comprimés de naproxène.",
        }]

        selected = select_client_answer_candidates(candidates, limit=3)

        self.assertEqual(
            [item["name"] for item in selected],
            ["ADVIL 200MG CO100", "TYLENOL 500MG CO100", "ALEVE 220MG CO24"],
        )

    def test_brand_diversity_does_not_reselect_a_seeded_brand(self):
        candidates = [{
            "id": 1, "name": "ADVIL 200MG CO100", "brand": "Advil",
            "description": "Réduit la fièvre.",
        }, {
            "id": 2, "name": "ADVIL 200MG MINI GEL", "brand": "Advil",
            "description": "Réduit la fièvre.",
        }, {
            "id": 3, "name": "TYLENOL 500MG CO100", "brand": "Tylenol",
            "description": "Réduit la fièvre.",
        }, {
            "id": 4, "name": "ALEVE 220MG CO24", "brand": "Aleve",
            "description": "Réduit la fièvre.",
        }]

        selected = select_client_answer_candidates(
            candidates,
            limit=3,
            diversify_brands=True,
            question="Que prendre pour la fièvre?",
        )

        self.assertEqual(
            [item["brand"] for item in selected],
            ["Advil", "Tylenol", "Aleve"],
        )

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

    def test_query_plan_cache_avoids_repaying_for_identical_conversation(self):
        parsed = {
            "intent": "toothbrush", "corrected_query": "brosse à dents électrique",
            "search_queries": ["brosse à dents électrique"],
            "keywords": ["brosse", "dents", "électrique"],
            "must_include": ["brosse à dents électrique"], "exclude": ["dentifrice"],
            "wants_all": False, "needs_comparison": False,
            "answer_language": "fr", "medical": False,
            "retrieval_scope": "store",
        }
        history = [{"role": "user", "content": "Je cherche une brosse"}]
        with patch(
            "routes.ai.configured_ai_provider", return_value={"name": "kimi"},
        ), patch(
            "routes.ai._provider_structured_request", return_value=parsed,
        ) as provider:
            first = ai_module.generate_client_query_plan(
                "électrique si possible", history,
            )
            second = ai_module.generate_client_query_plan(
                "électrique si possible", history,
            )

        self.assertEqual(first, second)
        self.assertEqual(first["retrieval_scope"], "store")
        provider.assert_called_once()
        self.assertNotIn("required_schema", provider.call_args.args[1])
        self.assertEqual(provider.call_args.kwargs["max_tokens"], 320)
        self.assertEqual(
            provider.call_args.kwargs["model_override"], "moonshot-v1-8k"
        )

    def test_query_planner_retries_k26_only_when_compact_model_is_rejected(self):
        parsed = {
            "product_family": "shampoing sec",
            "answer_goal": "Trouver un shampoing sec.",
            "corrected_query": "shampoing sec",
            "search_queries": ["shampoing sec", "dry shampoo"],
            "constraints": [],
            "evidence_fields": ["format", "parfum"],
            "exclude": [],
            "answer_language": "fr",
            "medical": False,
            "retrieval_scope": "store",
        }

        def provider_side_effect(*_args, **kwargs):
            if kwargs["model_override"] == "moonshot-v1-8k":
                ai_module._AI_LAST_ERROR = (
                    "Le service est indisponible (HTTP 404)."
                )
                return None
            return parsed

        with patch(
            "routes.ai.configured_ai_provider",
            return_value={"name": "kimi", "model": "kimi-k2.6"},
        ), patch(
            "routes.ai.KIMI_PLANNER_MODEL", "moonshot-v1-8k"
        ), patch(
            "routes.ai.KIMI_REALTIME_MODEL", "kimi-k2.6"
        ), patch(
            "routes.ai._provider_structured_request",
            side_effect=provider_side_effect,
        ) as provider:
            plan = ai_module.generate_client_query_plan(
                "Je voudrais du shampoing sec en aérosol"
            )

        self.assertEqual(plan["product_family"], "shampoing sec")
        self.assertEqual(provider.call_count, 2)
        self.assertEqual(
            provider.call_args_list[1].kwargs["model_override"], "kimi-k2.6"
        )
        self.assertEqual(ai_module._AI_LAST_ERROR, "")

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

    def test_client_endpoint_returns_only_products_selected_by_ai(self):
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
            "retrieval_scope": "store",
        }
        verified = {
            "answer": "Advil est le produit trouve.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek", "label": "DeepSeek", "model": "test"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.generate_client_query_plan", return_value=plan) as planner, \
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
        self.assertEqual(
            [item["client_id"] for item in payload["products"]],
            ["product:1"],
        )
        self.assertEqual(payload["highlighted_product_ids"], ["product:1"])
        planner.assert_called_once()
        verifier.assert_called_once()

    def test_kimi_reads_the_full_question_in_one_answer_request(self):
        candidate = {
            "id": 1, "client_id": "product:1",
            "name": "BENYLIN SIROP GORGE TOUX 250ML", "brand": "Benylin",
            "barcode": "111", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "2", "position": "1", "in_stock": 1,
        }
        question = "Quelle est le meilleur produit pour la gorge et pourquoi"
        documented = {
            "answer": "Le choix dépend des symptômes; ce produit vise la gorge.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
            "key_points": [], "comparisons": [], "useful_guidance": [],
            "important_checks": [], "source_ids": ["store-plan"],
            "degraded": False, "warning": "",
        }
        captured = {}
        semantic_plan = build_client_query_plan(question, "documented")

        def retrieve(question, plan, limit=60):
            captured["question"] = question
            captured["plan"] = dict(plan)
            return [candidate]

        with patch(
            "routes.ai.configured_ai_provider",
            return_value={"name": "kimi", "label": "Kimi", "model": "kimi-k3"},
        ), patch(
            "routes.ai._check_ai_rate_limit", return_value=True
        ), patch(
            "routes.ai.generate_client_query_plan", return_value=semantic_plan,
        ) as planner, patch(
            "routes.products.hybrid_client_candidates", side_effect=retrieve
        ), patch(
            "routes.products.hydrate_candidate_images"
        ), patch(
            "routes.ai.retrieve_client_documentation", return_value=[{
                "source_id": "store-plan", "title": "Plan actuel",
                "publisher": "Familiprix Locator", "url": "",
                "evidence": "BENYLIN", "candidate_ids": ["product:1"],
            }]
        ), patch(
            "routes.ai.generate_documented_client_answer",
            return_value=documented,
        ) as generator, patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": question,
                    "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        planner.assert_not_called()
        generator.assert_called_once()
        self.assertEqual(generator.call_args.args[0], question)
        self.assertEqual(captured["question"], question)
        self.assertEqual(captured["plan"]["corrected_query"], question)
        self.assertTrue(captured["plan"]["semantic_search"])

    def test_kimi_semantic_plan_retries_weak_local_retrieval(self):
        candidate = {
            "id": 8, "client_id": "product:8",
            "name": "GRAVOL 50MG CO20", "brand": "Gravol",
            "description": "Produit du catalogue pour les nausees.",
            "barcode": "888", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "3", "position": "1", "in_stock": 1,
        }
        semantic_plan = {
            "intent": "nausea_relief",
            "corrected_query": "Produit pour soulager les nausees",
            "search_queries": ["nausees Gravol", "dimenhydrinate"],
            "keywords": ["nausees", "Gravol", "dimenhydrinate"],
            "must_include": ["nausees"],
            "exclude": [],
            "wants_all": False,
            "needs_comparison": False,
            "answer_language": "fr",
            "medical": True,
        }
        documented = {
            "answer": (
                "Le Gravol est le produit du plan le plus directement lie a "
                "cette demande; confirmez le format exact sur l'emballage."
            ),
            "selected_product_ids": ["product:8"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
            "key_points": [],
            "comparisons": [],
            "useful_guidance": [],
            "important_checks": [],
            "source_ids": ["store-plan"],
            "degraded": False,
            "warning": "",
        }
        with patch(
            "routes.ai.configured_ai_provider",
            return_value={"name": "kimi", "label": "Kimi", "model": "kimi-k3"},
        ), patch(
            "routes.ai._check_ai_rate_limit", return_value=True
        ) as rate_limit, patch(
            "routes.products.hybrid_client_candidates",
            return_value=[candidate],
        ) as retrieval, patch(
            "routes.ai.generate_client_query_plan", return_value=semantic_plan
        ) as planner, patch(
            "routes.products.hydrate_candidate_images"
        ), patch(
            "routes.ai.retrieve_client_documentation", return_value=[{
                "source_id": "store-plan",
                "title": "Plan actuel",
                "publisher": "Familiprix Locator",
                "url": "",
                "evidence": candidate["name"],
                "candidate_ids": ["product:8"],
            }]
        ), patch(
            "routes.ai.generate_documented_client_answer",
            return_value=documented,
        ), patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Il me faut quelque chose pour calmer le coeur leve",
                    "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json()["highlighted_product_ids"], ["product:8"]
        )
        self.assertEqual(retrieval.call_count, 1)
        self.assertEqual(retrieval.call_args.args[0], "Il me faut quelque chose pour calmer le coeur leve")
        self.assertTrue(retrieval.call_args.args[1]["semantic_search"])
        planner.assert_not_called()
        rate_limit.assert_called_once()

    def test_kimi_plan_prevents_a_wrong_object_from_entering_retrieval(self):
        unrelated = {
            "id": 1, "client_id": "product:1",
            "name": "TASSE TRANSPARENTE 1", "brand": "Test",
            "description": "Tasse en plastique transparent.",
            "barcode": "111", "aisle": "4", "side": "A",
            "section": "1", "shelf": "2", "position": "1",
        }
        dressing = {
            "id": 2, "client_id": "product:2",
            "name": "PARAMEDIC PANS TRANSP 5CMX1M", "brand": "Paramedic",
            "description": "Pansement transparent pour proteger une blessure.",
            "barcode": "222", "aisle": "2", "side": "B",
            "section": "6", "shelf": "4", "position": "3",
        }
        semantic_plan = {
            "intent": "transparent_wound_dressing",
            "corrected_query": "pansement transparent pour blessure",
            "search_queries": ["pansement transparent", "film pour plaie"],
            "keywords": ["pansement", "transparent", "blessure", "plaie"],
            "must_include": ["pansement transparent"],
            "exclude": ["tasse"], "wants_all": False,
            "needs_comparison": False, "answer_language": "fr",
            "medical": True,
        }
        documented = {
            "answer": "Le pansement transparent est dans le plan du magasin.",
            "selected_product_ids": ["product:2"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
            "key_points": [], "comparisons": [], "useful_guidance": [],
            "important_checks": [], "source_ids": ["store-plan"],
        }
        with patch(
            "routes.ai.configured_ai_provider",
            return_value={"name": "kimi", "label": "Kimi", "model": "kimi-k3"},
        ), patch(
            "routes.ai._check_ai_rate_limit", return_value=True,
        ), patch(
            "routes.products.hybrid_client_candidates",
            return_value=[dressing],
        ) as retrieval, patch(
            "routes.ai.generate_client_query_plan", return_value=semantic_plan,
        ) as planner, patch(
            "routes.products.hydrate_candidate_images",
        ), patch(
            "routes.ai.retrieve_client_documentation", return_value=[{
                "source_id": "store-plan", "title": "Plan actuel",
                "publisher": "Familiprix Locator", "url": "",
                "evidence": dressing["name"],
                "candidate_ids": ["product:2"],
            }],
        ), patch(
            "routes.ai.generate_documented_client_answer",
            return_value=documented,
        ), patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "objet transparent pour proteger une blessure",
                    "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["client_id"] for item in response.get_json()["products"]],
            ["product:2"],
        )
        self.assertEqual(retrieval.call_count, 1)
        self.assertEqual(
            retrieval.call_args.args[0],
            "objet transparent pour proteger une blessure",
        )
        self.assertTrue(retrieval.call_args.args[1]["semantic_search"])
        planner.assert_not_called()

    def test_semantic_retry_clears_unrelated_cards_when_object_is_absent(self):
        unrelated = {
            "id": 1, "client_id": "product:1",
            "name": "DOVE MEN ANTI VAPO 107G", "brand": "Dove",
            "description": "Antisudorifique en vaporisateur.",
            "barcode": "111", "aisle": "4", "side": "A",
            "section": "1", "shelf": "2", "position": "1",
        }
        semantic_plan = {
            "intent": "insect_repellent",
            "corrected_query": "vaporisateur chasse moustiques",
            "search_queries": ["chasse moustiques", "insectifuge"],
            "keywords": ["moustiques", "insectifuge"],
            "must_include": ["moustiques"],
            "exclude": ["antisudorifique"],
            "wants_all": False, "needs_comparison": False,
            "answer_language": "fr", "medical": False,
        }
        documented = {
            "answer": "Aucun chasse-moustiques n'est localise dans le plan actuel.",
            "selected_product_ids": [], "follow_up_questions": [],
            "safety_flags": [], "pharmacist_referral": False,
            "pharmacist_reason": "", "key_points": [], "comparisons": [],
            "useful_guidance": [], "important_checks": [],
            "source_ids": ["store-plan"],
        }
        with patch(
            "routes.ai.configured_ai_provider",
            return_value={"name": "kimi", "label": "Kimi", "model": "kimi-k3"},
        ), patch(
            "routes.ai._check_ai_rate_limit", return_value=True,
        ), patch(
            "routes.products.hybrid_client_candidates",
            return_value=[],
        ) as retrieval, patch(
            "routes.ai.generate_client_query_plan", return_value=semantic_plan,
        ), patch(
            "routes.products.hydrate_candidate_images",
        ), patch(
            "routes.ai.retrieve_client_documentation", return_value=[],
        ), patch(
            "routes.ai.generate_documented_client_answer",
            return_value=documented,
        ), patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "spray anti moustique",
                    "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["products"], [])
        self.assertEqual(retrieval.call_count, 1)

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
        self.assertEqual(
            [item["client_id"] for item in payload["products"]],
            ["product:1"],
        )
        self.assertEqual(payload["highlighted_product_ids"], ["product:1"])
        verifier.assert_called_once()

    def test_documented_code_question_sends_exact_product_file_to_ai(self):
        candidate = {
            "id": 1884, "client_id": "product:1884",
            "name": "BIOMEDIC GEL ANALG GLACE 255G", "brand": "Biomedic",
            "barcode": "063848907665", "product_code": "146962",
            "description": (
                "Gel for temporary muscular and joint pain relief."
            ),
            "description_status": "verified",
            "_verified_fields": ["description", "dosage_form", "purpose"],
            "dosage_form": "GEL", "purpose": "Temporary pain relief.",
            "aisle": "Labo", "side": "Gauche", "section": "3",
            "shelf": "2", "position": "5", "in_stock": 1,
            "_identifiers": [],
            "_exact_identifier_matches": [{
                "field": "upc", "value": "063848907665",
            }],
        }
        documented = {
            "answer": (
                "Ce code correspond au gel Biomedic; il s'agit d'un gel, "
                "pas d'un liquide."
            ),
            "selected_product_ids": ["product:1884"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
            "key_points": [], "comparisons": [], "useful_guidance": [],
            "important_checks": [], "source_ids": [],
        }
        with patch(
            "routes.products.resolve_client_exact_identifiers",
            return_value=[candidate],
        ), patch(
            "routes.products.hybrid_client_candidates"
        ) as semantic_search, patch(
            "routes.products.hydrate_candidate_images"
        ), patch(
            "routes.ai.configured_ai_provider", return_value={"name": "kimi"},
        ), patch(
            "routes.ai._check_ai_rate_limit", return_value=True,
        ), patch(
            "routes.ai.retrieve_client_documentation", return_value=[],
        ), patch(
            "routes.ai.generate_documented_client_answer",
            return_value=documented,
        ) as answerer, patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": (
                        "063848907665 c'est le UPC. Est-ce liquide ou en gel?"
                    ),
                    "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(
            [item["client_id"] for item in payload["products"]],
            ["product:1884"],
        )
        self.assertIn("gel", payload["answer"].lower())
        semantic_search.assert_not_called()
        sent_candidates = answerer.call_args.args[2]
        self.assertEqual(sent_candidates[0]["description"], candidate["description"])
        self.assertEqual(
            sent_candidates[0]["_exact_identifier_matches"],
            candidate["_exact_identifier_matches"],
        )

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
        with patch("routes.products.resolve_client_exact_identifiers", return_value=[candidate]), \
             patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
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

    def test_documented_melatonin_assortment_uses_immediate_grounded_answer(self):
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
        documented = {
            "answer": (
                "Pour choisir, distinguez l'endormissement occasionnel, l'horaire "
                "décalé et le décalage horaire."
            ),
            "selected_product_ids": [
                product["client_id"] for product in candidates
            ],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": True,
            "pharmacist_reason": "Confirmer le choix avec le pharmacien.",
            "key_points": [],
            "comparisons": [],
            "useful_guidance": [],
            "important_checks": [],
            "source_ids": ["store-plan"],
            "degraded": False,
            "warning": "",
        }
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents) as retriever, \
             patch(
                 "routes.ai.generate_documented_client_answer",
                 return_value=documented,
             ) as generator, \
             patch(
                 "routes.ai.configured_ai_provider",
                 return_value={"name": "deepseek"},
             ) as provider, \
             patch(
                 "routes.ai.generate_client_query_plan",
                 return_value=build_client_query_plan(
                     "Montre tous les types et saveurs de mélatonine", "documented"
                 ),
             ), \
             patch("routes.ai._check_ai_rate_limit", return_value=True) as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Montre tous les types et saveurs de mélatonine",
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["degraded"])
        self.assertEqual(payload["answer"], documented["answer"])
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()
        retriever.assert_called_once_with(
            unittest.mock.ANY,
            unittest.mock.ANY,
            include_live_regulatory=False,
        )

    def test_kimi_composes_common_documented_questions_instead_of_bypassing_ai(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "TYLENOL 500MG X/F CO100", "brand": "Tylenol",
            "description": "Contient de l'acétaminophène.",
            "barcode": "1001", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "3", "position": "4",
        }]
        documented = {
            "answer": "Pour un mal de tête, voici le choix pertinent et les vérifications utiles.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
            "key_points": [],
            "comparisons": [],
            "useful_guidance": [],
            "important_checks": [],
            "source_ids": ["store-plan"],
            "degraded": False,
            "warning": "",
        }
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.retrieve_client_documentation", return_value=[]), \
             patch(
                 "routes.ai.generate_documented_client_answer",
                 return_value=documented,
             ) as generator, \
              patch(
                  "routes.ai.configured_ai_provider",
                  return_value={"name": "kimi", "label": "Kimi", "model": "kimi-k2.6"},
              ), \
              patch(
                  "routes.ai.generate_client_query_plan",
                  return_value=build_client_query_plan(
                      "Jai mal à la tête que prendre", "documented"
                  ),
              ), \
              patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Jai mal à la tête que prendre",
                    "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["answer"], documented["answer"])
        generator.assert_called_once()

    def test_realtime_empty_stream_uses_grounded_products_instead_of_false_empty(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "BIOMEDIC MELATON 5MG CO100",
            "description": "Melatonine 5 mg, comprimes.",
            "barcode": "1001", "aisle": "2", "side": "A",
            "section": "3", "shelf": "2", "position": "1",
        }]
        empty_stream = {
            "answer": "", "selected_product_ids": [],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch(
            "routes.products.hybrid_client_candidates", return_value=candidates,
        ), patch(
            "routes.products.hydrate_candidate_images",
        ), patch(
            "routes.ai.generate_verified_client_answer",
            return_value=empty_stream,
        ), patch(
            "routes.ai.configured_ai_provider",
            return_value={"name": "kimi", "label": "Kimi", "model": "kimi-k2.6"},
        ), patch(
            "routes.ai._check_ai_rate_limit", return_value=True,
        ), patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Quels types de melatonine avons-nous et comment choisir?",
                    "mode": "ai",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["degraded"])
        self.assertIn("Pour choisir une", payload["answer"])
        self.assertEqual(
            [product["name"] for product in payload["products"]],
            ["BIOMEDIC MELATON 5MG CO100"],
        )

    def test_documented_form_comparison_skips_ai_delay(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "ADVIL 200MG CO100", "description": "Comprimés 200 mg",
            "barcode": "1001", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "4", "position": "1",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "ADVIL 200MG LIQ/GEL CA115", "description": "Liqui-gels 200 mg",
            "barcode": "1002", "aisle": "Labo", "side": "A",
            "section": "2", "shelf": "4", "position": "2",
        }]
        documents = [{
            "source_id": "store-plan", "title": "Plan actuel",
            "publisher": "Familiprix Locator", "url": "", "evidence": "",
            "candidate_ids": [product["client_id"] for product in candidates],
        }]
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents), \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": (
                        "Quelle est la différence entre les comprimés et les "
                        "liqui-gels Advil?"
                    ),
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertLess(payload["elapsed_ms"], 1000)
        self.assertIn("façon de les prendre", payload["answer"])
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()

    def test_catalogue_comparison_fallback_remains_useful_if_ai_payload_is_invalid(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "SENSODYNE SENSIBILITE 100ML",
            "description": "Dentifrice conçu pour les dents sensibles.",
            "barcode": "1001", "aisle": "3", "side": "A",
            "section": "4", "shelf": "2", "position": "1",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "COLGATE BLANCHISSANT 120ML",
            "description": "Dentifrice qui aide à retirer les taches de surface.",
            "barcode": "1002", "aisle": "3", "side": "A",
            "section": "4", "shelf": "2", "position": "2",
        }]
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": (
                        "Quelle est la différence entre un dentifrice pour dents "
                        "sensibles et un dentifrice blanchissant?"
                    ),
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertLess(payload["elapsed_ms"], 1000)
        self.assertTrue(payload["degraded"])
        self.assertIn("objectif principal", payload["answer"])
        self.assertEqual(
            [
                point["heading"]
                for point in payload["advice"]["documentation"]["key_points"]
            ],
            ["Dents sensibles", "Blanchissant", "Produit combiné", "Quand référer"],
        )
        self.assertIn(
            "Association dentaire canadienne",
            {
                source["publisher"]
                for source in payload["advice"]["documentation"]["sources"]
            },
        )
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()

    def test_wound_comparison_fallback_remains_useful_if_ai_payload_is_invalid(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "PARAMEDIC PANS HYDRO 10X10CM 1",
            "description": "Pansement hydrocolloïde absorbant.",
            "barcode": "1001", "aisle": "2", "side": "B",
            "section": "6", "shelf": "7", "position": "3",
        }, {
            "id": 2, "client_id": "product:2",
            "name": "PARAMEDIC PANS TRANSP 5CMX1M 1",
            "description": "Film transparent imperméable.",
            "barcode": "1002", "aisle": "2", "side": "B",
            "section": "6", "shelf": "6", "position": "4",
        }]
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": (
                        "Quelle est la différence entre un pansement hydrocolloïde "
                        "et un pansement transparent?"
                    ),
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertLess(payload["elapsed_ms"], 1000)
        self.assertTrue(payload["degraded"])
        self.assertIn("forme un gel", payload["answer"])
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()

    def test_toothbrush_fallback_remains_useful_if_ai_payload_is_invalid(self):
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
        self.assertTrue(payload["degraded"])
        self.assertIn("brosse à pile", payload["answer"])
        self.assertIn("brosse rechargeable", payload["answer"])
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()
        retriever.assert_called_once_with(
            unittest.mock.ANY,
            unittest.mock.ANY,
            include_live_regulatory=False,
        )

    def test_headache_fallback_remains_useful_if_ai_payload_is_invalid(self):
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
        self.assertTrue(payload["degraded"])
        self.assertTrue(payload["warning"])
        self.assertIn("mal de tête", payload["answer"])
        self.assertIn("acétaminophène", payload["answer"])
        self.assertEqual(
            [point["heading"] for point in payload["advice"]["documentation"]["key_points"]],
            ["Choix rapide", "Avant de proposer", "Ne pas combiner", "Quand référer"],
        )
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()
        retriever.assert_called_once_with(
            unittest.mock.ANY,
            unittest.mock.ANY,
            include_live_regulatory=False,
        )

    def test_fever_fallback_remains_useful_if_ai_payload_is_invalid(self):
        candidates = [{
            "id": 1, "client_id": "product:1",
            "name": "TYLENOL 500MG X/F CO100", "brand": "Tylenol",
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
        with patch("routes.products.hybrid_client_candidates", return_value=candidates), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.generate_documented_client_answer") as generator, \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai._check_ai_rate_limit") as rate_limit, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Jai de la fievre que prendre",
                    "mode": "documented",
                })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertLess(payload["elapsed_ms"], 1000)
        self.assertTrue(payload["degraded"])
        self.assertIn("Pour soulager une fièvre", payload["answer"])
        self.assertIn("acétaminophène", payload["answer"])
        generator.assert_called_once()
        self.assertGreaterEqual(provider.call_count, 1)
        rate_limit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
