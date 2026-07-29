import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from flask import Flask

from memory_guard import memory_intensive_task, memory_snapshot
from routes import ai, products


class MemorySafetyTests(unittest.TestCase):
    def test_warm_product_corpus_never_waits_for_background_memory_task(self):
        original_cache = dict(products._PROD_CACHE)
        warm_rows = [({"id": 1, "name": "Advil"}, {"_name": "advil"})]
        try:
            products._PROD_CACHE.update(
                rows=warm_rows,
                generation=12,
                state_checked_at=time.time(),
            )
            with patch.object(
                products, "product_search_generation", return_value=12,
            ), patch.object(
                products, "memory_intensive_task",
                side_effect=AssertionError("warm search must not wait"),
            ):
                self.assertIs(
                    products._employee_product_corpus(object()), warm_rows
                )
        finally:
            products._PROD_CACHE.clear()
            products._PROD_CACHE.update(original_cache)

    def test_media_updates_do_not_expire_the_warm_search_corpus(self):
        original_cache = dict(products._PROD_CACHE)
        warm_rows = [({"id": 1, "name": "Advil"}, {"_name": "advil"})]
        try:
            products._PROD_CACHE.update(
                rows=warm_rows,
                generation=12,
                state_checked_at=0,
            )
            with patch.object(
                products, "product_search_generation", return_value=12,
            ), patch.object(
                products, "_schedule_product_corpus_refresh",
            ) as refresh, patch.object(
                products, "memory_intensive_task",
                side_effect=AssertionError("warm search must not wait"),
            ):
                self.assertIs(
                    products._employee_product_corpus(object()), warm_rows
                )
                refresh.assert_not_called()
        finally:
            products._PROD_CACHE.clear()
            products._PROD_CACHE.update(original_cache)

    def test_invalidated_warm_corpus_is_served_while_refresh_starts(self):
        original_cache = dict(products._PROD_CACHE)
        warm_rows = [({"id": 1, "name": "Advil"}, {"_name": "advil"})]
        try:
            products._PROD_CACHE.update(
                rows=warm_rows,
                generation=12,
                state_checked_at=0,
            )
            with patch.object(
                products, "product_search_generation", return_value=13,
            ), patch.object(
                products, "_schedule_product_corpus_refresh",
            ) as refresh, patch.object(
                products, "memory_intensive_task",
                side_effect=AssertionError("stale warm search must not wait"),
            ):
                self.assertIs(
                    products._employee_product_corpus(object()), warm_rows
                )
                refresh.assert_called_once_with()
        finally:
            products._PROD_CACHE.clear()
            products._PROD_CACHE.update(original_cache)

    def test_bootstrap_payload_keeps_media_but_removes_duplicate_identifiers(self):
        payload = products.bootstrap_product_payload({
            "id": 42,
            "name": "TYLENOL 500MG X/F FAC CO100",
            "description": "Acetaminophen 500 mg.",
            "image_url": "https://img.test/tylenol.jpg",
            "barcode": "062600142320",
            "product_code": "664375",
            "aisle": "Labo",
            "side": "Gauche",
            "section": "2",
            "shelf": "6",
            "position": "1",
            "facings": 2,
            "is_plano": 1,
            "in_stock": 1,
            "linked_position": "",
            "flipped_label": 0,
            "created_at": "server-only",
            "identifiers": [
                {"type": "GTIN", "value": "062600142320", "status": "confirmed"},
                {"type": "FAMILIPRIX_CODE", "value": "664375", "status": "confirmed"},
                {"type": "DIN", "value": "00559407", "status": "confirmed"},
                {"type": "HEALTH_CANADA_ID", "value": "5255", "status": "confirmed"},
            ],
            "regulatory_identifiers": [{
                "type": "DIN",
                "value": "00559407",
                "status": "confirmed",
                "source": "server-only provenance",
                "match_method": "manual_entry",
                "confidence": 1.0,
            }],
        })

        self.assertEqual(payload["description"], "Acetaminophen 500 mg.")
        self.assertEqual(payload["image_url"], "https://img.test/tylenol.jpg")
        self.assertNotIn("created_at", payload)
        self.assertEqual(
            payload["identifiers"],
            [{
                "type": "HEALTH_CANADA_ID",
                "value": "5255",
                "status": "confirmed",
                "label": "Confirmé",
            }],
        )
        self.assertEqual(payload["regulatory_identifiers"][0]["value"], "00559407")
        self.assertNotIn("source", payload["regulatory_identifiers"][0])

    def test_product_corpus_builds_are_process_serialized(self):
        active = 0
        peak = 0
        state_lock = threading.Lock()

        @products._serialized_product_corpus
        def simulated_build():
            nonlocal active, peak
            with state_lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.02)
            with state_lock:
                active -= 1

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _index: simulated_build(), range(8)))

        self.assertEqual(peak, 1)

    def test_search_cache_keeps_one_identifier_copy_and_restores_public_fields(self):
        compact = products._compact_search_cache_product({
            "id": 42,
            "name": "TYLENOL 500MG CO100",
            "barcode": "062600142320",
            "aisle": "Labo",
            "side": "A",
            "section": "2",
            "shelf": "6",
            "position": "1",
            "brand": "",
            "description": "",
            "image_url": "",
            "search_terms": "",
            "modified_at": "audit-only",
            "primary_source": "audit-only",
            "_identifiers": [{
                "type": "DIN",
                "value": "00559407",
                "authority": "Health Canada",
                "verification_status": "requires_review",
                "match_method": "health_canada_name_candidate",
                "confidence": 0.61,
            }],
            "identifiers": [{"duplicate": True}],
            "regulatory_identifiers": [{"duplicate": True}],
        })

        self.assertNotIn("modified_at", compact)
        self.assertNotIn("primary_source", compact)
        self.assertNotIn("brand", compact)
        self.assertNotIn("description", compact)
        self.assertNotIn("image_url", compact)
        self.assertNotIn("search_terms", compact)
        self.assertNotIn("identifiers", compact)
        self.assertNotIn("regulatory_identifiers", compact)
        self.assertEqual(len(compact["_identifiers"]), 1)
        public = products.public_product_payload(compact)
        self.assertEqual(
            public["regulatory_identifiers"][0]["value"], "00559407"
        )
        self.assertEqual(
            public["regulatory_identifiers"][0]["status"], "probable"
        )

    def test_busy_product_bootstrap_returns_retry_without_waiting(self):
        app = Flask(__name__)
        self.assertFalse(products._PRODUCT_STREAM_LOCK.locked())
        products._PRODUCT_STREAM_LOCK.acquire()
        try:
            with app.test_request_context("/api/products"), patch.object(
                products, "get_db", return_value=object(),
            ), patch.object(
                products, "products_state_key", return_value=(1,),
            ), patch.object(
                products, "product_identifier_state_key", return_value=(2,),
            ), patch.object(
                products, "reference_identifier_state_key", return_value=(3,),
            ), patch.object(
                products, "client_etag_matches", return_value=False,
            ):
                response = products.get_products()
        finally:
            products._PRODUCT_STREAM_LOCK.release()

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["Retry-After"], "2")
        self.assertTrue(response.get_json()["retry"])

    def test_unstarted_product_stream_releases_its_single_flight_slot(self):
        app = Flask(__name__)
        self.assertFalse(products._PRODUCT_STREAM_LOCK.locked())
        with app.test_request_context("/api/products"), patch.object(
            products, "get_db", return_value=object(),
        ), patch.object(
            products, "products_state_key", return_value=(1,),
        ), patch.object(
            products, "product_identifier_state_key", return_value=(2,),
        ), patch.object(
            products, "reference_identifier_state_key", return_value=(3,),
        ), patch.object(
            products, "client_etag_matches", return_value=False,
        ), patch.object(
            products, "release_unused_memory",
        ):
            response = products.get_products()
            self.assertTrue(products._PRODUCT_STREAM_LOCK.locked())
            response.close()

        self.assertFalse(products._PRODUCT_STREAM_LOCK.locked())

    def test_product_stream_uses_and_closes_its_own_database_connection(self):
        app = Flask(__name__)

        class StreamDatabase:
            closed = False

            def close(self):
                self.closed = True

        stream_db = StreamDatabase()
        item = {
            "id": 1, "name": "Advil", "barcode": "1", "product_code": "",
            "aisle": "1", "side": "A", "section": "1", "shelf": "1",
            "position": "1", "facings": 1, "is_plano": 1, "in_stock": 1,
            "linked_position": "", "flipped_label": 0,
        }
        with app.test_request_context("/api/products"), patch.object(
            products, "get_db", return_value=object(),
        ), patch.object(
            products, "connect_db", return_value=stream_db,
        ), patch.object(
            products, "_products_corpus",
            return_value=[(item, {"_name": "advil"})],
        ), patch.object(
            products, "products_state_key", return_value=(1,),
        ), patch.object(
            products, "product_identifier_state_key", return_value=(2,),
        ), patch.object(
            products, "reference_identifier_state_key", return_value=(3,),
        ), patch.object(
            products, "client_etag_matches", return_value=False,
        ), patch.object(
            products, "release_unused_memory",
        ):
            response = products.get_products()
            body = b"".join(response.iter_encoded())
            response.close()

        self.assertIn(b'"name":"Advil"', body)
        self.assertTrue(stream_db.closed)
        self.assertFalse(products._PRODUCT_STREAM_LOCK.locked())

    def test_optional_reference_cache_can_be_released_before_pdf_work(self):
        original_reference = dict(products._REF_CACHE)
        original_retries = dict(products._IMAGE_FILL_RETRY_AFTER)
        try:
            products._REF_CACHE.update(
                key=("catalogue",), rows=[{"barcode": "1"}, {"barcode": "2"}],
                built_at=time.time(),
            )
            products._IMAGE_FILL_RETRY_AFTER.clear()
            products._IMAGE_FILL_RETRY_AFTER.update({
                "expired": time.time() - 1,
                "future": time.time() + 60,
            })
            with patch.object(products, "release_unused_memory"):
                released = products.release_optional_product_caches()

            self.assertEqual(released["reference_rows"], 2)
            self.assertEqual(products._REF_CACHE["rows"], [])
            self.assertNotIn("expired", products._IMAGE_FILL_RETRY_AFTER)
            self.assertIn("future", products._IMAGE_FILL_RETRY_AFTER)
        finally:
            products._REF_CACHE.clear()
            products._REF_CACHE.update(original_reference)
            products._IMAGE_FILL_RETRY_AFTER.clear()
            products._IMAGE_FILL_RETRY_AFTER.update(original_retries)

    def test_bm25_token_counter_does_not_match_substrings(self):
        haystack = "advil confort advil ibuprofene"
        self.assertEqual(products._normalized_token_count(haystack, "advil"), 2)
        self.assertEqual(products._normalized_token_count(haystack, "fort"), 0)
        self.assertEqual(products._normalized_token_count(haystack, "ibuprofene"), 1)

    def test_client_search_materializes_only_ranked_products_and_all_locations(self):
        first = {
            "id": 1, "name": "TYLENOL 500MG CO100", "brand": "Tylenol",
            "barcode": "062600142320", "in_stock": 0, "is_plano": 1,
            "aisle": "Labo", "side": "A", "section": "2",
            "shelf": "6", "position": "1",
        }
        second_location = {
            **first, "id": 2, "in_stock": 1, "section": "3", "position": "4",
        }
        unrelated = {
            "id": 3, "name": "ORAL-B TETE BR DENTS", "brand": "Oral-B",
            "barcode": "300", "in_stock": 1, "is_plano": 1,
            "aisle": "3", "side": "A", "section": "1",
            "shelf": "1", "position": "1",
        }
        corpus = [
            (first, products._product_search_row(first)),
            (second_location, products._product_search_row(second_location)),
            (unrelated, products._product_search_row(unrelated)),
        ]
        plan = {
            "corrected_query": "Tylenol", "search_queries": [],
            "keywords": [], "must_include": [], "exclude": [],
        }

        with patch.object(products, "get_db", return_value=object()), \
             patch.object(products, "_products_corpus", return_value=corpus):
            matches = products.hybrid_client_candidates(
                "Tylenol", plan, limit=10
            )

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["name"], "TYLENOL 500MG CO100")
        self.assertEqual(len(matches[0]["locations"]), 2)
        self.assertEqual(matches[0]["in_stock"], 1)

    def test_image_lookup_does_not_stop_on_a_text_only_result(self):
        text_only = {
            "name": "Detailed razor product", "brand": "Example",
            "source": "Familiprix", "image_url": "",
        }
        with_image = {
            "name": "Razor", "brand": "", "source": "UPC Item DB",
            "image_url": "https://img.test/razor.jpg",
        }

        fast, _ = ai.best_lookup_result(
            [lambda: text_only, lambda: with_image],
            max_workers=1, good_enough=1,
        )
        visual, _ = ai.best_lookup_result(
            [lambda: text_only, lambda: with_image],
            max_workers=1, good_enough=1, require_image=True,
        )

        self.assertIs(fast, text_only)
        self.assertIs(visual, with_image)

    def test_online_image_lookup_continues_to_familiprix_phase(self):
        text_only = {"name": "Known razor", "image_url": ""}
        with_image = {
            "name": "Known razor", "image_url": "https://img.test/razor.jpg"
        }
        with patch.object(
            ai, "best_lookup_result",
            side_effect=[(text_only, 30), (with_image, 30)],
        ) as lookup:
            product = ai.lookup_product_online("063848966068", require_image=True)

        self.assertEqual(product["image_url"], "https://img.test/razor.jpg")
        self.assertEqual(lookup.call_count, 2)
        self.assertTrue(all(call.kwargs["require_image"] for call in lookup.call_args_list))

    def test_background_image_lookup_has_a_strict_source_budget(self):
        with patch.object(
            ai, "best_lookup_result", side_effect=[(None, 0), (None, 0)],
        ) as lookup, patch.object(
            ai, "ai_grounded_product_lookup", return_value=None,
        ):
            result = ai.lookup_product_online(
                "063848966068",
                max_workers=2,
                wait_for_cleanup=True,
                require_image=True,
                background=True,
            )

        self.assertIsNone(result)
        self.assertEqual(lookup.call_count, 2)
        self.assertLessEqual(len(lookup.call_args_list[0].args[0]), 7)
        self.assertEqual(len(lookup.call_args_list[1].args[0]), 1)

    def test_lookup_sources_have_one_process_wide_concurrency_limit(self):
        active = 0
        peak = 0
        lock = threading.Lock()

        def source_task():
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.03)
            with lock:
                active -= 1
            return None

        shared = ThreadPoolExecutor(max_workers=3)
        try:
            with patch.object(ai, "_LOOKUP_SOURCE_EXECUTOR", shared), \
                 patch.object(ai, "_LOOKUP_SOURCE_WORKERS", 3):
                callers = ThreadPoolExecutor(max_workers=4)
                try:
                    futures = [
                        callers.submit(
                            ai.best_lookup_result, [source_task] * 8, 3, None, True
                        )
                        for _ in range(4)
                    ]
                    for future in futures:
                        future.result(timeout=5)
                finally:
                    callers.shutdown(wait=True)
        finally:
            shared.shutdown(wait=True)

        self.assertLessEqual(peak, 3)

    def test_background_cleanup_waits_for_started_lookup_sources(self):
        slow_started = threading.Event()
        slow_finished = threading.Event()

        def good_result():
            self.assertTrue(slow_started.wait(1))
            return {"name": "Known product", "brand": "Known", "source": "Familiprix"}

        def slow_result():
            slow_started.set()
            time.sleep(0.05)
            slow_finished.set()
            return None

        result, _ = ai.best_lookup_result(
            [good_result, slow_result], max_workers=2, good_enough=1,
            wait_for_cleanup=True,
        )
        self.assertIsNotNone(result)
        self.assertTrue(slow_finished.is_set())

    def test_planogram_waiter_gets_priority_over_new_background_work(self):
        first_started = threading.Event()
        release_first = threading.Event()
        planogram_waiting = threading.Event()
        order = []

        def first_background():
            with memory_intensive_task("first_background"):
                first_started.set()
                release_first.wait(2)

        def planogram():
            planogram_waiting.set()
            with memory_intensive_task("planogram", priority=True):
                order.append("planogram")

        def second_background():
            with memory_intensive_task("second_background"):
                order.append("background")

        t1 = threading.Thread(target=first_background)
        tp = threading.Thread(target=planogram)
        t2 = threading.Thread(target=second_background)
        t1.start()
        self.assertTrue(first_started.wait(1))
        tp.start()
        self.assertTrue(planogram_waiting.wait(1))
        deadline = time.time() + 1
        while memory_snapshot()["planogram_waiters"] < 1 and time.time() < deadline:
            time.sleep(0.005)
        self.assertEqual(memory_snapshot()["planogram_waiters"], 1)
        t2.start()
        release_first.set()
        for thread in (t1, tp, t2):
            thread.join(2)

        self.assertEqual(order, ["planogram", "background"])


if __name__ == "__main__":
    unittest.main()
