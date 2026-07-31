import gzip
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from flask import Flask

from memory_guard import memory_intensive_task, memory_snapshot
from routes import ai, products


class MemorySafetyTests(unittest.TestCase):
    def test_boot_warmup_builds_product_corpus_once(self):
        original_cache = dict(products._PROD_CACHE)
        try:
            products._PROD_CACHE.update(rows=[], generation=-1)
            fake_db = MagicMock()
            with patch.object(products, "connect_db", return_value=fake_db), \
                 patch.object(
                     products, "_product_corpus_fast_ready", return_value=False,
                 ), patch.object(
                     products, "_products_corpus", return_value=[({}, {})],
                 ) as build, patch.object(
                     products, "memory_intensive_task",
                 ) as memory_task, patch.object(
                     products, "release_unused_memory",
                 ):
                memory_task.return_value.__enter__.return_value = None
                memory_task.return_value.__exit__.return_value = False
                self.assertEqual(products.warm_product_search_cache(), 1)

            build.assert_called_once_with(
                fake_db, allow_identifier_stale=True,
            )
            fake_db.close.assert_called_once_with()
        finally:
            products._PROD_CACHE.clear()
            products._PROD_CACHE.update(original_cache)

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

    def test_catalogue_metadata_invalidation_keeps_warm_search_available(self):
        original_cache = dict(products._PROD_CACHE)
        try:
            products._PROD_CACHE.update(
                rows=[({"id": 1}, {"_name": "advil"})],
                generation=12,
                metadata_dirty=False,
                metadata_dirty_at=0.0,
            )

            products.invalidate_product_search_cache()

            self.assertEqual(products._PROD_CACHE["generation"], 12)
            self.assertTrue(products._PROD_CACHE["metadata_dirty"])
            self.assertGreater(products._PROD_CACHE["metadata_dirty_at"], 0)
        finally:
            products._PROD_CACHE.clear()
            products._PROD_CACHE.update(original_cache)

    def test_old_metadata_refresh_is_deferred_when_worker_memory_is_high(self):
        original_cache = dict(products._PROD_CACHE)
        warm_rows = [({"id": 1, "name": "Advil"}, {"_name": "advil"})]
        try:
            products._PROD_CACHE.update(
                rows=warm_rows,
                generation=12,
                built_at=(
                    time.time() - products._PROD_IDENTIFIER_DRIFT_TTL_S - 1
                ),
                metadata_dirty=True,
                metadata_dirty_at=time.time() - 20,
            )
            with patch.object(
                products, "product_search_generation", return_value=12,
            ), patch.object(
                products, "current_rss_mb",
                return_value=products._PROD_METADATA_REFRESH_MAX_RSS_MB + 1,
            ), patch.object(
                products, "_schedule_product_corpus_refresh",
            ) as refresh:
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
                products, "product_payload_cache_ready", return_value=False,
            ):
                response = products.get_products()
        finally:
            products._PRODUCT_STREAM_LOCK.release()

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["Retry-After"], "1")
        self.assertTrue(response.get_json()["retry"])

    def test_product_bootstrap_is_built_once_then_served_from_disk(self):
        app = Flask(__name__)
        original_cache = dict(products._PROD_CACHE)
        original_payload = dict(products._PRODUCT_PAYLOAD_STATE)
        generated_paths = []
        item = {
            "id": 1, "name": "Advil", "barcode": "1", "product_code": "",
            "aisle": "1", "side": "A", "section": "1", "shelf": "1",
            "position": "1", "facings": 1, "is_plano": 1, "in_stock": 1,
            "linked_position": "", "flipped_label": 0,
        }
        corpus = [(item, {"_name": "advil"})]
        fake_db = MagicMock()
        self.assertFalse(products._PRODUCT_STREAM_LOCK.locked())
        try:
            products._PROD_CACHE.update(
                rows=corpus, generation=99, built_at=123.0, initialized=True,
            )
            products._PRODUCT_PAYLOAD_STATE.update(
                key=None, raw_path="", gzip_path="", etag="",
            )
            with patch.object(
                products, "connect_db", return_value=fake_db,
            ) as connect, patch.object(
                products, "_employee_product_corpus", return_value=corpus,
            ), patch.object(products, "release_unused_memory"):
                status = products.warm_product_payload_cache()
                generated_paths = [
                    products._PRODUCT_PAYLOAD_STATE["raw_path"],
                    products._PRODUCT_PAYLOAD_STATE["gzip_path"],
                ]
                self.assertEqual(status["rows"], 1)

                with app.test_request_context(
                    "/api/products", headers={"Accept-Encoding": "gzip"},
                ):
                    first_response = products.get_products()
                    first_response.direct_passthrough = False
                    first_body = first_response.get_data()
                    first_response.close()

                with patch.object(
                    products.json, "dumps",
                    side_effect=AssertionError("must not serialize again"),
                ), app.test_request_context(
                    "/api/products", headers={"Accept-Encoding": "gzip"},
                ):
                    second_response = products.get_products()
                    second_response.direct_passthrough = False
                    second_body = second_response.get_data()
                    second_response.close()

            self.assertIn(b'"name":"Advil"', gzip.decompress(first_body))
            self.assertEqual(first_body, second_body)
            connect.assert_called_once_with()
            fake_db.close.assert_called_once_with()
            self.assertFalse(products._PRODUCT_STREAM_LOCK.locked())
        finally:
            products._PROD_CACHE.clear()
            products._PROD_CACHE.update(original_cache)
            products._PRODUCT_PAYLOAD_STATE.clear()
            products._PRODUCT_PAYLOAD_STATE.update(original_payload)
            old_paths = {
                original_payload.get("raw_path"),
                original_payload.get("gzip_path"),
            }
            for path in generated_paths:
                if path and path not in old_paths:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

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

    def test_memory_pressure_guard_releases_only_rebuildable_state(self):
        original_reference = dict(products._REF_CACHE)
        original_check = products._MEMORY_PRESSURE_LAST_CHECK
        warm_products = products._PROD_CACHE.get("rows")
        try:
            products._REF_CACHE.update(
                key=("catalogue",), rows=[{"barcode": "1"}],
                built_at=time.time(),
            )
            products._MEMORY_PRESSURE_LAST_CHECK = 0.0
            with patch.object(
                products, "current_rss_mb", side_effect=[350.0, 245.0],
            ), patch.object(products, "release_unused_memory"):
                result = products.release_optional_product_caches_if_needed()

            self.assertEqual(result["reference_rows"], 1)
            self.assertEqual(result["rss_before_mb"], 350.0)
            self.assertEqual(products._REF_CACHE["rows"], [])
            self.assertIs(products._PROD_CACHE.get("rows"), warm_products)
        finally:
            products._REF_CACHE.clear()
            products._REF_CACHE.update(original_reference)
            products._MEMORY_PRESSURE_LAST_CHECK = original_check

    def test_reference_index_drops_full_metadata_but_keeps_it_searchable(self):
        original_reference = dict(products._REF_CACHE)

        class ReferenceDatabase:
            def execute(self, query, _params=()):
                if "FROM product_reference_evidence" in query:
                    return []
                if "FROM product_reference_identifiers" in query:
                    return [{
                        "gtin_key": "gtin:00062600142320",
                        "identifier_type": "DIN",
                        "identifier_value": "00559407",
                        "verification_status": "requires_review",
                    }]
                if "FROM product_reference" in query:
                    return [{
                        "barcode": "062600142320",
                        "name": "TYLENOL 500MG CO100",
                        "brand": "Tylenol",
                        "description": "Acetaminophene pour le soulagement de la douleur.",
                        "product_code": "664375",
                        "store_presence_status": "planogram_imported",
                        "source": "Familiprix",
                        "source_url": "https://example.test/product",
                    }]
                raise AssertionError(query)

        try:
            products._REF_CACHE.update(key=None, rows=[], built_at=0.0)
            with patch.object(
                products, "_reference_state_key", return_value=(1,),
            ), patch.object(products, "release_unused_memory"):
                rows = products._reference_corpus(ReferenceDatabase())

            self.assertEqual(len(rows), 1)
            self.assertNotIn("description", rows[0])
            self.assertIn("acetaminophene", rows[0]["_hay"])
            self.assertNotIn("source", rows[0]["_identifiers"][0])
            self.assertEqual(rows[0]["_identifiers"][0]["value"], "00559407")
        finally:
            products._REF_CACHE.clear()
            products._REF_CACHE.update(original_reference)

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
