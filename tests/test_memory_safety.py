import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from memory_guard import memory_intensive_task, memory_snapshot
from routes import ai, products


class MemorySafetyTests(unittest.TestCase):
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
