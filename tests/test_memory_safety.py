import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from memory_guard import memory_intensive_task, memory_snapshot
from routes import ai


class MemorySafetyTests(unittest.TestCase):
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
