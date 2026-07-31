import unittest
from unittest.mock import patch

import observability


class ObservabilityTests(unittest.TestCase):
    def setUp(self):
        with observability._LOCK:
            observability._REQUEST_SAMPLES.clear()
            observability._AI_SAMPLES.clear()
            observability._LAST_WARNING_LOG_AT = 0.0

    def test_metrics_are_bounded_and_report_percentiles(self):
        for index in range(80):
            observability.record_request(
                "GET", f"/api/test/{index}", 500 if index == 79 else 200,
                index + 1,
            )
        for index in range(300):
            observability.record_ai_answer(
                "documented", index + 1,
                degraded=index % 10 == 0,
                cache_hit=index % 8 == 0,
                model="kimi-test",
            )
        with patch.object(
            observability, "memory_snapshot",
            return_value={"rss_mb": 120.0, "active_task": None},
        ):
            snapshot = observability.observability_snapshot()

        self.assertLessEqual(
            len(snapshot["routes"]), observability._MAX_ROUTE_SERIES
        )
        self.assertLessEqual(
            snapshot["ai"]["documented"]["samples"],
            observability._AI_SAMPLES.maxlen,
        )
        self.assertGreater(snapshot["ai"]["documented"]["p95_ms"], 0)
        self.assertEqual(snapshot["status"], "healthy")

    def test_memory_pressure_is_reported_without_unbounded_history(self):
        with patch.object(
            observability, "memory_snapshot",
            return_value={"rss_mb": 410.0, "active_task": "catalogue"},
        ):
            snapshot = observability.observability_snapshot()

        self.assertEqual(snapshot["status"], "critical")
        self.assertIn("memory_near_instance_limit", snapshot["warnings"])


if __name__ == "__main__":
    unittest.main()
