import unittest
from unittest.mock import patch

import app as app_module


class StartupRecoveryTests(unittest.TestCase):
    def test_catalogue_warmup_recovers_after_transient_failure(self):
        original_state = app_module.catalogue_warmup_status()
        ready = {"search": False, "payload": False, "reference": False}
        attempts = {"search": 0}

        def warm_search():
            attempts["search"] += 1
            if attempts["search"] == 1:
                raise RuntimeError("temporary database failure")
            ready["search"] = True
            return 5521

        def warm_payload():
            ready["payload"] = True
            return {"rows": 5521, "gzip_bytes": 1853096}

        def warm_reference():
            ready["reference"] = True
            return 9418

        try:
            with app_module._CATALOGUE_WARMUP_LOCK:
                app_module._CATALOGUE_WARMUP.update(
                    active=True, stage="starting", attempts=0,
                    last_error="", started_at=1.0, ready_at=0.0,
                )
            with patch.object(
                app_module, "product_search_cache_ready",
                side_effect=lambda: ready["search"],
            ), patch.object(
                app_module, "product_payload_cache_ready",
                side_effect=lambda: ready["payload"],
            ), patch.object(
                app_module, "reference_search_cache_ready",
                side_effect=lambda: ready["reference"],
            ), patch.object(
                app_module, "warm_product_search_cache",
                side_effect=warm_search,
            ), patch.object(
                app_module, "warm_product_payload_cache",
                side_effect=warm_payload,
            ), patch.object(
                app_module, "warm_reference_search_cache",
                side_effect=warm_reference,
            ), patch.object(app_module.time, "sleep") as sleep:
                app_module._catalogue_warmup_worker()

            status = app_module.catalogue_warmup_status()
            self.assertEqual(attempts["search"], 2)
            self.assertEqual(status["stage"], "ready")
            self.assertEqual(status["attempts"], 2)
            self.assertFalse(status["active"])
            self.assertEqual(status["last_error"], "")
            sleep.assert_called_once_with(2)
        finally:
            with app_module._CATALOGUE_WARMUP_LOCK:
                app_module._CATALOGUE_WARMUP.clear()
                app_module._CATALOGUE_WARMUP.update(original_state)


if __name__ == "__main__":
    unittest.main()
