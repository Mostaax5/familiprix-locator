import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class PerformanceContractTests(unittest.TestCase):
    def test_startup_restores_local_plan_before_network_wait(self):
        source = (ROOT / "static" / "main.js").read_text(encoding="utf-8")
        boot = source.index("async function bootApp()")
        restore = source.index("restorePlanSnapshot()", boot)
        network_wait = source.index("await Promise.allSettled", boot)
        self.assertLess(restore, network_wait)

    def test_planogram_import_paints_committed_delta_before_revalidation(self):
        source = (ROOT / "static" / "layout-ui.js").read_text(encoding="utf-8")
        start = source.index("async function importPlanogram()")
        end = source.index("async function loadPlanogramHistory()", start)
        import_body = source[start:end]
        apply_delta = import_body.index("applyPlanogramImportResult(aisle, side, data)")
        background_refresh = import_body.index("void Promise.allSettled")
        self.assertLess(apply_delta, background_refresh)
        self.assertNotIn("await refreshProductsCache(true)", import_body)

    def test_client_products_render_before_ai_answer_finishes(self):
        source = (ROOT / "static" / "ai-ui.js").read_text(encoding="utf-8")
        start = source.index("async function runClientRequest")
        local_matches = source.index("localClientMatches(question", start)
        fast_lookup = source.index("apiClientFind(question", start)
        ai_wait = source.index("await apiGenerateClientHelp", start)
        self.assertLess(local_matches, ai_wait)
        self.assertLess(fast_lookup, ai_wait)


if __name__ == "__main__":
    unittest.main()
