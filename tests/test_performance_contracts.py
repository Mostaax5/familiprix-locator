import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class PerformanceContractTests(unittest.TestCase):
    def test_render_health_check_does_not_wait_for_database(self):
        render_config = (ROOT / "render.yaml").read_text(encoding="utf-8")
        app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        self.assertIn("healthCheckPath: /healthz", render_config)
        self.assertIn('@app.route("/healthz")', app_source)

    def test_render_runtime_and_port_are_explicit(self):
        render_config = (ROOT / "render.yaml").read_text(encoding="utf-8")
        gunicorn_config = (ROOT / "gunicorn.conf.py").read_text(encoding="utf-8")
        python_version = (ROOT / ".python-version").read_text(encoding="ascii").strip()
        self.assertEqual("3.13.7", python_version)
        self.assertIn("autoDeployTrigger: commit", render_config)
        self.assertIn("0.0.0.0", gunicorn_config)
        self.assertIn("os.environ.get('PORT', '10000')", gunicorn_config)

    def test_planogram_replacement_is_enabled_by_default(self):
        source = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="planoReplace" checked', source)

    def test_startup_restores_local_plan_before_network_wait(self):
        source = (ROOT / "static" / "main.js").read_text(encoding="utf-8")
        app_load = source.index("async function _loadAppData")
        restore = source.index("restorePlanSnapshot()", app_load)
        network_wait = source.index("await Promise.allSettled", app_load)
        self.assertLess(restore, network_wait)
        boot = source.index("async function bootApp()")
        auth_check = source.index("await initializeAuth()", boot)
        public_load = source.index("_appLoadPromise = _loadAppData()", auth_check)
        self.assertLess(auth_check, public_load)
        self.assertNotIn("if (!authenticated) return", source[boot:public_load])

        resume = source.index("async function resumeAuthenticatedApp")
        load_branch = source.index("if (_appLoadPromise)", resume)
        branch_end = source.index("return true;", load_branch)
        branch = source[load_branch:branch_end]
        self.assertIn("await switchTab(preferredTab)", branch)
        self.assertNotIn("await _appLoadPromise", branch)

    def test_planogram_import_paints_committed_delta_before_revalidation(self):
        source = (ROOT / "static" / "layout-ui.js").read_text(encoding="utf-8")
        start = source.index("async function importPlanogram()")
        end = source.index("async function loadPlanogramHistory", start)
        import_body = source[start:end]
        apply_delta = import_body.index("applyPlanogramImportResult(aisle, side, data)")
        background_refresh = import_body.index("void Promise.allSettled")
        self.assertLess(apply_delta, background_refresh)
        self.assertNotIn("await refreshProductsCache(true)", import_body)

    def test_public_tabs_and_planogram_history_are_compact_by_default(self):
        source = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertIn("const LOCKED_TABS = new Set(['scan', 'add'])", (
            ROOT / "static" / "config.js"
        ).read_text(encoding="utf-8"))
        self.assertIn('id="tabBtn-search" onclick="switchTab(\'search\')">Recherche</button>', source)
        self.assertIn('id="tabBtn-client" onclick="switchTab(\'client\')">Client</button>', source)
        self.assertIn('<details class="card plano-history-card" id="planoHistoryPanel"', source)
        self.assertNotIn('<details class="card plano-history-card" id="planoHistoryPanel" open', source)
        self.assertLess(source.index('id="planoHistoryPanel"'), source.index('Catalogue — tous les planogrammes'))
        self.assertNotIn('id="storePassword"', source)
        self.assertIn("STORES.length === 1", (
            ROOT / "static" / "store.js"
        ).read_text(encoding="utf-8"))

    def test_client_fast_mode_is_independent_from_ai_answer(self):
        source = (ROOT / "static" / "ai-ui.js").read_text(encoding="utf-8")
        self.assertIn("const CLIENT_FAST_PRODUCT_LIMIT = 100", source)
        start = source.index("async function runClientRequest")
        fast_branch = source.index("if (mode === 'fast')", start)
        local_matches = source.index("localClientMatches(retrievalQuestion", fast_branch)
        fast_lookup = source.index("const serverPromise = apiClientFind(", fast_branch)
        ai_wait = source.index("await apiGenerateClientHelp", start)
        fast_return = source.index("return;", fast_lookup)
        self.assertLess(fast_branch, ai_wait)
        self.assertLess(local_matches, ai_wait)
        self.assertLess(fast_lookup, ai_wait)
        self.assertLess(fast_return, ai_wait)
        self.assertIn("mode,", source[ai_wait - 500:ai_wait + 500])
        self.assertIn("mode === 'documented'", source[ai_wait - 800:ai_wait + 200])


if __name__ == "__main__":
    unittest.main()
