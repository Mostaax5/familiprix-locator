import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class PerformanceContractTests(unittest.TestCase):
    def test_render_deploy_waits_for_search_readiness(self):
        render_config = (ROOT / "render.yaml").read_text(encoding="utf-8")
        app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        self.assertIn("healthCheckPath: /readyz", render_config)
        self.assertIn('@app.route("/healthz")', app_source)
        self.assertIn('@app.route("/readyz")', app_source)
        ready_start = app_source.index("def readyz")
        ready_route = app_source[ready_start:ready_start + 450]
        self.assertIn("product_search_cache_ready()", ready_route)
        self.assertNotIn("get_db()", ready_route)

    def test_render_runtime_and_port_are_explicit(self):
        render_config = (ROOT / "render.yaml").read_text(encoding="utf-8")
        gunicorn_config = (ROOT / "gunicorn.conf.py").read_text(encoding="utf-8")
        python_version = (ROOT / ".python-version").read_text(encoding="ascii").strip()
        self.assertEqual("3.13.7", python_version)
        self.assertIn("autoDeployTrigger: commit", render_config)
        self.assertIn("0.0.0.0", gunicorn_config)
        self.assertIn("os.environ.get('PORT', '10000')", gunicorn_config)
        self.assertIn("threads = 4", gunicorn_config)
        self.assertIn("max_requests = 1000", gunicorn_config)
        self.assertIn("MALLOC_ARENA_MAX", render_config)
        self.assertIn("value: kimi-k3", render_config)
        documented_effort = render_config.index(
            "- key: KIMI_SYNC_DOCUMENTED_REASONING_EFFORT"
        )
        self.assertIn(
            "value: low",
            render_config[documented_effort:documented_effort + 100],
        )

    def test_ready_catalogue_releases_a_stale_background_boot_gate(self):
        import app as app_module

        old_pending = app_module.DB_BOOT_PENDING
        old_error = app_module.DB_BOOT_ERROR
        old_config_pending = app_module.app.config.get("DB_BOOT_PENDING")
        try:
            app_module.DB_BOOT_PENDING = True
            app_module.DB_BOOT_ERROR = "stale maintenance wait"
            app_module.app.config["DB_BOOT_PENDING"] = True

            app_module._mark_database_ready()

            self.assertFalse(app_module.DB_BOOT_PENDING)
            self.assertFalse(app_module.app.config["DB_BOOT_PENDING"])
            self.assertEqual(app_module.DB_BOOT_ERROR, "")
        finally:
            app_module.DB_BOOT_PENDING = old_pending
            app_module.DB_BOOT_ERROR = old_error
            app_module.app.config["DB_BOOT_PENDING"] = old_config_pending

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

    def test_planogram_import_defers_metadata_and_quality_work(self):
        source = (ROOT / "routes" / "products.py").read_text(encoding="utf-8")
        start = source.index("def bulk_import_products():")
        end = source.index("_QUALITY_AUDIT_LOCK", start)
        import_body = source[start:end]
        commit = import_body.index("db.commit()")
        queue = import_body.index("schedule_planogram_post_import(")

        self.assertLess(commit, queue)
        self.assertNotIn("_record_import_identifiers(", import_body)
        self.assertNotIn("audit_product_data(", import_body)
        self.assertNotIn("rows_to_verified_products(", import_body)
        self.assertIn("db.executemany(insert_sql", import_body)

    def test_explicit_identifier_search_revalidates_the_phone_cache(self):
        source = (ROOT / "static" / "search.js").read_text(encoding="utf-8")
        start = source.index("async function doSearchValue")
        end = source.index("if (looksLikeCompleteRetailBarcode", start)
        field_branch = source[start:end]
        self.assertIn("searchProductsByFieldFromCache", field_branch)
        self.assertIn("await apiSearchProducts(q, field)", field_branch)
        self.assertIn("mergeIndexedSearchResults", field_branch)
        self.assertNotIn(
            "cachedByField.length || allProductsCache.length", field_branch
        )
        self.assertIn("await apiSearchProducts(q, 'identifier')", source)
        broad_start = source.index("const cached = searchProductsFromCache", end)
        broad_end = source.index("// Fetch catalogue-only products", broad_start)
        broad_branch = source[broad_start:broad_end]
        self.assertIn("const indexed = await apiSearchProducts(q)", broad_branch)
        self.assertIn("mergeIndexedSearchResults(indexed, cached, 40)", broad_branch)
        self.assertNotIn("cached.length || allProductsCache.length", broad_branch)

    def test_background_image_work_cannot_block_the_web_memory_gate(self):
        source = (ROOT / "routes" / "products.py").read_text(encoding="utf-8")
        start = source.index("def schedule_image_fill")
        end = source.index("def hydrate_candidate_images", start)
        worker = source[start:end]
        self.assertIn("background=True", worker)
        self.assertNotIn('memory_intensive_task("product_image")', worker)
        self.assertIn("_IMAGE_FILL_MAX_PENDING = 24", source)
        self.assertIn("ORDER BY newest DESC LIMIT 12", source)

    def test_planogram_reader_uses_fast_validated_path_and_quick_polling(self):
        routes = (ROOT / "routes" / "import_export.py").read_text(encoding="utf-8")
        layout_ui = (ROOT / "static" / "layout-ui.js").read_text(encoding="utf-8")
        requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertIn("pypdfium2==", requirements)
        self.assertIn("def _fast_planogram_is_trustworthy", routes)
        self.assertIn("_parse_planogram_pdf_compatibility", routes)
        self.assertIn('return products, metadata, "pdfium-fast"', routes)
        self.assertIn("let pollDelay = 150", layout_ui)
        self.assertNotIn("setTimeout(r, 2500)", layout_ui)

    def test_product_media_is_cached_and_loaded_without_request_flooding(self):
        product_routes = (ROOT / "routes" / "products.py").read_text(encoding="utf-8")
        layout_ui = (ROOT / "static" / "layout-ui.js").read_text(encoding="utf-8")
        search_ui = (ROOT / "static" / "search.js").read_text(encoding="utf-8")
        template = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

        self.assertIn('_PRODUCTS_PAYLOAD_VERSION = "compact-stream-v4"', product_routes)
        etag_start = product_routes.index("etag = hashlib.sha256")
        self.assertIn("_PRODUCTS_PAYLOAD_VERSION", product_routes[etag_start:etag_start + 300])
        self.assertIn("@_serialized_product_corpus", product_routes)
        self.assertIn("stream_with_context(generate())", product_routes)
        self.assertIn("bootstrap_product_payload(item)", product_routes)
        self.assertIn("_PRODUCT_STREAM_CHUNK_BYTES = 256 * 1024", product_routes)
        self.assertIn("_PRODUCT_STREAM_LOCK.acquire(blocking=False)", product_routes)
        self.assertIn("response.call_on_close(close_product_stream)", product_routes)
        app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        warmup_start = app_source.index("def _start_self_keepalive")
        warmup_end = app_source.index(
            "def _finish_persistence_boot", warmup_start
        )
        warmup = app_source[warmup_start:warmup_end]
        self.assertNotIn('"/api/products"', warmup)
        self.assertIn("for _attempt in range(12)", warmup)
        self.assertIn('catalogue_schema.get("ready")', warmup)
        self.assertIn('rel="preconnect" href="https://magasiner.familiprix.com"', template)
        self.assertIn("scheduleRenderedProductImageHydration();", layout_ui)
        self.assertIn('loading="${imagePriority ? \'eager\' : \'lazy\'}"', layout_ui)
        self.assertIn("productCard(g[0], true, true, index < 3)", search_ui)

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

    def test_ai_training_log_never_writes_on_the_customer_request(self):
        source = (ROOT / "routes" / "ai.py").read_text(encoding="utf-8")
        start = source.index("def log_ai_interaction")
        end = source.index("PHARMACY_LOOKUP_SOURCES", start)
        logger = source[start:end]

        self.assertIn("_AI_LOG_EXECUTOR.submit(_persist_ai_log, values)", logger)
        self.assertIn("_AI_LOG_SLOTS.acquire(blocking=False)", logger)
        self.assertNotIn("db.execute(", logger)
        self.assertNotIn("db.commit()", logger)


if __name__ == "__main__":
    unittest.main()
