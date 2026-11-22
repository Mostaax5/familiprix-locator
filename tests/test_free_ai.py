import json
import os
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import URLError

from app import app
import free_ai
from routes import ai
from semantic_search import _embed_texts


class FreeAiTests(unittest.TestCase):
    def test_existing_paid_keys_cannot_enable_inference(self):
        with patch.dict(os.environ, {}, clear=True), patch.multiple(ai, KIMI_API_KEY="old-key", OPENAI_API_KEY="old-key", GEMINI_API_KEY="old-key"):
            self.assertEqual(ai.configured_ai_provider()["name"], "local")
            with self.assertRaises(URLError):
                ai._safe_urlopen(ai.KIMI_BASE_URL + "/chat/completions", timeout=1)
            with self.assertRaisesRegex(RuntimeError, "disabled"):
                _embed_texts(["test"], "RETRIEVAL_QUERY", 1)

    def test_cloudflare_requires_explicit_free_account_configuration(self):
        with patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_AI_TOKEN": "test"}, clear=True):
            self.assertFalse(free_ai.cloudflare_configured())
            with patch.dict(os.environ, {"CLOUDFLARE_WORKERS_FREE_CONFIRMED": "1"}):
                self.assertTrue(free_ai.cloudflare_configured())
                self.assertEqual(ai.configured_ai_provider()["name"], "cloudflare")

    def test_quota_error_never_retries_or_falls_through_to_paid_model(self):
        with patch("free_ai.cloudflare_configured", return_value=True), \
             patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_AI_TOKEN": "test"}), \
             patch("free_ai.build_opener") as opener:
            opener.return_value.open.side_effect = URLError("quota exhausted")
            self.assertIsNone(free_ai.structured_request("system", {}, 600, {}))
            self.assertEqual(opener.return_value.open.call_count, 1)

    def test_cloudflare_output_is_bounded_and_parsed(self):
        with patch("free_ai.cloudflare_configured", return_value=True), \
             patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "a" * 32, "CLOUDFLARE_AI_TOKEN": "test"}), \
             patch("free_ai.build_opener") as opener:
            response = opener.return_value.open.return_value.__enter__.return_value
            response.read.return_value = json.dumps({"success": True, "result": {"response": '{"answer":"ok"}'}}).encode()
            self.assertEqual(free_ai.structured_request("system", {}, 9000, {}), {"answer": "ok"})
            request = opener.return_value.open.call_args.args[0]
            self.assertEqual(json.loads(request.data)["max_tokens"], 900)
            response.read.return_value = b"x" * (free_ai.MAX_BODY + 1)
            self.assertIsNone(free_ai.structured_request("system", {}, 600, {}))

    def test_no_key_still_returns_a_catalogue_answer(self):
        app.config.update(TESTING=True, AUTH_TEST_BYPASS=True)
        product = {"id": 1, "client_id": "product:1", "name": "Acme dentifrice", "barcode": "063848966068",
                   "aisle": "1", "side": "Gauche", "section": "1", "shelf": "1", "position": "1"}
        with patch.dict(os.environ, {}, clear=True), \
             patch("routes.products.hybrid_client_candidates", return_value=[product]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.log_ai_interaction"), \
             patch("routes.ai._provider_structured_request") as remote:
            result = app.test_client().post("/api/client/help", json={"question": "Décris le dentifrice Acme", "mode": "ai"})
        self.assertEqual(result.status_code, 200, result.get_json())
        self.assertEqual(result.json["answer_engine"], "catalogue")
        self.assertFalse(result.json["degraded"])
        self.assertFalse(result.json.get("ai_pending", False))
        # No provider call should be needed for an ordinary local-mode question.
        remote.assert_not_called()
