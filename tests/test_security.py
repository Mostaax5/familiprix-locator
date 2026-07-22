import hashlib
import sqlite3
import time
import unittest
from unittest.mock import patch
from urllib.request import Request

from werkzeug.security import generate_password_hash

from app import app
from database import DatabaseConnection, init_sqlite_db
import security
from routes.layout import normalize_layout_config, valid_aisle_name
from routes.gist import _github_urlopen, _normalized_backup_product


class SecurityBoundaryTests(unittest.TestCase):
    password = "A unique employee passphrase 2026"

    def setUp(self):
        self.old_testing = app.testing
        self.old_bypass = app.config.get("AUTH_TEST_BYPASS")
        self.old_secure = app.config.get("AUTH_COOKIE_SECURE")
        app.testing = True
        app.config["AUTH_TEST_BYPASS"] = False
        app.config["AUTH_COOKIE_SECURE"] = True

        connection = sqlite3.connect(":memory:", check_same_thread=False)
        connection.row_factory = sqlite3.Row
        self.db = DatabaseConnection(connection, "sqlite")
        init_sqlite_db(self.db)
        self.db.commit()

        self.db_patcher = patch("security.get_db", return_value=self.db)
        self.db_patcher.start()
        password_hash = generate_password_hash(
            self.password, method="scrypt:32768:8:1", salt_length=32
        )
        self.password_patcher = patch(
            "security._password_record",
            return_value=(password_hash, False, "environment"),
        )
        self.password_patcher.start()
        with security._rate_lock:
            security._login_failures.clear()
            security._sensitive_requests.clear()
        self.client = app.test_client()

    def tearDown(self):
        self.password_patcher.stop()
        self.db_patcher.stop()
        self.db.close()
        app.testing = self.old_testing
        if self.old_bypass is None:
            app.config.pop("AUTH_TEST_BYPASS", None)
        else:
            app.config["AUTH_TEST_BYPASS"] = self.old_bypass
        app.config["AUTH_COOKIE_SECURE"] = self.old_secure

    def login(self):
        response = self.client.post(
            "/api/auth/login",
            json={"password": self.password, "username": "Security Tester"},
            headers={"Origin": "https://localhost"},
            base_url="https://localhost",
        )
        self.assertEqual(response.status_code, 200)
        return response, response.get_json()

    def test_forged_browser_identity_cannot_read_private_api(self):
        for path in (
            "/api/export",
            "/api/products/removed",
            "/api/planograms/history",
            "/api/gist/status",
            "/api/ai/logs/count",
            "/api/not-a-real-route",
        ):
            with self.subTest(path=path):
                response = self.client.get(
                    path,
                    headers={"X-User-Name": "forged-admin"},
                    base_url="https://localhost",
                )
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.get_json()["code"], "authentication_required")
                self.assertEqual(response.headers.get("Clear-Site-Data"), '"cache"')

    def test_search_and_client_help_are_public_but_cross_site_posts_are_rejected(self):
        search = self.client.get(
            "/api/products/search?q=",
            base_url="https://localhost",
        )
        self.assertEqual(search.status_code, 200)
        self.assertEqual(search.get_json(), [])

        client_find = self.client.get(
            "/api/client/find?q=",
            base_url="https://localhost",
        )
        self.assertEqual(client_find.status_code, 200)
        self.assertEqual(client_find.get_json(), [])

        with patch("routes.products.get_db", return_value=self.db):
            products = self.client.get("/api/products", base_url="https://localhost")
        self.assertEqual(products.status_code, 200)

        with patch("routes.layout.get_db", return_value=self.db):
            layouts = self.client.get("/api/layout/aisles", base_url="https://localhost")
        self.assertEqual(layouts.status_code, 200)

        same_origin = self.client.post(
            "/api/client/help",
            json={},
            headers={"Origin": "https://localhost"},
            base_url="https://localhost",
        )
        self.assertEqual(same_origin.status_code, 400)
        self.assertNotEqual(same_origin.get_json().get("code"), "authentication_required")

        cross_site = self.client.post(
            "/api/client/help",
            json={"question": "test"},
            headers={"Origin": "https://attacker.example"},
            base_url="https://localhost",
        )
        self.assertEqual(cross_site.status_code, 403)
        self.assertEqual(cross_site.get_json()["code"], "origin_rejected")

    def test_login_rejects_cross_site_and_non_object_json(self):
        cross_site = self.client.post(
            "/api/auth/login",
            json={"password": self.password},
            headers={"Origin": "https://attacker.example"},
            base_url="https://localhost",
        )
        self.assertEqual(cross_site.status_code, 403)
        self.assertEqual(cross_site.get_json()["code"], "origin_rejected")

        malformed = self.client.post(
            "/api/auth/login",
            data="[]",
            content_type="application/json",
            headers={"Origin": "https://localhost"},
            base_url="https://localhost",
        )
        self.assertEqual(malformed.status_code, 400)
        self.assertEqual(malformed.get_json()["code"], "invalid_json")

        oversized = self.client.post(
            "/api/auth/login",
            data='{"password":"' + ("x" * 5000) + '"}',
            content_type="application/json",
            headers={"Origin": "https://localhost"},
            base_url="https://localhost",
        )
        self.assertEqual(oversized.status_code, 413)
        self.assertEqual(oversized.get_json()["code"], "request_too_large")

    def test_login_uses_hardened_cookie_and_csrf_for_writes(self):
        response, payload = self.login()
        cookie = response.headers.get("Set-Cookie", "")
        self.assertIn("__Host-familiprix_session=", cookie)
        self.assertIn("Secure", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)
        self.assertNotIn(payload["csrf_token"], cookie)

        missing = self.client.post(
            "/api/auth/profile",
            json={"username": "No CSRF"},
            headers={"Origin": "https://localhost"},
            base_url="https://localhost",
        )
        self.assertEqual(missing.status_code, 403)
        self.assertEqual(missing.get_json()["code"], "csrf_rejected")

        wrong_origin = self.client.post(
            "/api/auth/profile",
            json={"username": "Wrong Origin"},
            headers={
                "Origin": "https://attacker.example",
                "X-CSRF-Token": payload["csrf_token"],
            },
            base_url="https://localhost",
        )
        self.assertEqual(wrong_origin.status_code, 403)
        self.assertEqual(wrong_origin.get_json()["code"], "origin_rejected")

        lookalike_referer = self.client.post(
            "/api/auth/profile",
            json={"username": "Wrong Referer"},
            headers={
                "Referer": "https://localhost.attacker.example/profile",
                "X-CSRF-Token": payload["csrf_token"],
            },
            base_url="https://localhost",
        )
        self.assertEqual(lookalike_referer.status_code, 403)
        self.assertEqual(lookalike_referer.get_json()["code"], "origin_rejected")

        accepted = self.client.post(
            "/api/auth/profile",
            json={"username": "Verified Employee"},
            headers={
                "Origin": "https://localhost",
                "X-CSRF-Token": payload["csrf_token"],
            },
            base_url="https://localhost",
        )
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(accepted.get_json()["username"], "Verified Employee")

    def test_logout_revokes_the_database_session(self):
        _response, payload = self.login()
        logout = self.client.post(
            "/api/auth/logout",
            headers={
                "Origin": "https://localhost",
                "X-CSRF-Token": payload["csrf_token"],
            },
            base_url="https://localhost",
        )
        self.assertEqual(logout.status_code, 200)
        self.assertEqual(logout.headers.get("Clear-Site-Data"), '"cache"')
        status = self.client.get("/api/auth/status", base_url="https://localhost")
        self.assertFalse(status.get_json()["authenticated"])

    def test_password_secret_rotation_revokes_existing_sessions(self):
        self.login()
        self.password_patcher.stop()
        replacement_hash = generate_password_hash(
            "A different private employee passphrase 2026",
            method="scrypt:32768:8:1",
            salt_length=32,
        )
        self.password_patcher = patch(
            "security._password_record",
            return_value=(replacement_hash, False, "environment"),
        )
        self.password_patcher.start()

        protected = self.client.get("/api/export", base_url="https://localhost")
        self.assertEqual(protected.status_code, 401)
        self.assertEqual(protected.get_json()["code"], "credentials_changed")
        row = self.db.execute(
            "SELECT revoked_at FROM auth_sessions ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        self.assertGreater(int(row["revoked_at"]), 0)

    def test_inactive_session_expires_and_cookie_is_cleared(self):
        self.login()
        self.db.execute(
            "UPDATE auth_sessions SET last_seen=? WHERE revoked_at=0",
            (int(time.time()) - security.SESSION_IDLE_SECONDS - 1,),
        )
        self.db.commit()

        response = self.client.get("/api/export", base_url="https://localhost")
        self.assertEqual(response.status_code, 401)
        self.assertIn("__Host-familiprix_session=;", response.headers.get("Set-Cookie", ""))
        row = self.db.execute(
            "SELECT revoked_at FROM auth_sessions ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        self.assertGreater(int(row["revoked_at"]), 0)

    def test_temporary_protected_area_password_does_not_force_rotation(self):
        self.password_patcher.stop()
        replacement_hash = generate_password_hash(
            "A different configured passphrase 2026",
            method="scrypt:32768:8:1",
            salt_length=32,
        )
        self.password_patcher = patch(
            "security._password_record",
            return_value=(replacement_hash, False, "environment"),
        )
        self.password_patcher.start()
        temporary_digest = hashlib.sha256(self.password.encode("utf-8")).hexdigest()
        with patch.object(security, "_LEGACY_PASSWORD_SHA256", temporary_digest):
            response, payload = self.login()
            self.assertFalse(payload["rotation_required"])
            with patch("routes.import_export.get_db", return_value=self.db):
                protected = self.client.get("/api/export", base_url="https://localhost")
        self.assertEqual(protected.status_code, 200)

    def test_security_headers_are_present_without_leaking_error_details(self):
        response = self.client.get("/", base_url="https://localhost")
        self.assertEqual(response.headers.get("X-Frame-Options"), "DENY")
        self.assertEqual(response.headers.get("Referrer-Policy"), "no-referrer")
        self.assertIn("frame-ancestors 'none'", response.headers.get("Content-Security-Policy", ""))
        self.assertIn("camera=(self)", response.headers.get("Permissions-Policy", ""))

    def test_database_reset_requires_exact_confirmation_and_is_audited(self):
        self.db.execute(
            "INSERT INTO products (name, aisle, side, section, shelf, position) "
            "VALUES ('Protected product', '3', 'Gauche', '1', '1', '1')"
        )
        self.db.commit()
        _response, auth = self.login()
        headers = {
            "Origin": "https://localhost",
            "X-CSRF-Token": auth["csrf_token"],
        }
        with patch("routes.import_export.get_db", return_value=self.db):
            rejected = self.client.post(
                "/api/reset",
                json={"wipe_layouts": False, "confirmation": "yes"},
                headers=headers,
                base_url="https://localhost",
            )
            self.assertEqual(rejected.status_code, 400)
            self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 1)

            accepted = self.client.post(
                "/api/reset",
                json={"wipe_layouts": False, "confirmation": "SUPPRIMER LES PRODUITS"},
                headers=headers,
                base_url="https://localhost",
            )
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 0)
        event = self.db.execute(
            "SELECT action, detail_json FROM security_events WHERE action='database_reset'"
        ).fetchone()
        self.assertIsNotNone(event)
        self.assertIn('"deleted_products":1', event["detail_json"])

    def test_database_import_rejects_non_object_json(self):
        _response, auth = self.login()
        response = self.client.post(
            "/api/import",
            json=[],
            headers={
                "Origin": "https://localhost",
                "X-CSRF-Token": auth["csrf_token"],
            },
            base_url="https://localhost",
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Structure de sauvegarde invalide.")

    def test_imported_layout_complexity_and_aisle_names_are_bounded(self):
        config = normalize_layout_config({
            "sides": {
                "Gauche": {"sections": [
                    {"shelves": [999999] * 150, "labels": ["x" * 1000] * 150}
                ] * 250},
                "Droite": {"sections": []},
            },
            "presentoirs": [{"name": "p" * 500, "facades": []}] * 150,
        })
        self.assertEqual(len(config["sides"]["Gauche"]["sections"]), 200)
        self.assertEqual(len(config["sides"]["Gauche"]["sections"][0]["shelves"]), 100)
        self.assertEqual(config["sides"]["Gauche"]["sections"][0]["shelves"][0], 500)
        self.assertEqual(len(config["sides"]["Gauche"]["sections"][0]["labels"][0]), 160)
        self.assertEqual(len(config["presentoirs"]), 100)
        self.assertTrue(valid_aisle_name("Allee 3-A"))
        self.assertFalse(valid_aisle_name("<script>alert(1)</script>"))

    def test_backup_product_flags_and_urls_are_safely_normalized(self):
        product = _normalized_backup_product({
            "name": "Test product",
            "aisle": "3",
            "side": "Gauche",
            "section": "1",
            "shelf": "2",
            "position": "4",
            "is_plano": "1",
            "in_stock": "0",
            "flipped_label": "1",
            "facings": "50000",
            "image_url": "javascript:alert(1)",
        }, "2026-01-01T00:00:00Z")
        self.assertEqual(product["is_plano"], 1)
        self.assertEqual(product["in_stock"], 0)
        self.assertEqual(product["flipped_label"], 1)
        self.assertEqual(product["facings"], 1000)
        self.assertEqual(product["image_url"], "")

    def test_github_backup_client_rejects_unsafe_urls_before_network_access(self):
        for url in (
            "http://api.github.com/gists/example",
            "https://attacker.example/gists/example",
            "https://user:password@api.github.com/gists/example",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                _github_urlopen(Request(url), timeout=1)


if __name__ == "__main__":
    unittest.main()
