import sqlite3
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from flask import Flask

from product_data import gtin_identity_key
from routes.expiry import expiry_bp


class ExpiryTrackingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute("CREATE TABLE users (username TEXT PRIMARY KEY, last_seen TEXT)")
        self.db.execute(
            """CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT, brand TEXT, description TEXT, image_url TEXT,
                barcode TEXT, gtin_key TEXT, product_code TEXT,
                aisle TEXT, side TEXT, section TEXT, shelf TEXT, position TEXT,
                in_stock INTEGER DEFAULT 1, is_plano INTEGER DEFAULT 1
            )"""
        )
        self.db.execute(
            """CREATE TABLE product_reference (
                barcode TEXT PRIMARY KEY, gtin_key TEXT, name TEXT, brand TEXT,
                description TEXT, image_url TEXT, product_code TEXT
            )"""
        )
        self.db.execute(
            """CREATE TABLE product_expiry_status (
                store_key TEXT NOT NULL, gtin_key TEXT NOT NULL,
                barcode TEXT NOT NULL, product_name TEXT NOT NULL DEFAULT '',
                brand TEXT DEFAULT '', image_url TEXT DEFAULT '',
                product_code TEXT DEFAULT '', earliest_expiry_date TEXT NOT NULL,
                checked_at TEXT NOT NULL DEFAULT '', checked_by TEXT NOT NULL DEFAULT '',
                recorded_by TEXT NOT NULL DEFAULT '', note TEXT DEFAULT '',
                locations_json TEXT DEFAULT '[]', revision INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(store_key, gtin_key)
            )"""
        )
        self.db.execute(
            """CREATE TABLE product_expiry_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_key TEXT NOT NULL, gtin_key TEXT NOT NULL,
                barcode TEXT NOT NULL, action TEXT NOT NULL,
                previous_expiry_date TEXT DEFAULT '', expiry_date TEXT DEFAULT '',
                product_name TEXT DEFAULT '', initials TEXT DEFAULT '',
                recorded_by TEXT DEFAULT '', note TEXT DEFAULT '', created_at TEXT DEFAULT ''
            )"""
        )
        self.barcode = "063848907665"
        self.gtin_key = gtin_identity_key(self.barcode)
        self.db.executemany(
            """INSERT INTO products
               (name, brand, description, image_url, barcode, gtin_key,
                product_code, aisle, side, section, shelf, position, in_stock, is_plano)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,1)""",
            [
                (
                    "BIOMEDIC GEL ANALG GLACE 255G", "Biomedic", "Gel analgésique",
                    "https://example.com/gel.jpg", self.barcode, self.gtin_key,
                    "123456", "Labo", "Gauche", "2", "3", "4",
                ),
                (
                    "BIOMEDIC GEL ANALG GLACE 255G", "Biomedic", "Gel analgésique",
                    "https://example.com/gel.jpg", self.barcode, self.gtin_key,
                    "123456", "4", "Droite", "1", "2", "7",
                ),
            ],
        )
        self.db.commit()
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, AUTH_TEST_BYPASS=True)
        self.app.register_blueprint(expiry_bp)

    def tearDown(self):
        self.db.close()

    def request(self, method, path, payload=None):
        with patch("routes.expiry.get_db", return_value=self.db), \
             patch("auth.get_db", return_value=self.db):
            with self.app.test_client() as client:
                return client.open(path, method=method, json=payload)

    def create_expiry(self, expiry_date=None, revision=0, initials="AM"):
        return self.request(
            "POST",
            "/api/expiry",
            {
                "store": "richelieu",
                "barcode": self.barcode,
                "earliest_expiry_date": expiry_date or (date.today() + timedelta(days=20)).isoformat(),
                "initials": initials,
                "expected_revision": revision,
            },
        )

    def test_lookup_finds_exact_product_and_every_store_location(self):
        response = self.request(
            "GET", f"/api/expiry/product/{self.barcode}?store=richelieu"
        )
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["product"]["found"])
        self.assertTrue(data["product"]["in_plan"])
        self.assertEqual(data["product"]["name"], "BIOMEDIC GEL ANALG GLACE 255G")
        self.assertEqual(len(data["product"]["locations"]), 2)
        self.assertIsNone(data["current"])

    def test_equivalent_gtin_representation_resolves_same_package(self):
        response = self.request(
            "GET", "/api/expiry/product/0063848907665?store=richelieu"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["product"]["barcode"], self.barcode)

    def test_create_update_and_history_are_atomic(self):
        first_date = (date.today() + timedelta(days=20)).isoformat()
        second_date = (date.today() + timedelta(days=45)).isoformat()
        created = self.create_expiry(first_date)
        created_data = created.get_json()

        self.assertEqual(created.status_code, 200)
        self.assertEqual(created_data["action"], "created")
        self.assertEqual(created_data["current"]["revision"], 1)
        self.assertEqual(created_data["current"]["checked_by"], "AM")

        updated = self.create_expiry(second_date, revision=1, initials="JL")
        updated_data = updated.get_json()
        lookup = self.request(
            "GET", f"/api/expiry/product/{self.barcode}?store=richelieu"
        ).get_json()

        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated_data["action"], "updated")
        self.assertEqual(updated_data["current"]["revision"], 2)
        self.assertEqual(updated_data["current"]["earliest_expiry_date"], second_date)
        self.assertEqual([event["action"] for event in lookup["history"]], ["updated", "created"])

    def test_stale_revision_cannot_overwrite_another_employee(self):
        self.create_expiry()
        response = self.create_expiry(
            (date.today() + timedelta(days=30)).isoformat(), revision=99
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "expiry_conflict")
        self.assertEqual(
            self.db.execute("SELECT revision FROM product_expiry_status").fetchone()[0], 1
        )

    def test_board_is_sorted_and_reports_urgency_buckets(self):
        self.create_expiry((date.today() + timedelta(days=20)).isoformat())
        second_barcode = "041220000056"
        second_key = gtin_identity_key(second_barcode)
        self.db.execute(
            """INSERT INTO products
               (name, brand, barcode, gtin_key, aisle, side, section, shelf, position)
               VALUES('CROUSTILLES TEST', 'Test', ?, ?, '2', 'Gauche', '1', '1', '1')""",
            (second_barcode, second_key),
        )
        self.db.commit()
        expired_date = (date.today() - timedelta(days=2)).isoformat()
        response = self.request(
            "POST", "/api/expiry",
            {
                "store": "richelieu", "barcode": second_barcode,
                "earliest_expiry_date": expired_date, "initials": "AB",
            },
        )
        self.assertEqual(response.status_code, 200)

        board = self.request("GET", "/api/expiry?store=richelieu").get_json()
        self.assertEqual([item["barcode"] for item in board["items"]], [second_barcode, self.barcode])
        self.assertEqual(board["summary"]["expired"], 1)
        self.assertEqual(board["summary"]["soon"], 1)

    def test_clear_removes_current_status_but_keeps_audit_event(self):
        created = self.create_expiry().get_json()
        response = self.request(
            "DELETE",
            f"/api/expiry/{self.barcode}",
            {
                "store": "richelieu", "initials": "AM",
                "expected_revision": created["current"]["revision"],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM product_expiry_status").fetchone()[0], 0
        )
        actions = [
            row[0] for row in self.db.execute(
                "SELECT action FROM product_expiry_events ORDER BY id"
            ).fetchall()
        ]
        self.assertEqual(actions, ["created", "cleared"])

    def test_unknown_upc_can_still_be_tracked_without_fake_product_identity(self):
        unknown = "012345678905"
        response = self.request(
            "POST", "/api/expiry",
            {
                "store": "richelieu", "barcode": unknown,
                "earliest_expiry_date": (date.today() + timedelta(days=5)).isoformat(),
                "initials": "AM",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["product"]["found"])
        self.assertIn("Produit à identifier", response.get_json()["current"]["product_name"])

    def test_mutations_reject_non_object_json(self):
        created = self.create_expiry().get_json()
        create_response = self.request("POST", "/api/expiry", ["invalid"])
        clear_response = self.request(
            "DELETE", f"/api/expiry/{self.barcode}", ["invalid"]
        )

        self.assertEqual(create_response.status_code, 400)
        self.assertEqual(clear_response.status_code, 400)
        self.assertEqual(
            self.db.execute("SELECT revision FROM product_expiry_status").fetchone()[0],
            created["current"]["revision"],
        )


if __name__ == "__main__":
    unittest.main()
