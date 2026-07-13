import re
import unittest
from pathlib import Path

from jinja2 import Template


class AssetVersioningTests(unittest.TestCase):
    def test_every_local_app_asset_uses_the_same_deployment_version(self):
        source = Path("templates/index.html").read_text(encoding="utf-8-sig")
        html = Template(source).render(asset_version="release123")

        scripts = re.findall(r'<script src="(/static/[^"]+)"', html)
        self.assertGreaterEqual(len(scripts), 10)
        self.assertTrue(all(path.endswith("?v=release123") for path in scripts))
        self.assertIn('/static/style.css?v=release123', html)
        self.assertIn('name="app-asset-version" content="release123"', html)


if __name__ == "__main__":
    unittest.main()
