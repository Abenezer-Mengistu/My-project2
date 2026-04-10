import os
import tempfile
import unittest
from pathlib import Path

from cli.stubhub_catalog_sync import (
    catalog_sync_on_start_enabled,
    dedupe_preserve_order,
    parse_stubhub_urls_file,
)


class StubhubCatalogSyncCliTest(unittest.TestCase):
    def test_parse_urls_file_skips_comments_and_dedupes(self):
        sample = """# header
https://www.stubhub.com/parking-passes-only-foo/event/1/
https://www.stubhub.com/parking-passes-only-foo/event/1/

https://www.stubhub.com/parking-passes-only-bar/event/2/
"""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "urls.txt"
            p.write_text(sample, encoding="utf-8")
            urls = parse_stubhub_urls_file(p)
        self.assertEqual(len(urls), 2)
        self.assertIn("parking-passes-only-foo", urls[0])
        self.assertIn("parking-passes-only-bar", urls[1])

    def test_dedupe_preserve_order(self):
        self.assertEqual(
            dedupe_preserve_order(["a", "b", "a", "c"]),
            ["a", "b", "c"],
        )

    def test_catalog_sync_on_start_default_and_opt_out(self):
        old = os.environ.pop("STUBHUB_CATALOG_SYNC_ON_START", None)
        try:
            self.assertTrue(catalog_sync_on_start_enabled())
            os.environ["STUBHUB_CATALOG_SYNC_ON_START"] = "0"
            self.assertFalse(catalog_sync_on_start_enabled())
            os.environ["STUBHUB_CATALOG_SYNC_ON_START"] = "yes"
            self.assertTrue(catalog_sync_on_start_enabled())
        finally:
            if old is None:
                os.environ.pop("STUBHUB_CATALOG_SYNC_ON_START", None)
            else:
                os.environ["STUBHUB_CATALOG_SYNC_ON_START"] = old


if __name__ == "__main__":
    unittest.main()
