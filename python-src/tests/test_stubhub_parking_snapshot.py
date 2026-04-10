import tempfile
import unittest
from pathlib import Path

from utils.stubhub_parking_snapshot import (
    diff_card_listings,
    load_snapshot,
    save_snapshot_atomic,
)


class StubhubParkingSnapshotTest(unittest.TestCase):
    def test_diff_detects_added_removed_price_change(self):
        old = [
            {"listing_id": "1", "lot_name": "Lot A", "price_display": "$10", "price_value": 10, "availability": "1 pass"},
            {"listing_id": "2", "lot_name": "Lot B", "price_display": "$20", "price_value": 20, "availability": "1 pass"},
        ]
        new = [
            {"listing_id": "2", "lot_name": "Lot B", "price_display": "$25", "price_value": 25, "availability": "1 pass"},
            {"listing_id": "3", "lot_name": "Lot C", "price_display": "$30", "price_value": 30, "availability": "2 passes"},
        ]
        d = diff_card_listings(old, new)
        self.assertEqual(d["added_count"], 1)
        self.assertEqual(d["removed_count"], 1)
        self.assertGreaterEqual(d["price_or_avail_changed_count"], 1)

    def test_save_and_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            url = "https://www.stubhub.com/parking-passes-only-test/event/123/"
            body = {"success": True, "parking_url": url, "card": {"listings": []}, "errors": []}
            save_snapshot_atomic(base, url, {"scraped_at": 1.0, "response": body})
            loaded = load_snapshot(base, url)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.get("scraped_at"), 1.0)
            self.assertEqual(loaded.get("response", {}).get("parking_url"), url)


if __name__ == "__main__":
    unittest.main()
