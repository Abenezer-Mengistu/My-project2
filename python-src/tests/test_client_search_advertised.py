import unittest

from app import _apply_stubhub_parity_policy, _choose_stubhub_lot_name, _group_client_search_results, _normalize_availability


class ClientSearchAdvertisedTotalTest(unittest.TestCase):
    def test_group_sets_advertised_total_from_phase2_rows(self):
        search_items = [
            {
                "url": "https://www.stubhub.com/parking-passes-only-test/event/159107040/",
                "name": "Test Event",
                "dayOfWeek": "Sat",
                "formattedDate": "Apr 12",
                "formattedTime": "7 PM",
                "venueName": "Venue",
                "formattedVenueLocation": "City",
            }
        ]
        rows = [
            {
                "parking_url": "https://www.stubhub.com/parking-passes-only-test/event/159107040/",
                "lot_name": "Inspiration Lot",
                "price": "14.00",
                "listing_id": "11105174029",
                "advertised_total": 264,
                "listing_details": {"availability": "1 pass"},
            }
        ]
        out = _group_client_search_results(search_items, rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].get("advertised_total"), 264)
        self.assertEqual(out[0].get("listing_count"), 1)

    def test_group_picks_max_advertised_total(self):
        search_items = [{"url": "https://www.stubhub.com/parking/foo/event/1/", "name": "E"}]
        rows = [
            {
                "parking_url": "https://www.stubhub.com/parking/foo/event/1/",
                "lot_name": "A",
                "price": "1",
                "listing_id": "1",
                "advertised_total": 100,
                "listing_details": {"availability": "1 pass"},
            },
            {
                "parking_url": "https://www.stubhub.com/parking/foo/event/1/",
                "lot_name": "B",
                "price": "2",
                "listing_id": "2",
                "advertised_total": 200,
                "listing_details": {"availability": "1 pass"},
            },
        ]
        out = _group_client_search_results(search_items, rows)
        self.assertEqual(out[0].get("advertised_total"), 200)

    def test_group_propagates_scrape_incomplete(self):
        search_items = [
            {
                "url": "https://www.stubhub.com/parking-passes-only-x/event/99/",
                "name": "E",
            }
        ]
        rows = [
            {
                "parking_url": "https://www.stubhub.com/parking-passes-only-x/event/99/",
                "lot_name": "Lot A",
                "price": "10",
                "listing_id": "1",
                "advertised_total": 200,
                "scrape_incomplete": True,
                "scrape_stop_reason": "timeout",
                "listing_details": {"availability": "1 pass"},
            }
        ]
        out = _group_client_search_results(search_items, rows)
        self.assertTrue(out[0].get("scrape_incomplete"))
        self.assertEqual(out[0].get("scrape_stop_reason"), "timeout")

    def test_group_includes_generic_section_row_with_availability(self):
        search_items = [
            {
                "url": "https://www.stubhub.com/parking-passes-only-tobin/event/1/",
                "name": "Cheerleaders",
            }
        ]
        rows = [
            {
                "parking_url": "https://www.stubhub.com/parking-passes-only-tobin/event/1/",
                "lot_name": "Section 1699535",
                "price": "42.00",
                "advertised_total": 40,
                "listing_details": {"availability": "1 pass", "price_incl_fees": "$42.00"},
            }
        ]
        out = _group_client_search_results(search_items, rows)
        self.assertEqual(len(out), 1)
        listings = out[0].get("listings") or []
        self.assertEqual(len(listings), 1)
        self.assertEqual(listings[0].get("lot_name"), "Section 1699535")


class ChooseStubhubLotNameTest(unittest.TestCase):
    def test_preserves_raw_lot_name(self):
        row = {
            "lot_name": "Section 1550779",
            "listing_details": {"title": "Lot 6"},
        }
        details = row["listing_details"]
        self.assertEqual(_choose_stubhub_lot_name(row, details), "Lot 6")

    def test_prefers_address_or_garage_over_section(self):
        row = {
            "lot_name": "Section 1551270",
            "listing_details": {"title": "306 E. 4th St. - 0.2 mi from venue"},
        }
        details = row["listing_details"]
        self.assertEqual(_choose_stubhub_lot_name(row, details), "306 E. 4th St. - 0.2 mi from venue")

    def test_uses_distance_only_when_no_better_name_exists(self):
        row = {
            "lot_name": "Within 0.75 mi",
            "listing_details": {"availability": "1 - 2 passes"},
        }
        details = row["listing_details"]
        self.assertEqual(_choose_stubhub_lot_name(row, details), "Within 0.75 mi")


class StubHubParityPolicyTest(unittest.TestCase):
    def test_mismatch_hides_rows_until_parity(self):
        card = {
            "parking_url": "https://www.stubhub.com/parking-passes-only-x/event/99/",
            "advertised_total": 11,
            "listings": [{"lot_name": "A", "price_value": "10.00"}],
            "listing_count": 1,
            "scrape_incomplete": False,
        }
        out = _apply_stubhub_parity_policy(card, card["parking_url"])
        self.assertTrue(out.get("parity_pending"))
        self.assertEqual(out.get("parity_reason"), "count_mismatch")
        self.assertEqual(out.get("parity_loaded_count"), 1)
        self.assertEqual(out.get("listing_count"), 0)
        self.assertEqual(out.get("listings"), [])

    def test_equal_counts_keeps_rows(self):
        card = {
            "parking_url": "https://www.stubhub.com/parking-passes-only-y/event/1/",
            "advertised_total": 2,
            "listings": [{"lot_name": "A"}, {"lot_name": "B"}],
            "listing_count": 2,
            "scrape_incomplete": False,
        }
        out = _apply_stubhub_parity_policy(card, card["parking_url"])
        self.assertFalse(out.get("parity_pending"))
        self.assertEqual(out.get("listing_count"), 2)
        self.assertEqual(len(out.get("listings") or []), 2)

    def test_equal_counts_clear_parity_even_when_scrape_flagged_incomplete(self):
        """Stop reasons like no_growth/timeout must not block UI if loaded == advertised."""
        listings = [{"lot_name": f"L{i}", "price_value": "1.00"} for i in range(19)]
        card = {
            "parking_url": "https://www.stubhub.com/parking-passes-only-z/event/2/",
            "advertised_total": 19,
            "listings": listings,
            "listing_count": 19,
            "scrape_incomplete": True,
            "scrape_stop_reason": "no_growth",
        }
        out = _apply_stubhub_parity_policy(card, card["parking_url"])
        self.assertFalse(out.get("parity_pending"))
        self.assertFalse(out.get("scrape_incomplete"))
        self.assertEqual(len(out.get("listings") or []), 19)


class NormalizeAvailabilityTest(unittest.TestCase):
    def test_zero_pass_hidden(self):
        self.assertIsNone(_normalize_availability("0 passes"))
        self.assertIsNone(_normalize_availability("0 pass"))
        self.assertIsNone(_normalize_availability("0"))
        self.assertEqual(_normalize_availability("1 pass"), "1 pass")
        self.assertEqual(_normalize_availability("2 - 4 passes"), "2 - 4 passes")


if __name__ == "__main__":
    unittest.main()
