import unittest

from scraper.stubhub_parking import StubHubParkingScraper


def _scraper() -> StubHubParkingScraper:
    return StubHubParkingScraper.__new__(StubHubParkingScraper)


class StubHubParkingParserTest(unittest.TestCase):
    def test_extract_passes_from_visible_section_popups_on_load_text(self):
        payload = (
            '[{"Data":[{"VisibleSectionPopupsOnLoad":"['
            '{\\"sectionId\\":\\"1719085\\",\\"price\\":\\"$100\\",\\"eventId\\":159107040},'
            '{\\"sectionId\\":\\"1550908\\",\\"price\\":\\"$65\\",\\"eventId\\":159107040}'
            ']"}]}]'
        )
        passes = StubHubParkingScraper._extract_passes_from_text(payload, source="har_fixture")
        # Section-only telemetry rows are intentionally excluded.
        self.assertEqual(len(passes), 0)

    def test_extract_passes_from_text_with_listing_id_and_prices(self):
        payload = (
            '{"visiblePopups":{"1719085_1719085":{"rawPrice":1222,"formattedPrice":"$12.22",'
            '"listingId":11105174029,"popupValue":"1 - 2 passes"}}}'
        )
        passes = StubHubParkingScraper._extract_passes_from_text(payload, source="har_fixture")
        self.assertGreaterEqual(len(passes), 1)
        first = passes[0]
        self.assertEqual(first.get("listing_id"), "11105174029")
        self.assertEqual(str(first.get("price")), "12.22")

    def test_extract_passes_from_json_payload_accepts_inventory_id_and_current_price(self):
        scraper = _scraper()
        payload = (
            '{"event":{"listings":[{"inventoryId":"inv-1","listingKey":"lk-1","currentPrice":"$44.00",'
            '"listingTitle":"Lot A","minQuantity":1,"maxQuantity":2}]}}'
        )
        passes = scraper._extract_passes_from_json_payload(payload)
        self.assertEqual(len(passes), 1)
        row = passes[0]
        self.assertEqual(row.get("listing_id"), "inv-1")
        self.assertEqual(row.get("lot_name"), "Lot A")
        self.assertEqual(row.get("price"), "44.00")

    def test_filter_telemetry_rows_drops_generic_without_id(self):
        rows = [
            {
                "lot_name": "Section 1550779",
                "price": "24.00",
                "listing_details": {"price_incl_fees": "$24.00"},
                "_source": "embedded_xhr",
            },
            {
                "lot_name": "Lot 6",
                "price": "24.00",
                "availability": "1 - 4 passes",
                "_source": "dom",
            },
        ]
        filtered = StubHubParkingScraper._filter_telemetry_rows(rows)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].get("lot_name"), "Lot 6")

    def test_filter_telemetry_rows_keeps_id_backed_generic(self):
        rows = [
            {
                "lot_name": "Section 1550779",
                "price": "24.00",
                "listing_id": "11105174029",
                "_source": "payload_json",
                "listing_details": {"price_incl_fees": "$24.00"},
            }
        ]
        filtered = StubHubParkingScraper._filter_telemetry_rows(rows)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].get("listing_id"), "11105174029")

    def test_extract_total_listing_count_uses_max(self):
        body = "Sidebar 9 listings. Parking 264 listings total."
        self.assertEqual(StubHubParkingScraper._extract_total_listing_count(body), 264)

    def test_extract_total_listing_count_single_match(self):
        self.assertEqual(StubHubParkingScraper._extract_total_listing_count("12 listings for this event"), 12)

    def test_extract_total_listing_count_no_match(self):
        self.assertIsNone(StubHubParkingScraper._extract_total_listing_count("no listing count here"))

    def test_expansion_budget_none_or_small(self):
        self.assertEqual(StubHubParkingScraper._expansion_budget_for_advertised(None), (45.0, 36))
        self.assertEqual(StubHubParkingScraper._expansion_budget_for_advertised(0), (45.0, 36))

    def test_expansion_budget_264_scales_within_caps(self):
        d, r = StubHubParkingScraper._expansion_budget_for_advertised(264)
        self.assertGreaterEqual(d, 90.0)
        self.assertLessEqual(d, 180.0)
        self.assertGreaterEqual(r, 36)
        self.assertLessEqual(r, 90)

    def test_expansion_budget_huge_advertised_hits_ceiling(self):
        d, r = StubHubParkingScraper._expansion_budget_for_advertised(5000)
        self.assertEqual(d, 180.0)
        self.assertEqual(r, 90)

    def test_is_real_listing_row_accepts_dom_section_without_listing_id(self):
        row = {
            "lot_name": "Section 1699535",
            "_source": "dom",
            "price": "27.00",
            "availability": "1 pass",
            "listing_details": {"price_incl_fees": "$27.00"},
        }
        self.assertTrue(StubHubParkingScraper._is_real_listing_row(row))

    def test_is_real_listing_row_rejects_payload_json_section_without_id(self):
        row = {
            "lot_name": "Section 1699535",
            "_source": "payload_json",
            "price": "27.00",
            "availability": "1 pass",
        }
        self.assertFalse(StubHubParkingScraper._is_real_listing_row(row))

    def test_is_real_listing_row_rejects_dom_section_without_detail_signal(self):
        row = {
            "lot_name": "Section 1699535",
            "_source": "dom",
            "price": "27.00",
            "listing_details": {"price_incl_fees": "$27.00"},
        }
        self.assertFalse(StubHubParkingScraper._is_real_listing_row(row))

    def test_pick_display_lot_name_prefers_listing_title_over_section(self):
        inv = {
            "listingTitle": "Lot 6",
            "sectionName": "Section 1550779",
            "ticketClassName": "Other",
        }
        self.assertEqual(StubHubParkingScraper._pick_display_lot_name_from_inventory(inv), "Lot 6")

    def test_pick_display_lot_name_prefers_ticket_class_over_numeric_section(self):
        inv = {"sectionName": "1550779", "ticketClassName": "Lot 7"}
        self.assertEqual(StubHubParkingScraper._pick_display_lot_name_from_inventory(inv), "Lot 7")

    def test_pick_display_lot_name_fallback_to_section_only(self):
        inv = {"sectionName": "1550779"}
        self.assertEqual(StubHubParkingScraper._pick_display_lot_name_from_inventory(inv), "1550779")

    def test_merge_prefers_non_generic_lot_name_same_listing_id(self):
        a = {
            "listing_id": "111",
            "lot_name": "Section 1550779",
            "price": "27.00",
            "availability": "1 pass",
            "listing_details": {"title": "Section 1550779"},
            "_source": "payload_json",
        }
        b = {
            "listing_id": "111",
            "lot_name": "Lot 6",
            "price": "27.00",
            "availability": "1 pass",
            "listing_details": {"title": "Lot 6"},
            "_source": "dom",
        }
        merged = StubHubParkingScraper._merge_pass_collections([a], [b])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].get("lot_name"), "Lot 6")

    def test_primary_price_prefers_sale_when_percent_off(self):
        card = "VIP Parking\n40% off\n$261\n$155"
        price, _ = StubHubParkingScraper._primary_price_value_from_card_text(card)
        self.assertEqual(price, "155.00")

    def test_primary_price_uses_last_when_two_amounts_no_discount_hint(self):
        card = "Some Lot\n$100\n$90"
        price, _ = StubHubParkingScraper._primary_price_value_from_card_text(card)
        self.assertEqual(price, "90.00")

    def test_dom_pick_lot_name_skips_price_per_pass_header(self):
        lines = [
            "Price per pass",
            "Star Parking",
            "1 pass",
            "$122",
        ]
        self.assertEqual(
            StubHubParkingScraper._dom_pick_lot_name("Price per pass", lines),
            "Star Parking",
        )

    def test_is_real_listing_row_rejects_chaff_title_without_id(self):
        row = {
            "lot_name": "Price per pass",
            "_source": "dom",
            "price": "125.00",
            "listing_details": {"price_incl_fees": "$125.00"},
        }
        self.assertFalse(StubHubParkingScraper._is_real_listing_row(row))

    def test_price_identity_token_normalizes_integer_cents_style(self):
        self.assertEqual(StubHubParkingScraper._price_identity_token("125"), "125.00")
        self.assertEqual(StubHubParkingScraper._price_identity_token("125.00"), "125.00")


if __name__ == "__main__":
    unittest.main()
