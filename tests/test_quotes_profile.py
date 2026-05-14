import datetime
import unittest

from src.program_client_profile import ClientProfile
from src.program_constants import BUFFER_MARGIN, INDEX_DETAILS_FNO
from src.program_quotes import Quotes


class FakeFivePaisaQuoteClient:
    def __init__(self):
        self.expiry_calls = []
        self.feed_calls = []

    def get_expiry(self, exchange, index):
        self.expiry_calls.append((exchange, index))
        return {"lastrate": [{"LTP": 23456.7}]}

    def fetch_market_feed(self, payload):
        self.feed_calls.append(payload)
        return {"Data": [{"LastRate": 101.5, "High": 110, "Low": 95, "Symbol": "X"}]}


class FakeNeoProfileClient:
    NeoWebSocket = object()

    def __init__(self, net: int | float = 100000):
        self.net = net

    def limits(self):
        return {"Net": str(self.net)}

    def order_report(self):
        return {
            "data": [
                {"trnsTp": "B", "stat": "complete"},
                {"trnsTp": "S", "stat": "complete"},
                {"trnsTp": "B", "stat": "rejected"},
                {"trnsTp": "B", "stat": "COMPLETE"},
            ]
        }


class FakeFivePaisaProfileClient:
    def __init__(self, net: int | float = 90000):
        self.net = net

    def margin(self):
        return [{"NetAvailableMargin": str(self.net)}]


class TestQuotes(unittest.TestCase):
    def test_get_ltp_index_uses_expected_exchange(self):
        client = FakeFivePaisaQuoteClient()
        quotes = Quotes(client, INDEX_DETAILS_FNO)

        self.assertEqual(quotes.get_ltp_index("NIFTY"), 23456.7)
        self.assertEqual(client.expiry_calls[-1], ("N", "NIFTY"))

        self.assertEqual(quotes.get_ltp_index("SENSEX"), 23456.7)
        self.assertEqual(client.expiry_calls[-1], ("B", "SENSEX"))

    def test_get_ltp_index_bankex_uses_current_workaround(self):
        client = FakeFivePaisaQuoteClient()
        quotes = Quotes(client, INDEX_DETAILS_FNO)

        self.assertEqual(quotes.get_ltp_index("BANKEX"), 30456.7)
        self.assertEqual(client.expiry_calls[-1], ("N", "BANKNIFTY"))

    def test_expiry_helpers_and_current_week_expiry(self):
        quotes = Quotes(FakeFivePaisaQuoteClient(), INDEX_DETAILS_FNO)

        self.assertEqual(quotes._get_expiry_day("Tuesday"), 1)
        self.assertTrue(quotes._is_holiday("2024-04-10", ["20240410"]))

        expiry = quotes.get_current_week_expiry_date("NIFTY")

        self.assertRegex(expiry, r"^\d{4}-\d{2}-\d{2}$")
        with self.assertRaises(ValueError):
            quotes.get_current_week_expiry_date("UNKNOWN")

    def test_nse_holiday_moves_nifty_tuesday_expiry_to_previous_trading_day(self):
        quotes = Quotes(FakeFivePaisaQuoteClient(), INDEX_DETAILS_FNO)

        expiry = quotes.get_current_week_expiry_date(
            "NIFTY",
            holiday_list=["20261020"],
            today=datetime.date(2026, 10, 19),
        )

        self.assertEqual(expiry, "2026-10-19")

    def test_may_2026_sensex_holiday_shifts_expiry_left(self):
        quotes = Quotes(FakeFivePaisaQuoteClient(), INDEX_DETAILS_FNO)

        expiry = quotes.get_current_week_expiry_date(
            "SENSEX",
            holiday_list=["20260528"],
            today=datetime.date(2026, 5, 25),
        )

        self.assertEqual(expiry, "2026-05-27")

    def test_after_all_may_2026_expiries_pass_next_week_is_selected(self):
        quotes = Quotes(FakeFivePaisaQuoteClient(), INDEX_DETAILS_FNO)

        nifty_expiry = quotes.get_current_week_expiry_date(
            "NIFTY",
            holiday_list=["20260528"],
            today=datetime.date(2026, 5, 29),
        )
        sensex_expiry = quotes.get_current_week_expiry_date(
            "SENSEX",
            holiday_list=["20260528"],
            today=datetime.date(2026, 5, 29),
        )

        self.assertEqual(nifty_expiry, "2026-06-02")
        self.assertEqual(sensex_expiry, "2026-06-04")

    def test_strike_price_generation_uses_step_size_and_chain_depth(self):
        quotes = Quotes(FakeFivePaisaQuoteClient(), INDEX_DETAILS_FNO)

        strikes = quotes.get_opt_strike_price_list("NIFTY", 23456.7)

        self.assertEqual(len(strikes), 7)
        self.assertEqual(strikes[0], "23300")
        self.assertEqual(strikes[-1], "23600")

    def test_get_ltp_for_option_uses_optional_payload_without_building_new_one(self):
        client = FakeFivePaisaQuoteClient()
        quotes = Quotes(client, INDEX_DETAILS_FNO)
        payload = [{"Symbol": "NIFTY 19 May 2026 CE 23400.00"}]

        result = quotes.get_ltp_for_opt_strike_price(optional_list=payload)

        self.assertEqual(result["Data"][0]["LastRate"], 101.5)
        self.assertEqual(client.feed_calls[-1], payload)

    def test_get_ltp_for_option_default_payload_is_preserved(self):
        client = FakeFivePaisaQuoteClient()
        quotes = Quotes(client, INDEX_DETAILS_FNO)

        result = quotes.get_ltp_for_opt_strike_price()

        self.assertEqual(result["Data"][0]["LastRate"], 101.5)
        self.assertEqual(client.feed_calls[-1], [{"Random": "Data"}])

    def test_get_ltp_for_option_builds_payload_when_optional_list_absent(self):
        client = FakeFivePaisaQuoteClient()
        quotes = Quotes(client, INDEX_DETAILS_FNO)

        result = quotes.get_ltp_for_opt_strike_price(
            current_expiry="2026-05-19",
            strike_price=23400,
            option_type="CE",
            index_key="NIFTY",
            optional_list=[],
        )

        self.assertEqual(result, (101.5, 23400))
        self.assertEqual(client.feed_calls[-1][0]["Expiry"], "20260519")


class TestClientProfile(unittest.TestCase):
    def test_neo_margin_subtracts_buffer_and_counts_complete_buy_orders(self):
        client = FakeNeoProfileClient(net=900000)
        profile = ClientProfile(client)

        self.assertEqual(profile.get_client_available_margin(), 900000 - BUFFER_MARGIN)
        self.assertEqual(profile.get_completed_buy_order_count(), 2)

    def test_margin_never_goes_below_one(self):
        self.assertEqual(
            ClientProfile(FakeNeoProfileClient(net=100)).get_client_available_margin(),
            1,
        )
        self.assertEqual(
            ClientProfile(
                FakeFivePaisaProfileClient(net=100)
            ).get_client_available_margin(),
            1,
        )

    def test_5paisa_margin_subtracts_buffer_and_completed_order_count_is_zero(self):
        profile = ClientProfile(FakeFivePaisaProfileClient(net=900000))

        self.assertEqual(profile.get_client_available_margin(), 900000 - BUFFER_MARGIN)
        self.assertEqual(profile.get_completed_buy_order_count(), 0)

    def test_profile_margin_does_not_apply_time_window_override(self):
        profile = ClientProfile(FakeFivePaisaProfileClient(net=900000))
        self.assertEqual(profile.get_client_available_margin(), 900000 - BUFFER_MARGIN)

    def test_profile_margin_is_rounded_after_buffer(self):
        profile = ClientProfile(FakeFivePaisaProfileClient(net=900000.126))
        self.assertEqual(
            profile.get_client_available_margin(), round(900000.126 - BUFFER_MARGIN, 2)
        )
