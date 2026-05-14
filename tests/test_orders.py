import copy
import unittest
from unittest.mock import patch

from src.program_orders import Orders


class FakeFivePaisaOrderClient:
    def __init__(self):
        self.placed_orders = []
        self.cancelled_bulk_orders = []
        self.order_book_calls = 0

    def place_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        return {"status": "ok"}

    def positions(self):
        return {
            "data": [
                {
                    "BuyQty": 10,
                    "SellQty": 0,
                    "NetQty": 10,
                    "Exch": "N",
                    "ExchType": "D",
                    "ScripName": "NIFTY 19 MAY 2026 CE 23400.00",
                    "ScripCode": 123,
                },
                {
                    "BuyQty": 0,
                    "SellQty": 5,
                    "NetQty": -5,
                    "Exch": "N",
                    "ExchType": "D",
                    "ScripName": "NIFTY 19 MAY 2026 PE 23400.00",
                    "ScripCode": 124,
                },
            ]
        }

    def order_book(self):
        self.order_book_calls += 1
        if self.order_book_calls == 1:
            return [
                {
                    "ExchOrderID": "A",
                    "TradedQty": 0,
                    "ScripCode": 123,
                    "OrderStatus": "Pending",
                },
                {"OrderStatus": "Complete"},
            ]
        return []

    def cancel_bulk_order(self, payload):
        self.cancelled_bulk_orders.append(payload)
        return {"cancelled": True}


class FakeNeoOrderClient:
    NeoWebSocket = object()

    def __init__(self):
        self.placed_orders = []
        self.cancelled_orders = []

    def place_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        return {"status": "ok"}

    def positions(self):
        return {
            "data": [
                {
                    "trdSym": "NIFTY2651923400CE",
                    "sym": "NIFTY",
                    "tok": "555",
                    "exSeg": "nse_fo",
                    "prod": "MIS",
                    "flBuyQty": "130",
                    "flSellQty": "0",
                },
                {
                    "trdSym": "BANKNIFTY2651953600PE",
                    "sym": "BANKNIFTY",
                    "tok": "556",
                    "exSeg": "nse_fo",
                    "prod": "MIS",
                    "flBuyQty": "0",
                    "flSellQty": "30",
                },
                {
                    "trdSym": "NIFTY2651923400PE",
                    "sym": "NIFTY",
                    "tok": "557",
                    "exSeg": "nse_fo",
                    "prod": "NRML",
                    "flBuyQty": "130",
                    "flSellQty": "0",
                },
            ]
        }

    def quotes(self, instrument_tokens, quote_type):
        return [{"ltp": 100.0}]

    def order_report(self):
        return {
            "data": [
                {"nOrdNo": "111", "ordSt": "open", "stat": "complete", "trnsTp": "B"},
                {
                    "nOrdNo": "222",
                    "ordSt": "complete",
                    "stat": "complete",
                    "trnsTp": "S",
                },
                {"nOrdNo": "333", "ordSt": "open", "stat": "rejected", "trnsTp": "B"},
            ]
        }

    def cancel_order(self, order_id):
        self.cancelled_orders.append(order_id)
        return {"cancelled": order_id}


class EmptyPositionsListClient(FakeFivePaisaOrderClient):
    def positions(self):
        return []


class FailsOnMutationFivePaisaClient(FakeFivePaisaOrderClient):
    def place_order(self, **kwargs):
        raise AssertionError(f"dry-run should not place order: {kwargs}")

    def cancel_bulk_order(self, payload):
        raise AssertionError(f"dry-run should not cancel bulk order: {payload}")


class FailsOnMutationNeoClient(FakeNeoOrderClient):
    def place_order(self, **kwargs):
        raise AssertionError(f"dry-run should not place order: {kwargs}")

    def cancel_order(self, order_id):
        raise AssertionError(f"dry-run should not cancel order: {order_id}")


def run_sync(target, *args):
    target(*args)


class TestOrders(unittest.TestCase):
    def test_place_buy_order_bulk_chunks_and_never_uses_real_client(self):
        client = FakeFivePaisaOrderClient()
        orders = Orders(client)
        bulk_order = [
            [
                {
                    "index": "NIFTY",
                    "tag": "unit",
                    "quantity": "2000",
                    "trading_symbol": "NIFTY2651923400CE",
                }
            ]
        ]

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_buy_order_bulk(copy.deepcopy(bulk_order), intraday=False)

        self.assertEqual(
            [order["quantity"] for order in client.placed_orders], ["1625", "375"]
        )
        self.assertTrue(
            all(order["product"] == "NRML" for order in client.placed_orders)
        )
        self.assertNotIn("index", client.placed_orders[0])
        self.assertNotIn("tag", client.placed_orders[0])

    def test_place_buy_order_bulk_accepts_product_string_flag(self):
        client = FakeFivePaisaOrderClient()
        orders = Orders(client)
        bulk_order = [
            [
                {
                    "index": "NIFTY",
                    "tag": "unit",
                    "quantity": "65",
                    "trading_symbol": "NIFTY2651923400CE",
                }
            ]
        ]

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_buy_order_bulk(copy.deepcopy(bulk_order), intraday="NRML")

        self.assertEqual(client.placed_orders[0]["product"], "NRML")

    def test_place_buy_order_bulk_preserves_special_index_chunk_overrides(self):
        client = FakeFivePaisaOrderClient()
        orders = Orders(client)
        bulk_order = [
            [
                {
                    "index": "BANKEX",
                    "tag": "unit",
                    "quantity": "1000",
                    "trading_symbol": "BANKEX2651980000CE",
                }
            ],
            [
                {
                    "index": "MIDCPNifty",
                    "tag": "unit",
                    "quantity": "3000",
                    "trading_symbol": "MIDCPNIFTY2651912000PE",
                }
            ],
        ]

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_buy_order_bulk(copy.deepcopy(bulk_order))

        self.assertEqual(
            [order["quantity"] for order in client.placed_orders],
            ["900", "100", "2800", "200"],
        )
        self.assertTrue(
            all(order["product"] == "MIS" for order in client.placed_orders)
        )

    def test_5paisa_sell_order_places_sell_and_buy_cover_payloads(self):
        client = FakeFivePaisaOrderClient()
        orders = Orders(client)

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_sell_order_all(intraday=True)

        self.assertEqual(len(client.placed_orders), 2)
        self.assertEqual(client.placed_orders[0]["OrderType"], "S")
        self.assertEqual(client.placed_orders[0]["Qty"], 10)
        self.assertEqual(client.placed_orders[1]["OrderType"], "B")
        self.assertEqual(client.placed_orders[1]["Qty"], 5)

    def test_neo_sell_order_uses_live_ltp_and_skips_nrml(self):
        client = FakeNeoOrderClient()
        orders = Orders(client)

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_sell_order_all()

        self.assertEqual(len(client.placed_orders), 2)
        self.assertEqual(client.placed_orders[0]["transaction_type"], "S")
        self.assertEqual(client.placed_orders[0]["price"], "100")
        self.assertEqual(client.placed_orders[1]["transaction_type"], "B")
        self.assertEqual(client.placed_orders[1]["price"], "100")

    def test_neo_sell_order_infers_missing_symbol_before_chunking(self):
        class MissingSymbolNeoClient(FakeNeoOrderClient):
            def positions(self):
                return {
                    "data": [
                        {
                            "trdSym": "MIDCPNIFTY2651912000CE",
                            "tok": "777",
                            "exSeg": "nse_fo",
                            "prod": "MIS",
                            "flBuyQty": "3000",
                            "flSellQty": "0",
                        }
                    ]
                }

            def quotes(self, instrument_tokens, quote_type):
                return [{"ltp": 200.0}]

        client = MissingSymbolNeoClient()
        orders = Orders(client)

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_sell_order_all()

        self.assertEqual(
            [order["quantity"] for order in client.placed_orders], ["2800", "200"]
        )
        self.assertTrue(
            all(order["transaction_type"] == "S" for order in client.placed_orders)
        )
        self.assertTrue(all(order["price"] == "199" for order in client.placed_orders))

    def test_cancel_all_open_orders_for_5paisa_and_neo(self):
        five_client = FakeFivePaisaOrderClient()
        Orders(five_client).cancel_all_open_orders()
        self.assertEqual(five_client.cancelled_bulk_orders, [[{"ExchOrderID": "A"}]])

        neo_client = FakeNeoOrderClient()
        Orders(neo_client).cancel_all_open_orders()
        self.assertEqual(neo_client.cancelled_orders, ["111", "333"])

    def test_dry_run_buy_builds_payloads_without_placing_orders(self):
        client = FailsOnMutationFivePaisaClient()
        orders = Orders(client)
        bulk_order = [
            [
                {
                    "index": "NIFTY",
                    "tag": "unit",
                    "quantity": "2000",
                    "trading_symbol": "NIFTY2651923400CE",
                }
            ]
        ]

        with (
            patch("src.program_orders.DRY_RUN_ORDERS", True),
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            orders.place_buy_order_bulk(copy.deepcopy(bulk_order), intraday=False)

        self.assertEqual(client.placed_orders, [])

    def test_dry_run_sell_and_cancel_never_call_mutating_broker_apis(self):
        five_client = FailsOnMutationFivePaisaClient()
        neo_client = FailsOnMutationNeoClient()

        with (
            patch("src.program_orders.DRY_RUN_ORDERS", True),
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.disable_loguru_to_devnull"),
        ):
            Orders(five_client).place_sell_order_all(intraday=True)
            Orders(five_client).cancel_all_open_orders()
            Orders(neo_client).place_sell_order_all()
            Orders(neo_client).cancel_all_open_orders()

        self.assertEqual(five_client.placed_orders, [])
        self.assertEqual(five_client.cancelled_bulk_orders, [])
        self.assertEqual(neo_client.placed_orders, [])
        self.assertEqual(neo_client.cancelled_orders, [])

    def test_open_positions_and_completed_counts(self):
        five_client = FakeFivePaisaOrderClient()
        five_orders = Orders(five_client)

        self.assertEqual(len(five_orders.get_open_positions()), 2)
        self.assertEqual(five_orders.count_completed_orders(), 1)

        neo_orders = Orders(FakeNeoOrderClient())
        self.assertEqual(neo_orders.count_completed_orders(), 1)

    def test_empty_positions_list_is_not_logged_as_warning(self):
        client = EmptyPositionsListClient()
        orders = Orders(client)

        with patch("src.program_orders.log.warning") as warning_log:
            self.assertEqual(orders.get_open_positions(), [])

        warning_log.assert_not_called()

    def test_sell_all_handles_empty_positions_list_without_error_log(self):
        client = EmptyPositionsListClient()
        orders = Orders(client)

        with (
            patch("src.program_orders.run_as_background_thread", side_effect=run_sync),
            patch("src.program_orders.log.exception") as exception_log,
        ):
            orders.place_sell_order_all()

        self.assertEqual(client.placed_orders, [])
        exception_log.assert_not_called()

    def test_order_methods_swallow_broker_exceptions(self):
        class BrokenClient(FakeFivePaisaOrderClient):
            def place_order(self, **kwargs):
                raise RuntimeError("blocked")

            def positions(self):
                raise RuntimeError("blocked")

            def order_book(self):
                raise RuntimeError("blocked")

        orders = Orders(BrokenClient())

        with patch("src.program_orders.run_as_background_thread", side_effect=run_sync):
            orders.place_buy_order_bulk(
                [[{"index": "NIFTY", "tag": "x", "quantity": "1"}]]
            )
        orders.place_sell_order_all()
        orders.cancel_all_open_orders()
        self.assertEqual(orders.get_open_positions(), [])
        self.assertEqual(orders.count_completed_orders(), 0)
