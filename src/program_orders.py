"""
This module contains the Orders class which is used to place buy and sell orders.
"""

# ruff: noqa: E402

import copy
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.program_helpers import (
    disable_loguru_to_devnull,
    run_as_background_thread,
    setup_logging,
    setup_signal_handlers,
)

setup_signal_handlers()

log = setup_logging("program_orders")

from src.program_constants import (
    DRY_RUN_ORDERS,
    INDEX_DETAILS_FNO,
    ORDER_TYPE_BUY,
    ORDER_TYPE_SELL,
)

SUPPORTED_INDEX_SYMBOLS = (
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
    "BANKEX",
    "MIDCPNIFTY",
    "FINNIFTY",
)

POSITION_SYMBOL_INFERENCE = (
    ("FINNIFTY", "FINNIFTY"),
    ("BANKNIFTY", "BANKNIFTY"),
    ("BANKEX", "BANKEX"),
    ("SENSEX", "SENSEX"),
    ("MIDCPNIFTY", "MIDCPNifty"),
    ("NIFTY", "NIFTY"),
)

MAX_QTY_OVERRIDES = {"BANKEX": 900, "MIDCPNifty": 2800}


def _is_neo_client(client: Any) -> bool:
    """Return True when the SDK object looks like a Kotak Neo client."""
    return "NeoWebSocket" in dir(client)


def _is_supported_index_symbol(trading_symbol: str) -> bool:
    """Return True when a trading symbol belongs to a supported index option."""
    return any(symbol in trading_symbol for symbol in SUPPORTED_INDEX_SYMBOLS)


def _positions_data(response: Any) -> list[dict[str, Any]]:
    """Normalize broker position responses into a list of dictionaries."""
    if response is None:
        return []
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict):
        data = response.get("data", [])
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    return []


def _infer_position_symbol(trading_symbol: str) -> str | None:
    """Infer the index key from a broker trading symbol."""
    for marker, index_name in POSITION_SYMBOL_INFERENCE:
        if marker in trading_symbol:
            return index_name
    return None


def _max_order_quantity(
    index_name: str, *, use_floor_division: bool, apply_special_overrides: bool = True
) -> int | float:
    """Return the max quantity per child order for the configured index."""
    if apply_special_overrides and index_name in MAX_QTY_OVERRIDES:
        return MAX_QTY_OVERRIDES[index_name]

    index_config: Mapping[str, Any] = INDEX_DETAILS_FNO.get(index_name, {})
    max_lot_size = cast(int, index_config.get("max_lot_size", 25))
    lot_quantity = cast(int, index_config.get("lot_quantity", 1))
    max_qty = max_lot_size * lot_quantity
    if use_floor_division:
        return max_qty // 10
    return max_qty / 10


def _product_from_intraday(intraday: bool | str) -> str:
    """Map the historical intraday flag into the broker product string."""
    if isinstance(intraday, str):
        return intraday
    return "MIS" if intraday else "NRML"


def _order_summary(order_details: dict[str, Any]) -> str:
    """Return a compact, non-secret order summary for logs."""
    symbol = (
        order_details.get("trading_symbol")
        or order_details.get("ScripCode")
        or order_details.get("ScripName")
        or "NA"
    )
    side = (
        order_details.get("transaction_type") or order_details.get("OrderType") or "NA"
    )
    quantity = order_details.get("quantity") or order_details.get("Qty") or "NA"
    product = order_details.get("product") or order_details.get("IsIntraday") or "NA"
    price = order_details.get("price") or order_details.get("Price") or "NA"
    return f"symbol={symbol} side={side} qty={quantity} product={product} price={price}"


def _dry_run_response(action: str, payload: Any) -> dict[str, Any]:
    """Return a structured dry-run response without calling the broker."""
    log.info("[DRY_RUN] Skipping mutating broker call. action=%s", action)
    log.debug("[DRY_RUN] Payload for action=%s: %s", action, payload)
    return {"dry_run": True, "action": action, "payload": payload}


def _place_order(client: Any, order_details: dict[str, Any]) -> dict[str, Any]:
    """Place an order or return the dry-run payload."""
    if DRY_RUN_ORDERS:
        return _dry_run_response("place_order", order_details)
    return client.place_order(**order_details)


def _cancel_order(client: Any, order_id: str) -> dict[str, Any]:
    """Cancel one order or return the dry-run payload."""
    if DRY_RUN_ORDERS:
        return _dry_run_response("cancel_order", {"order_id": order_id})
    return client.cancel_order(order_id=order_id)


def _cancel_bulk_order(
    client: Any, cancel_order_list: list[dict[str, Any]]
) -> dict[str, Any]:
    """Cancel a 5paisa bulk order list or return the dry-run payload."""
    if DRY_RUN_ORDERS:
        return _dry_run_response("cancel_bulk_order", cancel_order_list)
    return client.cancel_bulk_order(cancel_order_list)


class Orders:
    """
    Represents a class for managing trading orders.

    This class provides methods for placing buy and sell orders, as well as canceling open orders.

    Args:
        client (FivePaisaClient): An instance of the FivePaisaClient class.

    Attributes:
        client (FivePaisaClient): An instance of the FivePaisaClient class.

    """

    def __init__(self, client: Any) -> None:
        """Store the broker SDK client used for order operations."""
        self.client = client

    def place_buy_order_bulk(
        self, bulk_order: list[list[dict[str, Any]]], intraday: bool | str = True
    ) -> None:
        """
        Places multiple buy orders in bulk.

        Args:
            bulk_order: A list of lists, where each inner list contains
                dictionaries representing individual buy orders.
            intraday: Specifies whether the orders are intraday orders or not.
                Defaults to True.

        Raises:
            Exception: If there is an error while placing the bulk order.

        """

        def place_buy_order_bulk_t(order_in: dict[str, Any]) -> None:
            """Submit one prepared buy order payload in a background thread."""
            try:
                disable_loguru_to_devnull()
                log.info("Submitting buy order. %s", _order_summary(order_in))
                log.debug("Buy order payload: %s", order_in)
                response = _place_order(self.client, order_in)
                log.info(
                    "Buy order submitted. %s response=%s",
                    _order_summary(order_in),
                    response,
                )

            except Exception as e:
                log.exception(
                    "Failed to submit buy order. %s error=%s",
                    _order_summary(order_in),
                    e,
                )

        try:
            for order in bulk_order:
                order_in = order[0]

                max_qty = _max_order_quantity(
                    order_in["index"], use_floor_division=True
                )

                del order_in["index"]
                del order_in["tag"]
                order_in["product"] = _product_from_intraday(intraday)

                qty_remaining = int(order_in["quantity"])
                order_count = 0
                while qty_remaining > 0:
                    qty_to_order = min(qty_remaining, max_qty)
                    order_copy = copy.deepcopy(order_in)
                    order_copy["quantity"] = str(qty_to_order)
                    run_as_background_thread(place_buy_order_bulk_t, order_copy)
                    qty_remaining -= qty_to_order
                    order_count += 1
                    if order_count % 10 == 0:
                        time.sleep(0.75)

        except Exception as e:
            log.exception("Error preparing bulk buy orders: %s", e)

    def place_sell_order_all(self, intraday: bool | str = "MIS") -> None:
        """
        Processes all open positions and places exit IOC limit orders using live Kotak LTP.
        Uses token and exchange segment directly from client.positions().
        """

        def get_live_ltp_from_kotak_position(position: dict[str, Any]) -> float:
            """Fetch live Kotak LTP for the instrument token in a position row."""
            response = self.client.quotes(
                instrument_tokens=[
                    {
                        "instrument_token": str(position["tok"]),
                        "exchange_segment": position["exSeg"],
                    }
                ],
                quote_type="ltp",
            )

            if not response or not isinstance(response, list):
                raise ValueError(
                    f"Unexpected quote response for {position['trdSym']}: {response}"
                )

            row = response[0]
            ltp = row.get("ltp")
            if ltp in (None, "", "0", 0):
                raise ValueError(
                    f"No valid ltp returned for {position['trdSym']}: {response}"
                )

            return float(ltp)

        def place_sell_order_t(order_details: dict[str, Any]) -> None:
            """
            Function to place an individual order. This function is designed to be run in a background thread.
            """
            try:
                disable_loguru_to_devnull()
                response = _place_order(self.client, order_details)
                log.info(
                    "Exit order submitted. %s response=%s",
                    _order_summary(order_details),
                    response,
                )
            except Exception as e:
                log.exception(
                    "Exit order placement failed. %s error=%s",
                    _order_summary(order_details),
                    e,
                )

        try:
            if _is_neo_client(self.client):
                open_positions = _positions_data(self.client.positions())

                for position in open_positions:
                    trading_symbol = position.get("trdSym", "")

                    if not _is_supported_index_symbol(trading_symbol):
                        continue

                    flBuyQty = int(position.get("flBuyQty", 0))
                    flSellQty = int(position.get("flSellQty", 0))
                    qty_remaining = flBuyQty - flSellQty

                    if qty_remaining == 0 or position.get("prod") == "NRML":
                        continue

                    position_sym = position.get("sym") or _infer_position_symbol(
                        trading_symbol
                    )
                    if not position_sym:
                        continue

                    max_qty = _max_order_quantity(position_sym, use_floor_division=True)

                    try:
                        live_ltp = get_live_ltp_from_kotak_position(position)
                    except Exception as e:
                        log.warning(
                            "Skipping %s because live quote lookup failed: %s",
                            trading_symbol,
                            e,
                        )
                        continue

                    sell_price = str(int(round(live_ltp * 0.995, 0)))
                    buy_price = str(int(round(live_ltp * 1.005, 0)))

                    order_count = 0
                    while qty_remaining > 0:
                        qty_to_order = min(qty_remaining, max_qty)
                        order_details = copy.deepcopy(
                            {
                                "exchange_segment": position["exSeg"],
                                "product": position["prod"],
                                "price": sell_price,
                                "order_type": "L",
                                "quantity": str(qty_to_order),
                                "validity": "DAY",
                                "trading_symbol": trading_symbol,
                                "transaction_type": "S",
                            }
                        )
                        qty_remaining -= qty_to_order
                        log.info(
                            "Placing IOC sell. Symbol=%s Token=%s LTP=%s Price=%s Qty=%s",
                            trading_symbol,
                            position["tok"],
                            live_ltp,
                            sell_price,
                            qty_to_order,
                        )
                        run_as_background_thread(place_sell_order_t, order_details)
                        order_count += 1
                        if order_count % 10 == 0:
                            time.sleep(0.75)

                    while qty_remaining < 0:
                        qty_to_order = min(abs(qty_remaining), max_qty)
                        order_details = copy.deepcopy(
                            {
                                "exchange_segment": position["exSeg"],
                                "product": position["prod"],
                                "price": buy_price,
                                "order_type": "L",
                                "quantity": str(qty_to_order),
                                "validity": "DAY",
                                "trading_symbol": trading_symbol,
                                "transaction_type": "B",
                            }
                        )
                        qty_remaining += qty_to_order
                        log.info(
                            "Placing IOC buy-cover. Symbol=%s Token=%s LTP=%s Price=%s Qty=%s",
                            trading_symbol,
                            position["tok"],
                            live_ltp,
                            buy_price,
                            qty_to_order,
                        )
                        run_as_background_thread(place_sell_order_t, order_details)
                        order_count += 1
                        if order_count % 10 == 0:
                            time.sleep(0.75)
                return

            open_positions = _positions_data(self.client.positions())

            for position in open_positions:
                if position["BuyQty"] != position["SellQty"] or position["NetQty"] != 0:
                    qty_remaining = position["NetQty"]
                    max_qty = _max_order_quantity(
                        position["ScripName"].split()[0],
                        use_floor_division=False,
                        apply_special_overrides=False,
                    )

                    while qty_remaining > 0:
                        qty_to_order = min(qty_remaining, max_qty)
                        order_details = {
                            "OrderType": ORDER_TYPE_SELL,
                            "Exchange": position["Exch"],
                            "ExchangeType": position["ExchType"],
                            "ScripCode": position["ScripCode"],
                            "Qty": int(qty_to_order),
                            "IsIntraday": intraday,
                            "Price": 0,
                        }
                        qty_remaining -= qty_to_order
                        run_as_background_thread(place_sell_order_t, order_details)

                    while qty_remaining < 0:
                        qty_to_order = min(qty_remaining * -1, max_qty)
                        order_details = {
                            "OrderType": ORDER_TYPE_BUY,
                            "Exchange": position["Exch"],
                            "ExchangeType": position["ExchType"],
                            "ScripCode": position["ScripCode"],
                            "Qty": int(qty_to_order),
                            "IsIntraday": intraday,
                            "Price": 0,
                        }
                        qty_remaining += qty_to_order
                        run_as_background_thread(place_sell_order_t, order_details)

        except Exception as e:
            log.exception("Error preparing sell orders: %s", e)

    def cancel_all_open_orders(self) -> None:
        """
        Cancels all open orders.

        This method retrieves the order book and cancels all open orders that meet the following criteria:
        - Have an 'ExchOrderID' field
        - Have a 'TradedQty' field
        - Have a 'ScripCode' field
        - Have an 'OrderStatus' field with a value of 'Pending'

        The method will attempt to cancel the orders up to a maximum number of attempts defined by 'max_attempts'.
        If the order cancellation is successful or there are no open orders to cancel, the method will return.

        Returns:
            None
        """
        try:
            if _is_neo_client(self.client):
                open_orders = self.client.order_report()
                for order in open_orders.get("data", []):
                    if order.get("ordSt") == "open":
                        order_id = order.get("nOrdNo")
                        if order_id:
                            cancel_response = _cancel_order(self.client, order_id)
                            log.info(
                                "Cancelled open Neo order. order_id=%s response=%s",
                                order_id,
                                cancel_response,
                            )
                return

            response = self.client.order_book()
            max_attempts = 2
            total_attempts = 0
            while total_attempts < max_attempts and response is not None:
                cancel_order_list = [
                    {"ExchOrderID": item["ExchOrderID"]}
                    for item in response
                    if "ExchOrderID" in item
                    and "TradedQty" in item
                    and "ScripCode" in item
                    and item.get("OrderStatus") == "Pending"
                ]
                if len(cancel_order_list) == 0:
                    return
                cancel_response = _cancel_bulk_order(self.client, cancel_order_list)
                log.info(
                    "Cancelled pending 5paisa orders. count=%d response=%s",
                    len(cancel_order_list),
                    cancel_response,
                )
                response = self.client.order_book()
                total_attempts += 1
        except Exception as e:
            log.exception("Error cancelling open orders: %s", e)

    def get_open_positions(self) -> list[dict[str, Any]]:
        """
        Retrieves the open positions.

        Returns:
            list[dict[str, Any]]: A list of dictionaries representing the open positions.
        """
        try:
            open_positions = _positions_data(self.client.positions())
            display_positions = []
            for position in open_positions:
                if position["BuyQty"] != position["SellQty"] or position["NetQty"] != 0:
                    new_entry = {
                        "Exchange": position["Exch"],
                        "ExchangeType": position["ExchType"],
                        "ScripName": position["ScripName"],
                        "ScripCode": position["ScripCode"],
                        "Qty": position["NetQty"],
                    }
                    display_positions.append(new_entry)
            return display_positions
        except Exception as e:
            log.warning("Error retrieving open positions: %s", e)
            return []

    def count_completed_orders(self) -> int:
        """
        Counts the number of completed orders.

        Returns:
            int: The number of completed orders.
        """
        try:
            if _is_neo_client(self.client):
                response = self.client.order_report()
                count = len(
                    [
                        item
                        for item in response.get("data", [])
                        if item.get("stat") == "complete" and item.get("trnsTp") == "B"
                    ]
                )
                return count
            response = self.client.order_book()
            count = len(
                [item for item in response if item.get("OrderStatus") == "Complete"]
            )
            return count
        except Exception as e:
            log.warning("Error counting completed orders: %s", e)
            return 0
