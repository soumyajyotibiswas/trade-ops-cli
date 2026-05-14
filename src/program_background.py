# pylint: disable=wrong-import-position
# pylint: disable=too-many-nested-blocks
# pylint: disable=broad-exception-caught
# pylint: disable=line-too-long
# pylint: disable=consider-using-f-string
# ruff: noqa: E402

"""
This module contains the `ProgramBackground` class responsible for running background tasks related to the trading program.
"""

import sys
import time
from collections.abc import Mapping
from datetime import date as dt_date
from datetime import datetime
from datetime import time as dt_time
from datetime import timedelta
from math import floor
from pathlib import Path
from typing import Any, cast

from pandas import DataFrame

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.program_client_profile import ClientProfile
from src.program_constants import (
    DATA_DIR,
    INDEX_DETAILS_FNO,
    OPTION_TYPE_CALL,
    OPTION_TYPE_PUT,
)
from src.program_helpers import (
    create_empty_file_if_not_exists,
    create_scrip_code_match,
    disable_loguru_to_devnull,
    dump_data_to_file,
    fetch_scrip_code_from_csv,
    read_data_from_file,
    run_as_background_thread,
    setup_logging,
    setup_signal_handlers,
)
from src.program_orders import Orders
from src.program_quotes import Quotes

setup_signal_handlers()

log = setup_logging("program_background")
BACKGROUND_ERROR_LOG_INTERVAL_SECONDS = 60.0
_LAST_BACKGROUND_ERROR_LOGGED_AT: dict[str, float] = {}

WEEKLY_EXPIRY_MONTH_CODES = {
    1: "1",
    2: "2",
    3: "3",
    4: "4",
    5: "5",
    6: "6",
    7: "7",
    8: "8",
    9: "9",
    10: "O",
    11: "N",
    12: "D",
}


def _is_neo_client(client: Any) -> bool:
    return "NeoWebSocket" in dir(client)


def _log_background_exception(task_name: str, exc: Exception) -> None:
    now = time.monotonic()
    last_logged_at = _LAST_BACKGROUND_ERROR_LOGGED_AT.get(task_name, 0.0)
    if now - last_logged_at < BACKGROUND_ERROR_LOG_INTERVAL_SECONDS:
        return
    _LAST_BACKGROUND_ERROR_LOGGED_AT[task_name] = now
    log.warning("Background task failed; retrying. task=%s error=%s", task_name, exc)


def _ist_timestamp() -> str:
    return "%s +5:30" % (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _is_last_week_expiry(expiry_date: dt_date) -> bool:
    return (expiry_date + timedelta(days=7)).month != expiry_date.month


def _transform_symbol(symbol_value: str) -> str:
    parts = symbol_value.split()
    symbol = parts[0]
    day = parts[1]
    month = parts[2]
    year = parts[3]
    option_type = parts[4]
    strike_price = parts[5].replace(".00", "")
    expiry_date = datetime.strptime(f"{day} {month} {year}", "%d %b %Y").date()

    if _is_last_week_expiry(expiry_date):
        return (
            f"{symbol}{year[-2:]}{expiry_date.strftime('%b').upper()}"
            f"{strike_price}{option_type}"
        )

    month_code = WEEKLY_EXPIRY_MONTH_CODES[expiry_date.month]
    return f"{symbol}{year[-2:]}{month_code}{day.zfill(2)}{strike_price}{option_type}"


class ProgramBackground:
    """
    Class responsible for running background tasks related to the trading program.

    Args:
        client (FivePaisaClient): The client object used for interacting with the trading platform.
        client_key (str): The key associated with the client.

    Attributes:
        client (FivePaisaClient): The client object used for interacting with the trading platform.
        client_key (str): The key associated with the client.
        quotes (Quotes): The quotes object used for retrieving index quotes.
        client_profile (ClientProfile): The client profile object used for retrieving client information.
        client_margin_file_path (str): The file path for storing the client's available margin.

    """

    def __init__(
        self,
        client: Any,
        client_key: str,
        df_pd: DataFrame,
        additional_client: Any,
    ) -> None:
        self.client = client
        self.additional_client = additional_client  # trip wire
        self.client_key = client_key.lower()
        log.info("Initialising background tasks for account=%s", self.client_key)
        self.quotes = Quotes(client, INDEX_DETAILS_FNO)
        if _is_neo_client(self.client):  # trip wire
            self.client_profile = ClientProfile(self.client)
        else:
            self.client_profile = ClientProfile(self.additional_client)
        self.client_path = None
        self.client_dir_path = DATA_DIR / self.client_key
        self.client_margin_file_path = self.client_dir_path / "client_margin.json"
        self.completed_buy_order_count_file_path = (
            self.client_dir_path / "completed_buy_order_count.json"
        )
        self.__create_client_directory(self.client_dir_path)
        self.client_orders = Orders(self.client)
        self.df_pd = df_pd

    def __create_client_directory(self, path_to_create: Path) -> None:
        """
        Create a directory for the client data.

        Returns:
            None
        """
        path_to_create.mkdir(parents=True, exist_ok=True)

    def store_client_open_positions_to_file(self) -> None:
        """
        Stores the client's open positions to a file.

        This method continuously retrieves the client's open positions and stores them in a file.
        The open positions are fetched using the `get_open_positions` method from the `client_profile` object.
        The open positions are then dumped to a file along with additional information such as the timestamp.

        Raises:
            Exception: If an error occurs while storing the client's open positions.

        """
        try:

            def store_client_open_positions_to_file_t() -> None:
                """
                Stores the client's open positions to a file.

                This function continuously retrieves the client's open positions,
                stores them in a dictionary, and dumps the data to a file.
                The process repeats every 1 second until stopped.

                Note: This function assumes the existence of certain helper functions
                such as `#disable_loguru_to_devnull()`, `#restore_loguru()`,
                `create_empty_file_if_not_exists()`, and `dump_data_to_file()`.

                Returns:
                    None
                """
                # log.info("Storing client open positions to file...")
                while True:
                    try:
                        time.sleep(2)
                        disable_loguru_to_devnull()
                        open_positions = self.client_orders.get_open_positions()
                        # restore_loguru()
                        open_positions_file_path = (
                            self.client_dir_path / "open_positions.json"
                        )
                        create_empty_file_if_not_exists(open_positions_file_path)
                        data_to_dump = {
                            "client": self.client_key,
                            "open_positions": open_positions,
                            "timestamp": _ist_timestamp(),
                        }
                        if data_to_dump != {}:
                            dump_data_to_file(data_to_dump, open_positions_file_path)
                    except Exception as e:
                        _log_background_exception(
                            "store_client_open_positions_to_file", e
                        )
                        time.sleep(2)

            run_as_background_thread(store_client_open_positions_to_file_t)
        except Exception as e:
            log.exception("Failed to start open-position background task: %s", e)
            sys.exit(1)

    def store_index_quotes_to_file(self) -> None:
        """
        Stores index quotes to a file.

        This method continuously retrieves quotes for each index in `INDEX_DETAILS_FNO` and stores them in separate JSON files.
        The quotes are fetched using the `get_ltp_index` method from the `quotes` object.
        The quotes are then dumped to a file along with additional information such as the current week's expiry date and timestamp.

        Raises:
            Exception: If an error occurs while storing the index quotes.

        """
        try:

            def store_index_quotes_to_file_t(index_key: str) -> None:
                """
                Stores index quotes to a file.

                Args:
                    index_key (str): The key of the index for which quotes are to be stored.

                Returns:
                    None
                """
                # log.info("Storing index quotes to file...")
                while True:
                    try:
                        time.sleep(2)
                        disable_loguru_to_devnull()
                        # log.info("Getting quote for index: %s", index_key)
                        index_quote = self.quotes.get_ltp_index(index_key)
                        # log.info("Index quote for %s: %s", index_key, index_quote)
                        # restore_loguru()
                        index_quote_file_path = (
                            self.client_dir_path / f"{index_key}.json"
                        )
                        create_empty_file_if_not_exists(index_quote_file_path)
                        current_week_expiry_date = (
                            self.quotes.get_current_week_expiry_date(
                                index_key=index_key
                            )
                        )
                        data_to_dump = {
                            "index": index_key,
                            "quote": index_quote,
                            "current_week_expiry_date": current_week_expiry_date,
                            "timestamp": _ist_timestamp(),
                        }
                        # log.info("Dumping data to file: %s.json", index_key)
                        if data_to_dump != {}:
                            dump_data_to_file(data_to_dump, index_quote_file_path)
                        # log.info("Data dumped to file: %s.json", index_key)
                    except Exception as e:
                        _log_background_exception(
                            f"store_index_quotes_to_file:{index_key}", e
                        )
                        time.sleep(2)

            for index_key in INDEX_DETAILS_FNO:
                run_as_background_thread(store_index_quotes_to_file_t, index_key)
        except Exception as e:
            log.exception("Failed to start index-quote background tasks: %s", e)
            sys.exit(1)

    def store_client_margin_to_file(self) -> None:
        """
        Stores the client's available margin to a file.

        This method continuously retrieves the client's available margin,
        and stores it along with the current timestamp in a file.

        Raises:
            Exception: If an error occurs while storing the client margin to file.
        """
        try:

            def store_client_margin_to_file_t() -> None:
                """
                Stores the client's available margin to a file at regular intervals.

                This function continuously retrieves the client's available margin,
                stores it along with the current timestamp in a dictionary, and dumps
                the data to a file. The process repeats every 1 second until stopped.

                Note: This function assumes the existence of certain helper functions
                such as `#disable_loguru_to_devnull()`, `#restore_loguru()`,
                `create_empty_file_if_not_exists()`, and `dump_data_to_file()`.

                Returns:
                    None
                """
                # log.info("Storing client margin to file...")
                while True:
                    try:
                        time.sleep(2)
                        disable_loguru_to_devnull()
                        client_margin = (
                            self.client_profile.get_client_available_margin()
                        )
                        # log.info("Client margin: %s", client_margin)
                        # restore_loguru()
                        margin_file_path = self.client_margin_file_path
                        create_empty_file_if_not_exists(margin_file_path)
                        data_to_dump = {
                            "client": self.client_key,
                            "available_margin": client_margin,
                            "timestamp": _ist_timestamp(),
                        }
                        if data_to_dump != {}:
                            # log.info("Dumping data to file: %s", margin_file_path)
                            dump_data_to_file(data_to_dump, margin_file_path)
                            # log.info("Data dumped to file: %s", margin_file_path)
                    except Exception as e:
                        _log_background_exception("store_client_margin_to_file", e)
                        time.sleep(2)

            run_as_background_thread(store_client_margin_to_file_t)
        except Exception as e:
            log.exception("Failed to start client-margin background task: %s", e)
            sys.exit(1)

    def store_completed_buy_order_count_to_file(self) -> None:
        """
        Stores the client's available margin to a file.

        This method continuously retrieves the client's available margin,
        and stores it along with the current timestamp in a file.

        Raises:
            Exception: If an error occurs while storing the client margin to file.
        """
        try:

            def store_completed_buy_order_count_to_file_t() -> None:
                """
                Stores the client's available margin to a file at regular intervals.

                This function continuously retrieves the client's available margin,
                stores it along with the current timestamp in a dictionary, and dumps
                the data to a file. The process repeats every 1 second until stopped.

                Note: This function assumes the existence of certain helper functions
                such as `#disable_loguru_to_devnull()`, `#restore_loguru()`,
                `create_empty_file_if_not_exists()`, and `dump_data_to_file()`.

                Returns:
                    None
                """
                # log.info("Storing client margin to file...")
                while True:
                    try:
                        time.sleep(2)
                        disable_loguru_to_devnull()
                        count = self.client_profile.get_completed_buy_order_count()
                        # log.info("Completed buy order count: %s", count)
                        # restore_loguru()
                        completed_buy_order_count_file_path = (
                            self.completed_buy_order_count_file_path
                        )
                        create_empty_file_if_not_exists(
                            completed_buy_order_count_file_path
                        )
                        data_to_dump = {
                            "completed_buy_order_count": count,
                            "timestamp": _ist_timestamp(),
                        }
                        # log.info(
                        #     "Dumping data to file: %s",
                        #     completed_buy_order_count_file_path,
                        # )
                        if data_to_dump != {}:
                            dump_data_to_file(
                                data_to_dump, completed_buy_order_count_file_path
                            )
                        # log.info(
                        #     "Data dumped to file: %s",
                        #     completed_buy_order_count_file_path,
                        # )
                    except Exception as e:
                        _log_background_exception(
                            "store_completed_buy_order_count_to_file", e
                        )
                        time.sleep(2)

            run_as_background_thread(store_completed_buy_order_count_to_file_t)
        except Exception as e:
            log.exception("Failed to start completed-buy-count background task: %s", e)
            sys.exit(1)

    def store_index_option_quotes_to_file(self) -> None:
        """
        Stores index option quotes to a file.

        This method retrieves index option quotes, calculates option details, and stores them in a file.
        It runs as a background thread for each index key in the INDEX_DETAILS_FNO dictionary.

        Returns:
            None
        """
        try:

            def create_option_details(
                index_key: str, expiry: str, strike_price_list: list[str]
            ) -> list[dict[str, Any]]:
                option_details_map = []
                log.debug("Creating option details for index=%s", index_key)
                for strike_price in strike_price_list:
                    for option_type in [OPTION_TYPE_CALL, OPTION_TYPE_PUT]:
                        symbol = create_scrip_code_match(
                            index_key, expiry, option_type, strike_price
                        )
                        option_details_map.append(
                            {
                                "Exch": (
                                    "B" if index_key in ("SENSEX", "BANKEX") else "N"
                                ),
                                "ExchType": "D",
                                "Symbol": symbol,
                                "Expiry": datetime.strptime(
                                    expiry, "%Y-%m-%d"
                                ).strftime("%Y%m%d"),
                                "StrikePrice": f"{float(strike_price):.0f}",
                                "OptionType": option_type,
                            }
                        )
                return option_details_map

            def create_options_map(
                option_details_map: list[dict[str, Any]],
                option_details: dict[str, Any],
                client_margin: float,
            ) -> list[dict[str, Any]]:
                log.debug(
                    "Creating options map. client_margin=%s requested=%d returned=%d",
                    client_margin,
                    len(option_details_map),
                    len(option_details.get("Data", [])),
                )
                options_map = []
                for detail_map, detail in zip(
                    option_details_map, option_details["Data"]
                ):
                    # log.info("Detail map: %s", detail_map)
                    # log.info("Detail: %s", detail)
                    index_base = detail_map["Symbol"].split()[0]
                    # log.info("Index base: %s", index_base)
                    if "LastRate" in detail and detail["LastRate"] > 0:
                        # Calculate how many units can be bought with the available margin
                        # log.info(
                        #     "Last rate found for %s: %s",
                        #     detail_map["Symbol"],
                        #     detail["LastRate"],
                        # )
                        index_config: Mapping[str, Any] = INDEX_DETAILS_FNO.get(
                            index_base, {}
                        )
                        lot_quantity = cast(int, index_config.get("lot_quantity", 1))
                        max_lot_size = cast(int, index_config.get("max_lot_size", 1))
                        last_rate = cast(float, detail["LastRate"])
                        now = datetime.now().time()
                        # if now > dt_time(14, 55) or now < dt_time(7, 0):
                        #     return 10000.0
                        if (
                            last_rate == 0
                            or last_rate is None
                            or last_rate == ""
                            or last_rate == 0.0
                            or (
                                (last_rate < 5)
                                and (now > dt_time(9, 14) and now < dt_time(15, 55))
                            )  # or (
                            # (last_rate < 10)
                            # and (now > dt_time(9, 14) and now < dt_time(14, 55))
                            # )
                        ):
                            last_rate = 100000000000000  # effectively infinity
                        # log.info("Last rate: %s, for index %s", last_rate, index_base)
                        units_can_buy = floor(client_margin / last_rate)

                        # Calculate how many full lots can be bought
                        # log.info(
                        #     "Units can buy: %s, for index %s", units_can_buy, index_base
                        # )
                        # log.info(
                        #     "Lot quantity: %s, for index %s", lot_quantity, index_base
                        # )
                        full_lots = units_can_buy // lot_quantity

                        # Calculate the quantity to purchase as full lots times the lot size
                        qty_to_purchase = full_lots * lot_quantity

                        # Fetch the scrip code from the CSV file
                        to_match = detail_map["Symbol"]
                        # log.info("To match: %s", to_match)
                        if "midcpnifty" in to_match.lower():
                            to_match = to_match.replace("MIDCPNifty", "MIDCPNIFTY")
                        scrip_code = int(
                            fetch_scrip_code_from_csv(self.df_pd, to_match.upper())
                        )
                        # log.info("For match %s Scrip code: %s", to_match, scrip_code)
                        max_qty_per_order = (lot_quantity * max_lot_size) // 10
                        # log.info("Max qty per order: %s", max_qty_per_order)

                        if index_base in ["BANKEX"]:
                            max_qty_per_order = 9000
                        if index_base in ["MIDCPNifty"]:
                            max_qty_per_order = 2800

                        max_orders_per_list = (
                            lot_quantity * max_lot_size
                        ) // max_qty_per_order

                        transformed_symbol = _transform_symbol(to_match)
                        # Initialize a list to store the bulk orders
                        bulk_order_list: list[list[dict[str, Any]]] = []
                        bulk_order: list[dict[str, Any]] = []
                        # Create bulk order dicts and distribute them into lists
                        qty_to_purchase_stat = qty_to_purchase
                        while qty_to_purchase > 0:
                            now = datetime.now().time()
                            order_qty = qty_to_purchase_stat
                            bulk_order.append(
                                {
                                    "exchange_segment": (
                                        "bse_fo"
                                        if index_base in ("SENSEX", "BANKEX")
                                        else "nse_fo"
                                    ),
                                    "product": "MIS",
                                    "price": (
                                        # change here
                                        str(round(detail["LastRate"] * 1.05, 0))
                                        if now > dt_time(9, 30)
                                        and now < dt_time(15, 55)
                                        else str(round(detail["LastRate"] * 1.05, 0))
                                    ),
                                    "order_type": (
                                        # change here
                                        # "L"
                                        # if index_base in ("MIDCPNifty", "BANKEX")
                                        # else "MKT"
                                        "L"
                                        if now > dt_time(9, 30)
                                        and now < dt_time(15, 55)
                                        else "L"
                                    ),
                                    "quantity": str(order_qty),
                                    "validity": "DAY",
                                    "trading_symbol": transformed_symbol,
                                    "transaction_type": "B",
                                    "tag": "trading_program_api",
                                    "index": index_base,
                                }
                            )

                            if (
                                len(bulk_order) == max_orders_per_list
                                or qty_to_purchase <= max_qty_per_order
                            ):
                                # Once the list reaches its max size or remaining qty is less than max per order, add to bulk_order_list
                                bulk_order_list.append(bulk_order)
                                bulk_order = []  # Reset the bulk order list for the next batch

                            qty_to_purchase -= (
                                order_qty  # Decrease the quantity left to purchase
                            )

                        # Add the remaining orders if any
                        if bulk_order:
                            bulk_order_list.append(bulk_order)

                        options_map.append(
                            {
                                "Index_Symbol": detail_map["Symbol"],
                                "Option_Symbol": detail["Symbol"],
                                "Expiry": detail_map["Expiry"],
                                "StrikePrice": detail_map["StrikePrice"],
                                "ScripCode": scrip_code,
                                "OptionType": detail_map["OptionType"],
                                "High": detail["High"],
                                "Low": detail["Low"],
                                "LastRate": detail["LastRate"],
                                "Quantity_to_Purchase": qty_to_purchase_stat,
                                "Client_Margin": client_margin,
                                "BulkOrderList": bulk_order_list,
                                "timestamp": _ist_timestamp(),
                            }
                        )
                # log.info("Options map: %s", options_map)
                return options_map

            def store_index_option_quotes_to_file_t(index_key: str) -> None:
                # log.info("Storing index option quotes to file for index: %s", index_key)
                while True:
                    try:
                        time.sleep(1)
                        index_quote_file = self.client_dir_path / f"{index_key}.json"
                        file_contents_index_quote = read_data_from_file(
                            index_quote_file
                        )
                        if file_contents_index_quote is None:
                            # log.info("Index quote file not found: %s", index_quote_file)
                            continue

                        index_quote = file_contents_index_quote["quote"]
                        expiry = file_contents_index_quote["current_week_expiry_date"]
                        option_strike_price_list = (
                            self.quotes.get_opt_strike_price_list(
                                index_key, index_quote
                            )
                        )
                        if not option_strike_price_list:
                            # log.info("No option strike prices found for %s.", index_key)
                            continue

                        file_contents_client_margin = read_data_from_file(
                            self.client_margin_file_path
                        )
                        if file_contents_client_margin is None:
                            # log.info(
                            #     "Client margin file not found: %s",
                            #     self.client_margin_file_path,
                            # )
                            continue

                        client_margin = file_contents_client_margin["available_margin"]
                        option_details_map = create_option_details(
                            index_key, expiry, option_strike_price_list
                        )
                        # log.info(
                        #     "For index %s Option details map: %s",
                        #     index_key,
                        #     option_details_map,
                        # )
                        # disable_loguru_to_devnull()
                        option_details = self.quotes.get_ltp_for_opt_strike_price(
                            optional_list=option_details_map
                        )
                        # log.info(
                        #     "Option details quotes for index %s: %s",
                        #     index_key,
                        #     option_details,
                        # )
                        options_map = create_options_map(
                            option_details_map, option_details, client_margin
                        )
                        # restore_loguru()

                        option_details_file_path = (
                            self.client_dir_path / f"{index_key}_options.json"
                        )
                        create_empty_file_if_not_exists(option_details_file_path)
                        # log.info("Dumping data to file: %s", option_details_file_path)
                        if options_map != []:
                            dump_data_to_file(options_map, option_details_file_path)
                    except Exception as e:
                        _log_background_exception(
                            f"store_index_option_quotes_to_file:{index_key}", e
                        )
                        time.sleep(1)

            for index_key in INDEX_DETAILS_FNO:
                run_as_background_thread(store_index_option_quotes_to_file_t, index_key)
        except Exception as e:
            log.exception("Failed to start index-option background tasks: %s", e)

    def store_symbol_price_lookup_to_file(self) -> None:
        """
        Continuously updates a merged trading_symbol -> reference price lookup
        from all *_options.json files in the client directory.

        Once a symbol appears, it is retained in the lookup file.
        Only its values are updated on subsequent refreshes.
        """
        try:

            def store_symbol_price_lookup_to_file_t() -> None:
                lookup_file_path = self.client_dir_path / "symbol_price_lookup.json"

                create_empty_file_if_not_exists(lookup_file_path)

                while True:
                    try:
                        time.sleep(1)

                        existing_lookup = read_data_from_file(lookup_file_path)
                        if not isinstance(existing_lookup, dict):
                            existing_lookup = {}

                        for file_path in self.client_dir_path.glob("*_options.json"):
                            file_contents = read_data_from_file(file_path)
                            if not file_contents:
                                continue

                            index_name = file_path.name.replace("_options.json", "")

                            for item in file_contents:
                                item_timestamp = item.get("timestamp")

                                for bulk_group in item.get("BulkOrderList", []):
                                    for order in bulk_group:
                                        trading_symbol = order.get("trading_symbol")
                                        price = order.get("price")

                                        if not trading_symbol or price in (
                                            None,
                                            "",
                                            "0",
                                            0,
                                        ):
                                            continue

                                        try:
                                            existing_lookup[trading_symbol] = {
                                                "price": float(price),
                                                "index": index_name,
                                                "source_file": file_path.name,
                                                "timestamp": item_timestamp,
                                                "updated_at_epoch": time.time(),
                                                "stale": False,
                                            }
                                        except (TypeError, ValueError):
                                            continue

                        dump_data_to_file(existing_lookup, lookup_file_path)

                    except Exception as e:
                        _log_background_exception(
                            "store_symbol_price_lookup_to_file", e
                        )
                        time.sleep(1)

            run_as_background_thread(store_symbol_price_lookup_to_file_t)

        except Exception as e:
            log.exception("Failed to start symbol-price lookup task: %s", e)
            sys.exit(1)

    def start_background_client_tasks(self) -> None:
        """
        Starts background tasks for the client.

        This method starts the following background tasks for the client:
        - Storing index quotes to a file
        - Storing client margin to a file
        - Storing index option quotes to a file

        Returns:
            None

        Raises:
            Exception: If an error occurs while starting the background tasks.
        """
        try:
            if _is_neo_client(self.client):  # trip wire
                self.store_client_margin_to_file()
                log.info(
                    "Started Neo background task set for account=%s", self.client_key
                )
            else:
                self.store_index_quotes_to_file()
                self.store_client_margin_to_file()
                self.store_completed_buy_order_count_to_file()
                self.store_index_option_quotes_to_file()
                self.store_client_open_positions_to_file()
                self.store_symbol_price_lookup_to_file()
                log.info(
                    "Started market-data background task set for account=%s",
                    self.client_key,
                )
        except Exception as e:
            log.exception(
                "Error starting background tasks for %s: %s", self.client_key, e
            )
            sys.exit(1)
