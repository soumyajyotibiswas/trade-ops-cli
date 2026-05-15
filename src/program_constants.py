"""
This module contains all the constants used in the trading program.
"""

import os
from pathlib import Path
from typing import TypeAlias, TypedDict


class IndexDetails(TypedDict):
    """Typed metadata required for index quote, expiry, and order calculations."""

    symbol: str
    weekly_expiry: str
    monthly_expiry: str
    lot_quantity: int
    max_lot_size: int
    max_multiplier: int
    step_size: int
    is_index: bool
    instrument_token: int
    exchange_segment: str
    exchange_segment_fo: str
    exchange_identifier: str


IndexDetailsMap: TypeAlias = dict[str, IndexDetails]


def _read_bool_env(*names: str, default: bool = False) -> bool:
    """Read the first matching boolean environment variable from a name list."""
    for name in names:
        raw_value = os.getenv(name)
        if raw_value is not None:
            return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


PARENT_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = PARENT_DIR / "logs"
DATA_DIR = PARENT_DIR / "data"
SCRIP_MASTER_FILE_NAME = "ScripMaster.csv"
SCRIP_MASTER_FILE_PATH = DATA_DIR / SCRIP_MASTER_FILE_NAME
SCRIP_MASTER_FILE_URL = (
    "https://openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/All"
)
BUFFER_MARGIN = 25000
PICKLE_DATA_AGE = 8  # in hours
DRY_RUN_ORDERS = _read_bool_env(
    "TRADING_PROGRAM_DRY_RUN_ORDERS", "TRADING_PROGRAM_DRY_RUN"
)
INDEX_DETAILS_FNO: IndexDetailsMap = {
    "NIFTY": {
        "symbol": "NIFTY",
        "weekly_expiry": "Tuesday",
        "monthly_expiry": "Tuesday",
        "lot_quantity": 65,
        "max_lot_size": 250,
        "max_multiplier": 5,
        "step_size": 50,
        "is_index": True,
        "instrument_token": 26000,
        "exchange_segment": "nse_cm",
        "exchange_segment_fo": "nse_fo",
        "exchange_identifier": "Nifty 50",
    },
    "BANKNIFTY": {
        "symbol": "BANKNIFTY",
        "weekly_expiry": "Tuesday",
        "monthly_expiry": "Tuesday",
        "lot_quantity": 30,
        "max_lot_size": 150,
        "max_multiplier": 5,
        "step_size": 100,
        "is_index": True,
        "instrument_token": 26009,
        "exchange_segment": "nse_cm",
        "exchange_segment_fo": "nse_fo",
        "exchange_identifier": "Nifty Bank",
    },
    "FINNIFTY": {
        "symbol": "FINNIFTY",
        "weekly_expiry": "Tuesday",
        "monthly_expiry": "Tuesday",
        "lot_quantity": 65,
        "max_lot_size": 250,
        "max_multiplier": 5,
        "step_size": 50,
        "is_index": True,
        "instrument_token": 26037,
        "exchange_segment": "nse_cm",
        "exchange_segment_fo": "nse_fo",
        "exchange_identifier": "Nifty Fin Service",
    },
    "SENSEX": {
        "symbol": "SENSEX",
        "weekly_expiry": "Thursday",
        "monthly_expiry": "Thursday",
        "lot_quantity": 20,
        "max_lot_size": 500,
        "max_multiplier": 5,
        "step_size": 100,
        "is_index": True,
        "instrument_token": 26037,
        "exchange_segment": "bse_cm",
        "exchange_segment_fo": "bse_fo",
        "exchange_identifier": "SENSEX",
    },
    "BANKEX": {
        "symbol": "BANKEX",
        "weekly_expiry": "Thursday",
        "monthly_expiry": "Thursday",
        "lot_quantity": 30,
        "max_lot_size": 450,
        "max_multiplier": 5,
        "step_size": 100,
        "is_index": True,
        "instrument_token": 26037,
        "exchange_segment": "bse_cm",
        "exchange_segment_fo": "bse_fo",
        "exchange_identifier": "BANKEX",
    },
    "MIDCPNifty": {
        "symbol": "MIDCPNifty",
        "weekly_expiry": "Tuesday",
        "monthly_expiry": "Tuesday",
        "lot_quantity": 120,
        "max_lot_size": 230,
        "max_multiplier": 5,
        "step_size": 25,
        "is_index": True,
        "instrument_token": 26000,
        "exchange_segment": "nse_cm",
        "exchange_segment_fo": "nse_fo",
        "exchange_identifier": "MIDCPNifty",
    },
}
EXCHANGE = "N"
EXCHANGE_TYPE = "D"
ORDER_TYPE_BUY = "B"
ORDER_TYPE_SELL = "S"
OPTION_TYPE_CALL = "CE"
OPTION_TYPE_PUT = "PE"
OPTION_CHAIN_DEPTH = 7
IS_INTRA_DAY = True
INDEX_FINAL_DETAILS_FILE_NAME = "index_final_details.file"
INDEX_DETAILS_FILE_NAME = "index_details.file"
INDEX_FINAL_DETAILS_FETCH_INTERVAL = 180  # in seconds
INDEX_DETAILS_FETCH_INTERVAL = 1  # in seconds
QUOTE_DETAILS_FETCH_INTERVAL = 1  # in seconds
MARKET_CLOSE_FETCH_INTERVAL = 1800  # in seconds
MARKET_OPEN_TIME = "09:15:00"
MARKET_CLOSE_TIME = "15:30:00"
MARKET_OPEN_DAYS: list[str] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
HOLIDAY_LIST: list[str] = [
    "20260126",
    "20260303",
    "20260326",
    "20260331",
    "20260403",
    "20260414",
    "20260501",
    "20260528",
    "20260626",
    "20260914",
    "20261002",
    "20261020",
    "20261110",
    "20261124",
    "20261225",
]
ENVIRONMENT = "PROD"  # for kotak
TOKEN_EXPIRY = 86400  # 24 hour set in oAuthTokens in https://napi.kotaksecurities.com/devportal/[MyTradeApp]
