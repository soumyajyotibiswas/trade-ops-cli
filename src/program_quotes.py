# ruff: noqa: E402

"""
This module contains the Quotes class, which is responsible for fetching quotes for a given index and expiry date, as well as calculating the nearest expiry date based on the index-specific rules.
"""

import datetime
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol, cast

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.program_constants import HOLIDAY_LIST, OPTION_CHAIN_DEPTH, IndexDetails
from src.program_helpers import setup_logging

log = setup_logging("program_quotes")

WEEKDAYS = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6,
}

DEFAULT_OPTION_FEED_PAYLOAD: list[dict[str, Any]] = [{"Random": "Data"}]


class QuoteClient(Protocol):
    """Protocol for the subset of 5paisa client methods used by Quotes."""

    def get_expiry(self, exchange: str, index: str) -> dict[str, Any] | None:
        """Return expiry/last-rate data from the broker client."""
        ...

    def fetch_market_feed(self, payload: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Return market feed data from the broker client."""
        ...


class Quotes:
    """
    A class that provides methods to retrieve quotes and expiry dates for trading programs.

    Args:
        client (FivePaisaClient): The client instance for interacting with the trading platform.
        index_details: A dictionary containing index-specific details.

    Attributes:
        client (FivePaisaClient): The client instance for interacting with the trading platform.
        index_details: A dictionary containing index-specific details.
    """

    def __init__(
        self, client: QuoteClient, index_details: Mapping[str, IndexDetails]
    ) -> None:
        """Store the quote client and index metadata used for calculations."""
        self.client = client
        self.index_details = index_details

    def get_ltp_index(self, index: str) -> float | None:
        """
        Get quote for a single scrip
        """
        try:
            if index == "BANKEX":
                response = cast(
                    dict[str, Any], self.client.get_expiry("N", "BANKNIFTY")
                )
                response["lastrate"][0]["LTP"] = response["lastrate"][0]["LTP"] + 7000
            else:
                exchange = "B" if index == "SENSEX" else "N"
                response = cast(dict[str, Any], self.client.get_expiry(exchange, index))
            if "lastrate" in response:
                return response["lastrate"][0]["LTP"]
        finally:
            pass

    def _get_expiry_day(self, weekday_str: str) -> int:
        """Returns the weekday number for a given weekday string."""
        return WEEKDAYS[weekday_str]

    def _is_holiday(self, date_str: str, holiday_list: list[str]) -> bool:
        """Check if the given date string in 'YYYY-MM-DD' format is a holiday."""
        formatted_date_str = date_str.replace("-", "")
        return formatted_date_str in holiday_list

    def _is_last_week_of_month(self, date: datetime.date) -> bool:
        """Check if the given date falls in the last week of its month."""
        next_week = date + datetime.timedelta(days=7)
        return next_week.month != date.month

    def get_current_week_expiry_date(
        self,
        index_key: str,
        holiday_list: list[str] = HOLIDAY_LIST,
        today: datetime.date | None = None,
    ) -> str:
        """
        Returns the current or next valid expiry date considering holidays and index-specific expiry rules.

        Args:
            index_key (str): Key for the index to determine the expiry rules.

        Returns:
            str: Expiry date in 'YYYY-MM-DD' format.
        """
        if index_key not in self.index_details:
            raise ValueError("Invalid index key provided")

        today = today or datetime.date.today()
        weekly_expiry_day = self._get_expiry_day(
            self.index_details[index_key]["weekly_expiry"]
        )
        monthly_expiry_day = self._get_expiry_day(
            self.index_details[index_key]["monthly_expiry"]
        )

        weekly_expiry_date = self._calculate_nearest_expiry_date(
            today, weekly_expiry_day, holiday_list
        )
        monthly_expiry_date = self._calculate_nearest_expiry_date(
            today, monthly_expiry_day, holiday_list, mode="monthly"
        )

        if self._is_last_week_of_month(today) and (
            today.day in (monthly_expiry_date.day, weekly_expiry_date.day)
            and today.month in (monthly_expiry_date.month, weekly_expiry_date.month)
        ):
            return today.strftime("%Y-%m-%d")

        if (
            monthly_expiry_date > today
            and (monthly_expiry_date - today).days < 7
            and self._is_last_week_of_month(today)
        ):
            chosen_expiry_date = monthly_expiry_date
        else:
            chosen_expiry_date = weekly_expiry_date
        return chosen_expiry_date.strftime("%Y-%m-%d")

    def _calculate_nearest_expiry_date(
        self,
        start_date: datetime.date,
        expiry_weekday: int,
        holiday_list: list[str],
        mode: str = "weekly",
    ) -> datetime.date:
        """
        Calculates the nearest future expiry date for a given weekday, ensuring it falls within the last week of the month for monthly expiries.

        Args:
            start_date (datetime.date): The date from which to start the calculation.
            expiry_weekday (int): The day of the week the expiry is typically set (0=Monday, 6=Sunday).
            holiday_list (list of str): Dates formatted as 'YYYY-MM-DD' that are public holidays.
            mode (str): 'weekly' or 'monthly' to specify the calculation mode.

        Returns:
            datetime.date: The calculated nearest expiry date, adjusted for being in the last week of the month if required, holidays, and weekends.
        """
        if mode == "monthly":
            next_month = start_date.replace(day=28) + datetime.timedelta(days=4)
            last_day_of_month = next_month - datetime.timedelta(days=next_month.day)
            last_possible_expiry = last_day_of_month
            while last_possible_expiry.weekday() != expiry_weekday:
                last_possible_expiry -= datetime.timedelta(days=1)

            if last_possible_expiry < start_date:
                next_month_start = last_day_of_month + datetime.timedelta(days=1)
                last_day_of_next_month = (
                    next_month_start.replace(day=28)
                    + datetime.timedelta(days=4)
                    - datetime.timedelta(days=next_month_start.day)
                )
                last_possible_expiry = last_day_of_next_month
                while last_possible_expiry.weekday() != expiry_weekday:
                    last_possible_expiry -= datetime.timedelta(days=1)
            expiry_date = last_possible_expiry
        else:
            days_until_expiry = (expiry_weekday - start_date.weekday() + 7) % 7
            expiry_date = start_date + datetime.timedelta(days=days_until_expiry)

        while (
            self._is_holiday(expiry_date.strftime("%Y-%m-%d"), holiday_list)
            or expiry_date.weekday() >= 5
        ):
            expiry_date -= datetime.timedelta(days=1)

        return expiry_date

    def _calculate_expiry_date(
        self,
        start_date: datetime.date,
        expiry_weekday: int,
        holiday_list: list[str],
    ) -> datetime.date:
        """
        Calculate the nearest expiry date from a given start date, adjusting for holidays and weekends.

        Args:
            start_date (datetime.date): The date from which to calculate the expiry.
            expiry_weekday (int): The weekday number of the expiry.
            holiday_list: List of holidays in 'YYYY-MM-DD' format.

        Returns:
            datetime.date: The next valid expiry date.
        """
        days_until_expiry = (expiry_weekday - start_date.weekday()) % 7
        if days_until_expiry == 0 and datetime.datetime.now().hour >= 15:
            days_until_expiry = 7
        expiry_date = start_date + datetime.timedelta(days=days_until_expiry)

        while self._is_holiday(
            expiry_date.strftime("%Y-%m-%d"), holiday_list
        ) or expiry_date.weekday() in [5, 6]:
            expiry_date -= datetime.timedelta(days=1)

        return expiry_date

    def get_opt_strike_price_list(self, index_key: str, index_ltp: float) -> list[str]:
        """
        Get a list of strike prices for options based on the given index key and last traded price (LTP).

        Args:
            index_key (str): The key to retrieve the index details from the index_details dictionary.
            index_ltp (float): The last traded price (LTP) of the index.

        Returns:
            list[str]: A list of strike prices for options, including 5 strike prices below and 5 strike prices above
                       the nearest rounded LTP.

        """
        step_size = self.index_details[index_key]["step_size"]

        strikes_below_above = OPTION_CHAIN_DEPTH // 2

        nearest_strike = round(index_ltp / step_size) * step_size

        strikes: list[str] = []
        start_strike = nearest_strike - (strikes_below_above * step_size)
        for i in range(OPTION_CHAIN_DEPTH):
            strike = start_strike + (i * step_size)
            strikes.append(str(int(strike)))
        return strikes

    def get_ltp_for_opt_strike_price(
        self,
        strike_price: int = 99,
        index_key: str = "NIFTY",
        current_expiry: str = "randomDateString",
        option_type: str = "CE",
        optional_list: list[dict[str, Any]] | None = None,
    ) -> Any:
        """
        Fetches the last rate and strike price for a given option.

        Args:
        - client: The trading client instance.
        - symbol: The underlying asset symbol for the option.
        - step_size: The step size of the option.
        - option_type: The type of the option (CE or PE).
        - current_date: The current date in YYYY-MM-DD format.
        - current_expiry: The current expiry date in YYYY-MM-DD format.
        - current_rate: The current market rate of the asset.

        Returns:
        - A tuple containing the last rate and strike price for the option.
        """
        if optional_list is None:
            optional_list = [dict(item) for item in DEFAULT_OPTION_FEED_PAYLOAD]

        if optional_list != []:
            market_feed = cast(
                dict[str, Any], self.client.fetch_market_feed(optional_list)
            )
            return market_feed
        current_expiry_dt = datetime.datetime.strptime(
            current_expiry, "%Y-%m-%d"
        ).date()
        option_data = [
            {
                "Exch": "N",
                "ExchType": "D",
                "Symbol": f"{index_key} {current_expiry_dt.strftime('%d %b %Y').upper()} {option_type} {strike_price:.2f}",
                "Expiry": current_expiry_dt.strftime("%Y%m%d"),
                "StrikePrice": f"{strike_price:.0f}",
                "OptionType": option_type,
            }
        ]
        market_feed = cast(dict[str, Any], self.client.fetch_market_feed(option_data))
        last_rate = market_feed["Data"][0]["LastRate"]
        return last_rate, strike_price
