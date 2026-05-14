# pylint: disable=wrong-import-position
# pylint: disable=too-many-nested-blocks
# pylint: disable=broad-exception-caught
# pylint: disable=line-too-long
# pylint: disable=consider-using-f-string
# pylint: disable=dangerous-default-value
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
        self.client = client
        self.index_details = index_details

    def get_ltp_index(self, index: str) -> float | None:
        """
        Get quote for a single scrip
        """
        # disable_loguru_to_devnull()
        try:
            if index == "BANKEX":
                # Temp workaround for BANKEX
                response = cast(
                    dict[str, Any], self.client.get_expiry("N", "BANKNIFTY")
                )
                response["lastrate"][0]["LTP"] = response["lastrate"][0]["LTP"] + 7000
            else:
                exchange = "B" if index == "SENSEX" else "N"
                response = cast(dict[str, Any], self.client.get_expiry(exchange, index))
            # log.info("Response for get_ltp_index for '%s' : %s", index, response)
            if "lastrate" in response:
                return response["lastrate"][0]["LTP"]
        finally:
            # restore_loguru()
            pass

    def _get_expiry_day(self, weekday_str: str) -> int:
        """Returns the weekday number for a given weekday string."""
        # log.info("Weekday string: %s", weekday_str)
        # log.info("Weekday number: %s", weekdays[weekday_str])
        return WEEKDAYS[weekday_str]

    def _is_holiday(self, date_str: str, holiday_list: list[str]) -> bool:
        """Check if the given date string in 'YYYY-MM-DD' format is a holiday."""
        formatted_date_str = date_str.replace(
            "-", ""
        )  # Remove dashes to match 'YYYYMMDD' format
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
        # Retrieve the weekly and monthly expiry weekdays
        weekly_expiry_day = self._get_expiry_day(
            self.index_details[index_key]["weekly_expiry"]
        )
        monthly_expiry_day = self._get_expiry_day(
            self.index_details[index_key]["monthly_expiry"]
        )

        # Calculate the nearest possible weekly and monthly expiry dates from today
        weekly_expiry_date = self._calculate_nearest_expiry_date(
            today, weekly_expiry_day, holiday_list
        )
        monthly_expiry_date = self._calculate_nearest_expiry_date(
            today, monthly_expiry_day, holiday_list, mode="monthly"
        )

        # Select the appropriate expiry date based on whether the monthly expiry has passed

        # return today.strftime("%Y-%m-%d")

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
        # log.info("Chosen expiry date: %s", chosen_expiry_date.strftime("%Y-%m-%d"))
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
            # Adjust the month calculation to determine the last week of the month
            next_month = start_date.replace(day=28) + datetime.timedelta(days=4)
            last_day_of_month = next_month - datetime.timedelta(days=next_month.day)
            last_possible_expiry = last_day_of_month
            while last_possible_expiry.weekday() != expiry_weekday:
                last_possible_expiry -= datetime.timedelta(days=1)

            # Determine if the last_possible_expiry is before or after today
            if last_possible_expiry < start_date:
                # If it's before today, calculate for the next month
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
            # Weekly mode: Calculate the next occurrence of the expiry weekday
            days_until_expiry = (expiry_weekday - start_date.weekday() + 7) % 7
            # log.info("Days until expiry: %s", days_until_expiry)
            expiry_date = start_date + datetime.timedelta(days=days_until_expiry)
            # log.info("Expiry date: %s", expiry_date)
            # if days_until_expiry == 0:
            #     expiry_date += datetime.timedelta(
            #         days=7
            #     )  # Ensure it's next week if today is the expiry day

        # Adjust for holidays and weekends
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
        if (
            days_until_expiry == 0 and datetime.datetime.now().hour >= 15
        ):  # Assuming markets close by 3 PM
            days_until_expiry = (
                7  # Move to the next week if today's trading hours are over
            )
        expiry_date = start_date + datetime.timedelta(days=days_until_expiry)

        # Adjust the expiry date backwards if it lands on a holiday or a weekend
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
        # Retrieve the step size for the index from the index_details
        step_size = self.index_details[index_key]["step_size"]

        # Determine the number of strikes above and below the nearest strike
        strikes_below_above = OPTION_CHAIN_DEPTH // 2

        # Round the LTP to the nearest multiple of the step size
        nearest_strike = round(index_ltp / step_size) * step_size

        # Calculate strike prices around the rounded nearest strike
        strikes: list[str] = []
        # Start from strikes_below_above steps below the nearest rounded strike
        start_strike = nearest_strike - (strikes_below_above * step_size)
        # Generate 10 strike prices (5 below and 5 above including the nearest to LTP)
        for i in range(OPTION_CHAIN_DEPTH):
            strike = start_strike + (i * step_size)
            strikes.append(
                str(int(strike))
            )  # Convert float to int and then to string for consistency
        # log.info("Strike prices: %s", strikes)
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

        # log.info("Optional list: %s", optional_list)
        if optional_list != []:
            market_feed = cast(
                dict[str, Any], self.client.fetch_market_feed(optional_list)
            )
            # log.info("Market feed: %s", market_feed)
            return market_feed
        # Convert current_expiry to datetime objects
        current_expiry_dt = datetime.datetime.strptime(
            current_expiry, "%Y-%m-%d"
        ).date()
        # log.info("Current expiry date: %s", current_expiry_dt)
        # Create option data dictionary
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
        # log.info("Fetching LTP for Option data: %s", option_data)
        # Return the option data dictionary as a list
        market_feed = cast(dict[str, Any], self.client.fetch_market_feed(option_data))
        last_rate = market_feed["Data"][0]["LastRate"]
        # log.info("Last rate for option: %s", last_rate)
        return last_rate, strike_price
