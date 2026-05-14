# pylint: disable=wrong-import-position
# pylint: disable=too-many-nested-blocks
# pylint: disable=broad-exception-caught
# pylint: disable=line-too-long
# pylint: disable=consider-using-f-string
# pylint: disable=inconsistent-return-statements
# pylint: disable=dangerous-default-value
# pylint: disable=too-many-locals
# ruff: noqa: E402

"""
This module contains the ProgramDisplay class which is responsible for displaying the program's menu and handling user input.
"""

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pandas as pd
from tabulate import tabulate

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.program_constants import DATA_DIR, OPTION_TYPE_CALL, OPTION_TYPE_PUT
from src.program_helpers import clear_screen, read_data_from_file, setup_logging

log = setup_logging("program_display")


class ProgramDisplay:
    """
    A class that handles the display and user interaction for the trading program.
    """

    def __init__(
        self,
        client_sessions: dict[str, Any],
        index_details: Mapping[str, Any],
    ) -> None:
        self.client_sessions = client_sessions
        self.index_details = index_details

    def clear_screen(self) -> None:
        """
        Clears the screen.
        """
        clear_screen()

    def display_menu_title(self, menu_title: str | None = None) -> None:
        """
        Displays the menu title.

        Args:
            menu_title (str): The title of the menu.

        Returns:
            None
        """
        if menu_title:
            print("**" + menu_title.upper() + "**\n")

    def create_menu_out_of_list(
        self,
        menu_list: list[str] | None = None,
        menu_title: str | None = None,
        clear_screen_out: bool = True,
    ) -> None:
        """
        Creates a menu out of a list of options.

        Args:
            menu_list: A list of options for the menu.

        Returns:
            None
        """
        # How to order the list
        # If it is monday, then choose FINNIFTY first, then BANKNIFTY, then NIFTY, and then SENSEX
        # If it is tuesday, then choose FINNIFTY first, then BANKNIFTY, then NIFTY, and then SENSEX
        # If it is wednesday, then choose BANKNIFTY first, then NIFTY, then NIFTY, then SENSEX, and then FINNIFTY
        # If it is thursday, then choose NIFTY first, then SENSEX, then FINNIFTY, then SENSEX, and then BANKNIFTY
        # If it is friday, then choose SENSEX first, then FINNIFTY, then BANKNIFTY, then NIFTY
        menu_list = [] if menu_list is None else menu_list
        if clear_screen_out:
            self.clear_screen()
        self.display_menu_title(menu_title)
        if len(menu_list) != 0:
            for i, option in enumerate(menu_list):
                print(f"{i + 1}. {option}")
        print("\nSelect an option from the menu above.")
        print("\n")

    def get_menu_options(self, menu_list: list[str]) -> list[int]:
        """
        Returns a list of numbers representing the menu options.

        Args:
            menu_list: A list of menu options.

        Returns:
            list[int]: A list of numbers representing the menu options.
        """
        return list(range(1, len(menu_list) + 1))

    def validate_user_choice(
        self, user_choice: int | str, menu_list: list[str]
    ) -> bool:
        """
        Validates the user's choice against the available menu options.

        Args:
            user_choice (int): The user's choice.
            menu_list: The list of available menu options.

        Returns:
            bool: True if the user's choice is valid, False otherwise.
        """
        if isinstance(user_choice, str) and user_choice.lower() == "b":
            self.go_back_to_previous_menu()
        # Validate the user choice
        return user_choice in self.get_menu_options(menu_list)

    def go_back_to_previous_menu(self) -> None:
        """
        Prints a message indicating that the program is going back to the previous menu.
        """
        input("\nGoing back to previous menu. Press Enter to continue...")

    def give_user_choice_to_go_back(self) -> None:
        """
        Gives the user a choice to go back to the previous menu.
        """
        print("\nOR enter 'b' to go back to the previous menu.")

    def take_user_input(self) -> str:
        """
        Takes user input to continue.
        """
        return input("\nChoose one of the options above and press Enter to continue: ")

    def place_buy_order_choose_index_submenu(self) -> str | None:
        """
        Displays a submenu to choose an index for placing a buy order.

        Returns:
            str: The selected index from the list.
        """
        index_list = list(self.index_details.keys())
        self.create_menu_out_of_list(index_list, "Choose an index to place a buy order")
        self.give_user_choice_to_go_back()
        user_choice = self.take_user_input()
        if user_choice.lower() == "r" or user_choice == "":
            return "r"
        if user_choice.lower() == "b":
            self.go_back_to_previous_menu()
            return None

        if user_choice.isdigit() and int(user_choice) in self.get_menu_options(
            index_list
        ):
            return index_list[int(user_choice) - 1]
        max_attempts = 2
        attempts = 0
        while attempts < max_attempts:
            self.place_buy_order_choose_index_submenu()
            attempts += 1
        return None

    def create_dynamic_table(
        self, client_sessions: dict[str, Any], index_key: str
    ) -> tuple[pd.DataFrame, Any]:
        """
        Creates a dynamic table by concatenating data from multiple clients for a given index and option type.

        Args:
            client_sessions: A dictionary containing client sessions.
            index_key (str): The key for the selected index.

        Returns:
            pd.DataFrame: The final concatenated DataFrame containing the dynamic table.
        """
        concatenated_data = []

        # Loop through both CE and PE option types and concatenate the data
        for option_type in [OPTION_TYPE_CALL, OPTION_TYPE_PUT]:
            any_client_key = "ACCOUNT_5PAISA_PRIMARY".lower()
            # log.info("Any client key: %s", any_client_key)
            file_path_any_client = (
                DATA_DIR / any_client_key / f"{index_key}_options.json"
            )
            file_contents_any_client = read_data_from_file(file_path_any_client)
            index_file = DATA_DIR / any_client_key / f"{index_key}.json"
            index_data = read_data_from_file(index_file)
            if not index_data:
                raise ValueError(f"No data found for the selected index {index_key}.")
            # {
            #     "index": "BANKNIFTY",
            #     "quote": 47859.45,
            #     "current_week_expiry_date": "2024-05-15",
            #     "timestamp": "2024-05-15 08:53:34 +5:30"
            # }
            index_quote = index_data.get("quote", "NA")
            if not file_contents_any_client:
                raise ValueError(
                    f"No data found for the selected index {index_key} and option type {option_type}."
                )

            # List of symbols for the specified index and option type
            filtered_options_symbols_any_client = [
                option["Index_Symbol"]
                for option in file_contents_any_client
                if option["Index_Symbol"].startswith(index_key)
                and option["OptionType"] == option_type
            ]
            # Create the base DataFrame from the first client's filtered symbols
            df = pd.DataFrame(
                {
                    "Symbol": filtered_options_symbols_any_client,
                    "OptD [LR | H | L]": [
                        f"{option['LastRate']} | {option['High']} | {option['Low']}"
                        for option in file_contents_any_client
                        if option["Index_Symbol"] in filtered_options_symbols_any_client
                    ],
                }
            )
            # Adding client columns to the DataFrame
            for client_key in client_sessions.keys():
                if client_key == "ACCOUNT_5PAISA_PRIMARY":
                    client_dir_path = DATA_DIR / client_key.lower()
                    file_path = client_dir_path / f"{index_key}_options.json"
                    file_contents = read_data_from_file(file_path)

                    if not file_contents:
                        raise ValueError(
                            f"No data found for the selected index {index_key} and option type {option_type}."
                        )

                    # Mapping each symbol to quantity and margin
                    client_info = {
                        option[
                            "Index_Symbol"
                        ]: f"{option['Quantity_to_Purchase']} | {option['Client_Margin']}"
                        for option in file_contents
                        if option["Index_Symbol"] in filtered_options_symbols_any_client
                    }

                    # Add client-specific data to the DataFrame
                    header_name = f"{client_key.upper()} [Qty | M]"
                    df[header_name] = df["Symbol"].map(client_info).fillna("NA")

                    bulk_order_info = {
                        option["Index_Symbol"]: option.get("BulkOrderList", [])
                        for option in file_contents
                        if option["Index_Symbol"] in filtered_options_symbols_any_client
                    }
                    hidden_column_name = f"{client_key}-BulkOrderList-Hidden"
                    df[hidden_column_name] = (
                        df["Symbol"].map(bulk_order_info).fillna("[]")
                    )

            concatenated_data.append(df)

        # Concatenate the CE and PE dataframes vertically
        final_df = pd.concat(concatenated_data, ignore_index=True)
        final_df.insert(
            0, "S.No.", range(1, len(final_df) + 1)
        )  # Inserting S.No. at the beginning

        return final_df, index_quote

    def refresh_option_sub_menu(self, index_key: str) -> None:
        """
        Refreshes the option sub-menu by displaying option data to the user.

        Args:
            index_key (str): The index key for the sub-menu.
            client_sessions: A dictionary of client sessions.

        Returns:
            None
        """
        # log.info("Refreshing the option sub-menu for index %s.", index_key)
        self.display_option_data_menu_to_user_submenu(index_key)

    def display_option_data_menu_to_user_submenu(self, index_key: str) -> Any:
        """
        Displays the option data menu to the user for a specific index key.

        Args:
            index_key (str): The index key for which the option data menu is displayed.
            client_sessions: A dictionary of client sessions.

        Returns:
            list: The bulk order lists based on the user's choice.
        """
        client_sessions = self.client_sessions
        # log.info("Client sessions: %s", client_sessions)
        df, index_quote = self.create_dynamic_table(client_sessions, index_key)
        # log.info("Option data for index %s: %s", index_key, df)
        self.clear_screen()
        self.display_menu_title(f"Option Data for {index_key} -- {index_quote}")
        self.pretty_print_data_frame(df)
        self.create_menu_out_of_list(clear_screen_out=False)
        self.give_user_choice_to_go_back()
        print("\nOR enter 'r' refresh the data.")
        user_choice = self.take_user_input()
        if user_choice.lower() == "r" or user_choice == "":
            return "r"
        if user_choice.lower() == "b":
            self.go_back_to_previous_menu()
            return None
        if user_choice.isdigit() and int(user_choice) in list(df["S.No."]):
            df = (self.create_dynamic_table(client_sessions, index_key))[0]
            return self.get_bulk_order_lists_by_serial_number(df, int(user_choice))
        max_attempts = 2
        attempts = 0
        while attempts < max_attempts:
            self.refresh_option_sub_menu(index_key)
            attempts += 1
        return None

    def pretty_print_data_frame(self, df: pd.DataFrame) -> None:
        """
        Prints a formatted representation of a DataFrame with visible columns.

        Args:
            df (pd.DataFrame): The DataFrame to be printed.

        Returns:
            None
        """
        visible_columns = [col for col in df.columns if "Hidden" not in col]
        print(
            tabulate(
                cast(Any, df[visible_columns]),
                headers="keys",
                tablefmt="pretty",
                showindex=False,
            )
        )

    def get_bulk_order_lists_by_serial_number(
        self, df: pd.DataFrame, user_choice: int
    ) -> list[dict[str, Any]]:
        """
        Retrieves bulk order lists based on the user's choice of serial number.

        Args:
            df (pandas.DataFrame): The dataframe containing the data.
            user_choice (int): The serial number chosen by the user.

        Returns:
            list: A list of dictionaries, where each dictionary contains the client key and the corresponding bulk order list.

        Raises:
            ValueError: If no data is found for the specified serial number.
        """
        # Find the row index in the dataframe that matches the user's choice of S.No.
        row_index = df[df["S.No."] == user_choice].index

        if not row_index.empty:
            row_index = row_index[
                0
            ]  # Assuming S.No. are unique and taking the first match
            bulk_order_lists = []

            # Loop through each column to find those ending with "-BulkOrderList-Hidden"
            for col in df.columns:
                if col.endswith("-BulkOrderList-Hidden"):
                    client_key = col.replace("-BulkOrderList-Hidden", "")
                    bulk_order_list = df.at[row_index, col]
                    bulk_order_lists.append({client_key: bulk_order_list})

            return bulk_order_lists

        else:
            raise ValueError(f"No data found for S.No. {user_choice}")
