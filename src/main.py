# ruff: noqa: E402

"""
This is the main program runner for the trading program. It provides a menu for the user to interact with the program.
"""

import resource
import sys
from pathlib import Path
from typing import Any, cast

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.program_background import ProgramBackground
from src.program_constants import INDEX_DETAILS_FNO, LOGS_DIR, SCRIP_MASTER_FILE_PATH
from src.program_display import ProgramDisplay
from src.program_helpers import (
    clear_screen,
    create_data_frame_from_scrip_master_csv,
    get_account_config,
    get_account_names_from_config,
    get_scrip_master,
    remove_old_logs,
    run_as_background_thread,
    setup_logging,
    setup_signal_handlers,
    wait_for_user_input,
)
from src.program_login import Login
from src.program_orders import Orders

try:
    from src.program_secrets import SECRETS
except ModuleNotFoundError:
    SECRETS: dict[str, dict[str, str]] = {}

log = setup_logging("primary_program_runner", log_to_console=True)

setup_signal_handlers()
DF_PD: pd.DataFrame = pd.DataFrame()
CLIENT_SESSIONS: dict[str, Any] = {}
INTRADAY: str = "MIS"
OPTION_CHAIN_DEPTH: int = 6
KOTAK_PRIMARY_ACCOUNT: str = "ACCOUNT_KOTAK_NEO_PRIMARY"
VALID_ORDER_TYPES: set[str] = {"buy", "sell", "cancel"}


def start_background_client_tasks() -> None:
    """
    Starts background tasks for all logged-in clients.

    This function creates a ProgramBackground instance for each logged-in client and starts the background tasks.

    Returns:
        None
    """
    kotak_neo = CLIENT_SESSIONS.get(KOTAK_PRIMARY_ACCOUNT)
    for account_key, client in CLIENT_SESSIONS.items():
        bg = ProgramBackground(client, account_key, DF_PD, additional_client=kotak_neo)
        bg.start_background_client_tasks()
        log.info("Started background tasks for configured account.")


def login_to_accounts() -> None:
    """
    Logs in to user accounts based on user input.

    This function prompts the user to select accounts to log in to and then attempts to log in to each selected account.
    If successful, the logged-in client session is stored in the global CLIENT_SESSIONS dictionary.

    Returns:
        None
    """
    global CLIENT_SESSIONS
    clear_screen()
    print("Login to Accounts:\n")
    accounts = get_account_names_from_config(SECRETS)
    for index, account in enumerate(accounts, start=1):
        print(f"{index}. {account}")
    print("\n")

    selected = input(
        "Enter 'r' to return to the main menu.\nEnter the account number(s) you want to log in to, separated by commas, or 'all' to log in to all accounts: "
    )

    if selected.lower() == "r":
        log.info("Login menu returned without login.")
        return

    if selected.lower() == "all":
        selected_indices = range(len(accounts))
    else:
        try:
            selected_indices = [int(num.strip()) - 1 for num in selected.split(",")]
        except ValueError:
            log.warning("Invalid login selection: %s", selected)
            return

    for index in selected_indices:
        if index < 0 or index >= len(accounts):
            log.warning("Login selection out of range: %s", index + 1)
            continue
        account_key = accounts[index]
        try:
            print("\n")
            log.info("Starting login for selected account.")
            client = Login(account_key, get_account_config(account_key, SECRETS))
            authenticated_client = client.login()
            CLIENT_SESSIONS[account_key] = authenticated_client
            log.info("Logged in successfully for selected account.")
        except Exception as e:
            log.error(
                "Failed to log in for selected account. error_type=%s", type(e).__name__
            )
    start_background_client_tasks()
    wait_for_user_input()


def sell_order_t(order: Orders, intra_day: str) -> None:
    """
    Places a sell order for the given client.

    Args:
        order (Orders): The Orders object for the client.

    Returns:
        None
    """
    try:
        order.place_sell_order_all(intra_day)
    except Exception as e:
        log.exception("Failed to place sell order batch: %s", e)


def cancel_order_t(order: Orders) -> None:
    """
    Places a cancel order for the given client.

    Args:
        order (Orders): The Orders object for the client.

    Returns:
        None
    """
    try:
        order.cancel_all_open_orders()
    except Exception as e:
        log.exception("Failed to place cancel order batch: %s", e)


def place_order_for_all_clients(
    order_type: str, response_option: list[dict[str, Any]] | None = None
) -> None:
    """
    Places an order for all logged-in clients.

    This function places an order of the given type (buy/sell/cancel) for all clients that are currently logged in.

    Args:
        order_type (str): The type of order to place. Must be 'buy', 'sell', or 'cancel'.

    Returns:
        None
    """
    if not CLIENT_SESSIONS:
        log.warning("Order request ignored because no clients are logged in.")
        wait_for_user_input()
        return

    if order_type not in VALID_ORDER_TYPES:
        log.warning("Invalid order type requested: %s", order_type)
        wait_for_user_input()
        return

    if KOTAK_PRIMARY_ACCOUNT not in CLIENT_SESSIONS:
        log.error(
            "Order request cannot continue; primary Kotak account is not logged in."
        )
        wait_for_user_input()
        return

    if order_type in {"sell", "cancel"}:
        for account_key, client in CLIENT_SESSIONS.items():
            if account_key == KOTAK_PRIMARY_ACCOUNT:
                try:
                    orders = Orders(client)
                    if order_type == "sell":
                        log.info(
                            "Dispatching sell order workflow. product=%s",
                            INTRADAY,
                        )
                        run_as_background_thread(sell_order_t, orders, INTRADAY)
                    elif order_type == "cancel":
                        log.info("Dispatching cancel order workflow.")
                        run_as_background_thread(cancel_order_t, orders)
                except Exception as e:
                    log.error(
                        "Failed to dispatch order workflow. type=%s error_type=%s",
                        order_type,
                        type(e).__name__,
                    )
        return

    for response in response_option or []:
        for key, val in response.items():
            try:
                client = CLIENT_SESSIONS[KOTAK_PRIMARY_ACCOUNT]
                orders = Orders(client)
                if order_type == "buy":
                    log.info(
                        "Dispatching buy order workflow. product=%s groups=%d",
                        INTRADAY,
                        len(val),
                    )
                    run_as_background_thread(orders.place_buy_order_bulk, val, INTRADAY)
            except Exception as e:
                log.error(
                    "Failed to dispatch buy order workflow. error_type=%s",
                    type(e).__name__,
                )


def debug_client_interaction() -> None:
    """
    Allows debugging of client interactions.

    This function displays a menu of clients currently logged in and prompts the user to select a client to debug.
    Once a client is selected, the user can enter commands to run on the client object, prefixed with 'client.'.
    The command is executed using eval and the result is printed.

    Returns:
        None
    """
    clear_screen()
    print("DEBUG MODE\n")
    if not CLIENT_SESSIONS:
        print("No clients currently logged in.\n")
        input("Press Enter to continue...")
        return
    print("Select a client to debug:")
    show_logged_in_accounts()

    selected = int(input("Choose a client number: ")) - 1
    account_keys = list(CLIENT_SESSIONS.keys())
    if selected < 0 or selected >= len(account_keys):
        print("Invalid client selection.\n")
        input("Press Enter to continue...")
        return

    account_key = account_keys[selected]
    client = CLIENT_SESSIONS[account_key]  # noqa: F841 - referenced by eval below.

    while True:
        print(
            "Enter commands to run on the client, e.g., 'client.order_report()'. Type 'exit' to return.\n"
        )
        cmd = input(">> ")
        if cmd.lower() == "exit":
            break
        if not cmd.startswith("client."):
            print("Invalid command. Ensure your command starts with 'client.'")
            continue
        try:
            result = eval(cmd)
            print("Command result:", result)
        except Exception as e:
            log.warning("Debug command failed. error_type=%s", type(e).__name__)
            print("Failed to execute command:", str(e))


def show_logged_in_accounts() -> None:
    """
    Displays the accounts that are currently logged in.

    Returns:
        None
    """
    if not CLIENT_SESSIONS:
        print("No accounts are currently logged in.")
    else:
        print("Accounts currently logged in:")
        for idx, account in enumerate(CLIENT_SESSIONS, start=1):
            print(f"{idx}. {account}")
    print("\n")
    wait_for_user_input()


def main_menu() -> None:
    """
    Displays the main menu and handles user input for various options.

    The main menu allows the user to perform actions such as logging in to accounts,
    placing buy/sell/cancel orders, logging out of accounts, checking logged-in accounts,
    debugging, and exiting the program.

    Returns:
        None
    """

    def log_and_update_file_descriptor_limit() -> None:
        """Raise the soft file descriptor limit for concurrent background workers."""
        _soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        log.info(
            "Current file descriptor limit. soft=%s hard=%s", _soft_limit, hard_limit
        )

        new_soft_limit = min(4096, hard_limit)
        resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft_limit, hard_limit))

        _updated_soft_limit, _updated_hard_limit = resource.getrlimit(
            resource.RLIMIT_NOFILE
        )
        log.info(
            "Updated file descriptor limit. soft=%s hard=%s",
            _updated_soft_limit,
            _updated_hard_limit,
        )

    log_and_update_file_descriptor_limit()
    global INTRADAY
    global DF_PD
    global CLIENT_SESSIONS
    log.info("Starting main menu.")
    log.info("Refreshing scrip master before menu.")
    get_scrip_master()
    DF_PD = create_data_frame_from_scrip_master_csv(SCRIP_MASTER_FILE_PATH)
    remove_old_logs(LOGS_DIR)
    while True:
        try:
            clear_screen()
            print("\t\t\t\tTrade with 5paisa\n")
            print("Main Menu:\n")
            options = [
                "Login to accounts",
                "Place buy order for all logged in accounts",
                "Place sell order for all logged in accounts",
                "Place cancel order for all logged in accounts",
                "Logout of accounts",
                "See which accounts are logged in",
                "Debug",
                "Flip delivery flag for all clients",
                "Remove all session files for all clients",
                "Change option chain depth",
                "Exit program",
            ]
            for i, option in enumerate(options):
                print(f"{i + 1}. {option}")

            choice = input("\nSelect an option: ")
            if choice.isdigit():
                choice = int(choice)
                if choice == 1:
                    login_to_accounts()
                elif choice == 2:
                    if not CLIENT_SESSIONS:
                        log.warning(
                            "Buy order menu requested with no logged-in clients."
                        )
                        wait_for_user_input()
                        continue

                    display = ProgramDisplay(CLIENT_SESSIONS, INDEX_DETAILS_FNO)
                    while True:
                        response = display.place_buy_order_choose_index_submenu()
                        if response != "r":
                            break

                    if isinstance(response, str):
                        max_attempts = 3
                        while True:
                            try:
                                response_option = (
                                    display.display_option_data_menu_to_user_submenu(
                                        response
                                    )
                                )
                                max_attempts = 3
                            except Exception as e:
                                max_attempts -= 1
                                if max_attempts == 0:
                                    log.exception(
                                        "Option data submenu failed after retries. index=%s error=%s",
                                        response,
                                        e,
                                    )
                                    wait_for_user_input()
                                    break
                                continue
                            if response_option != "r":
                                break
                        if not response_option:
                            continue
                        place_order_for_all_clients(
                            "buy", cast(list[dict[str, Any]], response_option)
                        )
                elif choice == 3:
                    place_order_for_all_clients("sell")
                    place_order_for_all_clients("cancel")
                elif choice == 4:
                    place_order_for_all_clients("cancel")
                elif choice == 5:
                    raise NotImplementedError("Logout of accounts")
                elif choice == 6:
                    show_logged_in_accounts()
                elif choice == 7:
                    debug_client_interaction()
                elif choice == 8:
                    if INTRADAY == "MIS":
                        INTRADAY = "NRML"
                    else:
                        INTRADAY = "MIS"
                    log.info("Order product flag changed. product=%s", INTRADAY)
                    wait_for_user_input()
                elif choice == 9:
                    Login.delete_all_session_files(list(SECRETS.keys()))
                    CLIENT_SESSIONS = {}
                    log.info("Deleted cached sessions for all configured accounts.")
                    wait_for_user_input()
                elif choice == 10:
                    global OPTION_CHAIN_DEPTH
                    old_depth = OPTION_CHAIN_DEPTH
                    OPTION_CHAIN_DEPTH = int(input("Enter new option chain depth: "))
                    log.info(
                        "Option chain depth changed. old=%s new=%s",
                        old_depth,
                        OPTION_CHAIN_DEPTH,
                    )
                elif choice == 11:
                    break
                else:
                    log.warning("Invalid main menu option: %s", choice)
                    wait_for_user_input()
            else:
                log.warning("Non-numeric main menu input: %s", choice)
                wait_for_user_input()
        except Exception as e:
            log.exception("Main menu loop recovered from error: %s", e)
            continue
    log.info("Exiting program.")


if __name__ == "__main__":
    main_menu()
