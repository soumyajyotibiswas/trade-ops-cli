"""Shared helpers for logging, IO, threading, signals, and broker data files."""

import json
import logging
import os
import signal
import ssl
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pandas as pd
import requests
from loguru import logger as loguru_logger

from src.program_constants import (
    DATA_DIR,
    LOGS_DIR,
    SCRIP_MASTER_FILE_PATH,
    SCRIP_MASTER_FILE_URL,
)

ORIGINAL_LOGURU_HANDLERS: Any = []
DEFAULT_LOG_LEVEL = "INFO"
LOG_FORMAT = (
    "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s | "
    "%(threadName)s | %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _configured_log_level() -> int:
    """Return the configured logging level, defaulting to INFO."""
    level_name = os.getenv("TRADING_PROGRAM_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    return getattr(logging, level_name, logging.INFO)


def configure_requests_ca_bundle(
    env_override_name: str = "REQUESTS_CA_BUNDLE",
) -> str | None:
    """
    Configure CA environment for requests and httpx.

    requests uses REQUESTS_CA_BUNDLE, while httpx/py5paisa honors SSL_CERT_FILE.
    Prefer an explicit override, then Python/OpenSSL's default CA file.
    """
    override = os.environ.get(env_override_name)
    if override and Path(override).exists():
        os.environ["REQUESTS_CA_BUNDLE"] = override
        os.environ["SSL_CERT_FILE"] = override
        return override

    existing = (
        os.environ.get("REQUESTS_CA_BUNDLE")
        or os.environ.get("SSL_CERT_FILE")
        or os.environ.get("CURL_CA_BUNDLE")
    )
    if existing and Path(existing).exists():
        os.environ["REQUESTS_CA_BUNDLE"] = existing
        os.environ["SSL_CERT_FILE"] = existing
        return existing

    candidates = [
        ssl.get_default_verify_paths().cafile,
        "/opt/homebrew/etc/openssl@3/cert.pem",
        "/usr/local/etc/openssl@3/cert.pem",
        "/etc/ssl/cert.pem",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            os.environ["REQUESTS_CA_BUNDLE"] = candidate
            os.environ["SSL_CERT_FILE"] = candidate
            return candidate
    return None


def disable_loguru_to_devnull() -> None:
    """Temporarily redirect loguru output from noisy SDK internals to /dev/null."""
    global ORIGINAL_LOGURU_HANDLERS
    loguru_core = cast(Any, loguru_logger)._core
    ORIGINAL_LOGURU_HANDLERS = loguru_core.handlers.copy()
    loguru_logger.remove()
    loguru_logger.add(os.devnull, enqueue=True)


def restore_loguru() -> None:
    """Restore loguru handlers saved before disabling loguru output."""
    loguru_logger.remove()
    for handler_id, handler in ORIGINAL_LOGURU_HANDLERS.items():
        loguru_logger.add(**handler)


def get_account_config(account_name: str, account_config: dict[str, Any]) -> Any:
    """
    Retrieves the account configuration for the specified account name.

    Args:
        account_name (str): The name of the account to retrieve the configuration for.
        account_config (dict): A dictionary containing the account configurations.

    Returns:
        str: The account configuration for the specified account name. If the account name is not found, returns "Account not found".
    """
    return account_config.get(account_name, "Account not found")


def get_account_names_from_config(account_config: dict[str, Any]) -> list[str]:
    """
    Returns a list of account names from the given account configuration.

    Args:
        account_config (dict): A dictionary containing account configurations.

    Returns:
        list: A list of account names extracted from the account configuration.

    """
    return list(account_config.keys())


def get_scrip_master() -> None:
    """
    Downloads the scrip master csv file from a given URL and stores it in DATA_DIR.
    The file is downloaded only if the scrip master is not present or is older than 48 hours.

    Returns:
        None
    """
    if SCRIP_MASTER_FILE_PATH.exists():
        file_mod_time = datetime.fromtimestamp(SCRIP_MASTER_FILE_PATH.stat().st_mtime)
        print(file_mod_time)
        if datetime.now() - file_mod_time < timedelta(hours=48):
            print("Scrip Master file is up-to-date.")
            return
        else:
            print("Scrip Master file is outdated, downloading new file.")
    else:
        print("Scrip Master file does not exist, downloading new file.")
    try:
        ca_bundle = configure_requests_ca_bundle("FIVEPAISA_REQUESTS_CA_BUNDLE")
        response = requests.get(
            SCRIP_MASTER_FILE_URL,
            timeout=300,
            verify=ca_bundle if ca_bundle else True,
        )
        response.raise_for_status()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(SCRIP_MASTER_FILE_PATH, "wb") as f:
            f.write(response.content)
        print("Scrip Master file has been downloaded and saved.")
    except requests.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")


def continue_or_back() -> str | bool:
    """
    Prompts the user to continue with the action or go back to the main menu.

    Returns:
        str or bool: The user's choice. Returns False if the choice is invalid.
    """
    print("Y. Continue with the action")
    print("N. Back to main menu\n")
    choice = input("Select an option: ")
    if choice not in ["Y", "N", "y", "n"]:
        return False
    return choice


def clear_screen() -> None:
    """
    Clears the terminal screen.

    This function checks the operating system and uses the appropriate command
    to clear the terminal screen. On Windows, it uses the "cls" command, and on
    other operating systems, it uses the "clear" command.

    Note:
        This function relies on the `os.name` attribute to determine the
        operating system. Make sure to import the `os` module before using
        this function.

    """
    if os.name == "nt":
        _ = os.system("cls")
    else:
        _ = os.system("clear")


def setup_logging(script_name: str, log_to_console: bool = False) -> logging.Logger:
    """
    Set up a per-module logger with consistent file output.

    File logs use TRADING_PROGRAM_LOG_LEVEL (default INFO). Console logs, when enabled,
    emit WARNING and above so the interactive menus stay readable.

    Args:
        script_name (str): The name of the script, used to create a dedicated log directory and file.
        log_to_console (bool): Flag to determine whether logs should also be output to the console.
    """
    logs_dir = LOGS_DIR / script_name
    logs_dir.mkdir(parents=True, exist_ok=True)

    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_filename = logs_dir / f"{current_time}.log"

    level = _configured_log_level()
    logger = logging.getLogger(script_name)
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    logging.captureWarnings(True)
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if log_to_console:
        ch = logging.StreamHandler()
        ch.setLevel(max(level, logging.WARNING))
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


def wait_for_user_input() -> None:
    """
    Waits for user input.

    This function prompts the user to press Enter to continue.

    Parameters:
        None

    Returns:
        None
    """
    input("\nPress Enter to continue...")


def create_index_json_files(
    data: dict[str, Any], directory: Path | str = DATA_DIR
) -> None:
    """
    Create JSON files for each item in the given dictionary.

    Args:
        data (dict): The dictionary containing the items to be saved as JSON files.
        directory (str): The directory where the JSON files will be created. Defaults to DATA_DIR.

    Returns:
        None
    """
    Path(directory).mkdir(parents=True, exist_ok=True)

    for key, _details in data.items():
        file_path = Path(directory) / f"{key}_details.json"

        if not file_path.exists():
            file_path.touch()
            print(f"File created: {file_path}")
        else:
            print(f"File already exists: {file_path}")


def create_data_frame_from_scrip_master_csv(file_path: Path) -> pd.DataFrame:
    """
    Create a pandas DataFrame from a CSV file containing scrip master data.

    Args:
        file_path (Path or str): The path to the CSV file.

    Returns:
        df_pd (pandas.DataFrame): The DataFrame created from the CSV file.

    Raises:
        None
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    df_pd = pd.read_csv(file_path)
    print(df_pd.columns)
    df_pd.set_index("Name", inplace=True)
    return df_pd


def create_empty_file_if_not_exists(file_path: Path | str) -> None:
    """Ensures an empty file exists at the specified file path."""
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if not file_path.exists():
        file_path.touch()


def dump_data_to_file(data: Any, file_path: Path | str) -> None:
    """Dumps JSON data to a file atomically."""
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_file_path = None

    try:
        with tempfile.NamedTemporaryFile(
            "w",
            delete=False,
            dir=file_path.parent,
            encoding="utf-8",
            prefix=f".{file_path.name}.",
            suffix=".tmp",
        ) as file:
            tmp_file_path = Path(file.name)
            json.dump(data, file, ensure_ascii=False, indent=4)
            file.flush()
            os.fsync(file.fileno())

        os.replace(tmp_file_path, file_path)
    finally:
        if tmp_file_path is not None and tmp_file_path.exists():
            tmp_file_path.unlink()


def is_file_not_present_or_empty(file_path: Path | str) -> bool:
    """Checks if a file is not present or empty."""
    return not os.path.exists(file_path) or os.path.getsize(file_path) == 0


def read_data_from_file(file_path: Path | str) -> Any | None:
    """Reads JSON data from a file and returns it as a list of dictionaries."""
    if is_file_not_present_or_empty(file_path):
        return None
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError:
        return None


def thread_function(name: str) -> None:
    """Function to run in the background thread."""
    try:
        while True:
            print(f"Thread {name}: updating data...")
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Thread {name} received a signal to terminate")


def _run_background_target(target: Callable[..., Any], *args: Any) -> None:
    """Run a background target and log uncaught exceptions."""
    try:
        target(*args)
    except Exception:
        target_name = getattr(target, "__name__", repr(target))
        logging.getLogger(__name__).exception(
            "Background thread target %s failed", target_name
        )


def run_as_background_thread(
    target: Callable[..., Any], *args: Any
) -> threading.Thread:
    """Runs the given target function as a background daemon thread."""
    target_name = getattr(target, "__name__", "background")
    thread = threading.Thread(
        target=_run_background_target,
        args=(target, *args),
        name=f"trading-{target_name}",
        daemon=True,
    )
    thread.start()
    return thread


def signal_handler(signum: int, frame: Any) -> None:
    """Handle signals to terminate the main script and cleanup resources."""
    print("Signal handler called with signal", signum)
    sys.exit(0)


def setup_signal_handlers() -> None:
    """Setup signal handling to gracefully handle termination."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def create_scrip_code_match(
    symbol: str,
    expiry_date_dt: datetime | str,
    option_type: str,
    option_strike: int | float | str,
) -> str:
    """
    Creates a scrip code match based on the given parameters.

    Args:
        symbol (str): The symbol of the scrip.
        expiry_date_dt (datetime): The expiry date of the option in datetime format.
        option_type (str): The type of the option (e.g., 'CALL', 'PUT').
        option_strike (float): The strike price of the option.

    Returns:
        str: The scrip code match generated based on the given parameters.
    """
    if isinstance(expiry_date_dt, str):
        expiry_date_dt = datetime.strptime(expiry_date_dt, "%Y-%m-%d")
    option_strike = float(option_strike)
    return f"{symbol} {expiry_date_dt.strftime('%d %b %Y')} {option_type} {option_strike:.2f}"


def fetch_scrip_code_from_csv(df_pd: pd.DataFrame, to_match: str) -> Any:
    """
    Get the scrip code for a given name from a CSV file loaded into a pandas DataFrame.

    Args:
    - df_pd: pandas DataFrame containing the CSV file data.
    - to_match: the name of the scrip for which to find the scrip code.

    Returns:
    - Scrip code of the matching scrip name in the DataFrame.

    Raises:
    - ValueError: If the scrip code for the given name is not found in the DataFrame.
    """
    if to_match in df_pd.index:
        scrip_code = df_pd.loc[to_match, "ScripCode"]
        return scrip_code
    raise ValueError(f"Scripcode for {to_match} not found.")


def remove_old_logs(logs_dir: Path, days: int = 2) -> None:
    """
    Removes files in the specified logs directory that are older than the given number of days and
    keeps only the latest three files in each subdirectory of the logs directory.

    Args:
        logs_dir (Path): The path to the directory containing log files.
        days (int): The number of days beyond which a file is considered old and will be deleted.
    """
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(days=days)

    for log_file in logs_dir.rglob("*"):
        if log_file.is_file():
            modification_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            if modification_time < cutoff_time:
                log_file.unlink()

    for subdirectory in [d for d in logs_dir.iterdir() if d.is_dir()]:
        file_list = list(subdirectory.glob("*"))
        file_list = [f for f in file_list if f.is_file()]
        if len(file_list) > 3:
            file_list.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            for file_to_delete in file_list[3:]:
                file_to_delete.unlink()


def mask_mobile_number(mobile_number: str) -> str:
    """
    Masks the middle part of a mobile number.

    Args:
        mobile_number (str): The mobile number to be masked.

    Returns:
        str: The masked mobile number with the first 4 characters and last 2 characters visible.
    """
    masked_part_length = len(mobile_number) - 6
    masked_number = mobile_number[:4] + "x" * masked_part_length + mobile_number[-2:]
    return masked_number
