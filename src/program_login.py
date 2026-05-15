# ruff: noqa: E402

"""
Login/session management with safe pickling for 5paisa (httpx.Client inside)
and normal pickling for Kotak Neo.

Key improvements:
- Robust load: handles empty/corrupt pickle (EOFError/UnpicklingError) -> delete + re-auth.
- Safe save: for 5paisa, remove unpicklable httpx.Client before pickling; store session_state
  (headers/cookies/base_url) separately and rebuild on load.
- Atomic writes: temp file + replace to avoid zero-byte cache after interruptions.
"""

from __future__ import annotations

import io
import os
import pickle
import stat
import sys
import time
from datetime import datetime, timedelta
from getpass import getpass
from pathlib import Path
from typing import Any, cast

import httpx
from neo_api_client import NeoAPI
from py5paisa import FivePaisaClient

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.program_constants import DATA_DIR, ENVIRONMENT, PICKLE_DATA_AGE, TOKEN_EXPIRY
from src.program_helpers import (
    configure_requests_ca_bundle,
    mask_mobile_number,
    setup_logging,
)

log = setup_logging("program_login")


def _session_temp_path(dest_path: Path) -> Path:
    """Return the temporary path used for atomic session-cache writes."""
    return dest_path.with_suffix(dest_path.suffix + ".tmp")


def _atomic_pickle_dump_bytes(payload: bytes, dest_path: Path) -> None:
    """Write pickled bytes atomically with owner-only file permissions when possible."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _session_temp_path(dest_path)
    with open(tmp, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        log.debug("Unable to chmod temporary session cache %s", tmp, exc_info=True)
    tmp.replace(dest_path)


def _probe_pickle(obj: Any) -> tuple[bool, bytes | None, Exception | None]:
    """Try pickling an object and return the result without raising."""
    try:
        b = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        return True, b, None
    except Exception as e:
        return False, None, e


def _extract_httpx_state(sess: httpx.Client) -> dict[str, Any]:
    """
    Pull minimal serializable state out of an httpx.Client:
    - headers (as {str: str})
    - cookies (as {str: str})
    - base_url (as str or "")
    """
    try:
        headers = {str(k): str(v) for k, v in sess.headers.items()}
    except Exception:
        headers = {}
    try:
        cookies = {str(k): str(v) for k, v in sess.cookies.items()}
    except Exception:
        cookies = {}
    try:
        base_url = str(getattr(sess, "base_url", "") or "")
    except Exception:
        base_url = ""
    return {"headers": headers, "cookies": cookies, "base_url": base_url}


def _rebuild_httpx_from_state(state: dict[str, Any]) -> httpx.Client:
    """Rebuild a minimal httpx.Client from serialized headers/cookies/base URL."""
    headers = state.get("headers") or {}
    cookies = state.get("cookies") or {}
    base_url = state.get("base_url") or ""
    kw = {}
    if base_url:
        kw["base_url"] = base_url
    return httpx.Client(headers=headers, cookies=cookies, **kw)


def _is_5paisa(obj: Any) -> bool:
    """Return True when an object is from the py5paisa SDK package."""
    return obj is not None and obj.__class__.__module__.startswith("py5paisa.")


def _is_ssl_verification_error(exc: Exception) -> bool:
    """Return True when an exception message looks like certificate failure."""
    msg = str(exc)
    return "SSLError" in msg or "CERTIFICATE_VERIFY_FAILED" in msg


class Login:
    """
    Represents a login session for the trading program.

    Attributes:
        client_session_file: DATA_DIR/<account_name>/login_information.pkl
        client: The active SDK client instance (FivePaisaClient or NeoAPI)
    """

    def __init__(self, account_name: str, account_details: dict[str, str]) -> None:
        """Build a login manager for one configured account."""
        self.account_name = account_name
        self.account_details = account_details

        self.cred_5paisa = {
            "APP_NAME": account_details["APP_NAME"],
            "APP_SOURCE": account_details["APP_SOURCE"],
            "USER_ID": account_details["USER_ID"],
            "PASSWORD": account_details["PASSWORD"],
            "USER_KEY": account_details["USER_KEY"],
            "ENCRYPTION_KEY": account_details["ENCRYPTION_KEY"],
        }
        self.cred_kotak = {
            "CONSUMER_KEY": account_details["CONSUMER_KEY"],
            "CONSUMER_SECRET": account_details["CONSUMER_SECRET"],
            "CLIENT_MOBILE_NUMBER": account_details["CLIENT_MOBILE_NUMBER"],
            "CLIENT_PASSWORD": account_details["CLIENT_PASSWORD"],
            "CLIENT_UCC": account_details.get("CLIENT_UCC", ""),
            "CLIENT_MPIN": account_details.get("CLIENT_MPIN", ""),
        }
        self.pin = account_details["PIN"]
        self.client_code = account_details["CLIENT_CODE"]

        self.client_session_file = (
            DATA_DIR / account_name.lower() / "login_information.pkl"
        )
        self.client: Any | None = None

        self.environment = ENVIRONMENT
        self.token_expires_in = TOKEN_EXPIRY
        self.expiry: datetime | None = None

    def login(self) -> Any:
        """
        Load a cached client if fresh and valid; otherwise authenticate and cache safely.
        """
        log.info("Logging in for account '%s'.", self.account_name)
        ca_env = (
            "KOTAK_REQUESTS_CA_BUNDLE"
            if "kotak" in self.account_name.lower()
            else "FIVEPAISA_REQUESTS_CA_BUNDLE"
        )
        configure_requests_ca_bundle(ca_env)
        if self._is_session_valid():
            cached = self._load_client()
            if cached is not None and self._is_auth_valid(cached):
                log.info("Using cached client session for '%s'.", self.account_name)
                self.client = cached
                return self.client
            self._delete_old_session()

        log.info("No valid cached session; authenticating '%s'.", self.account_name)
        self.client = self._authenticate()
        log.info("Authentication successful for '%s'.", self.account_name)
        self._save_client_safely(self.client)
        return self.client

    def logout(self) -> None:
        """Best-effort logout to avoid noisy exits."""
        try:
            if self.client is not None and hasattr(self.client, "logout"):
                self.client.logout()
        except Exception as e:
            log.warning("Logout raised an exception and was ignored: %s", e)

    @staticmethod
    def delete_all_session_files(account_list: list[str]) -> None:
        """Remove cached session files for provided accounts."""
        for account in account_list:
            p = DATA_DIR / account.lower() / "login_information.pkl"
            for session_file in (p, _session_temp_path(p)):
                try:
                    if session_file.exists():
                        session_file.unlink()
                        log.info("Deleted cached session. account=%s", account)
                except Exception as e:
                    log.warning(
                        "Failed to delete cached session. account=%s file=%s error=%s",
                        account,
                        session_file,
                        e,
                    )

    def _authenticate(self) -> Any:
        """Authenticate with the trading API and return an SDK client."""
        log.info("Starting authentication for '%s'.", self.account_name)
        if "kotak" not in self.account_name.lower():
            log.info(
                "[5paisa] Initializing FivePaisaClient for '%s'.", self.account_name
            )
            client = FivePaisaClient(cred=self.cred_5paisa)
            max_retries = 2
            while max_retries > 0:
                log.debug(
                    "[5paisa] TOTP attempt %d/%d for '%s'.",
                    3 - max_retries,
                    2,
                    self.account_name,
                )
                totp = getpass(
                    f"Enter the TOTP for '{self.account_name}' using gAuthenticator: "
                )
                log.debug(
                    "[5paisa] Calling get_totp_session for '%s'.", self.account_name
                )
                client.get_totp_session(self.client_code, totp, self.pin)
                log.debug(
                    "[5paisa] get_totp_session completed, validating auth for '%s'.",
                    self.account_name,
                )
                if self._is_auth_valid(client):
                    log.info(
                        "[5paisa] Authentication successful for '%s'.",
                        self.account_name,
                    )
                    return client
                log.warning(
                    "[5paisa] Authentication failed for '%s', retries left: %d.",
                    self.account_name,
                    max_retries - 1,
                )
                max_retries -= 1
            log.error(
                "[5paisa] Authentication failed after all retries for '%s'.",
                self.account_name,
            )
        else:
            log.info("[Kotak] Initializing NeoAPI for '%s'.", self.account_name)
            ca_bundle = configure_requests_ca_bundle("KOTAK_REQUESTS_CA_BUNDLE")
            if ca_bundle:
                log.info("[Kotak] Using configured CA bundle: %s", ca_bundle)
            else:
                log.warning(
                    "[Kotak] No requests CA bundle could be configured; using requests default."
                )
            original_stdout = sys.stdout
            sys.stdout = io.StringIO()
            try:
                print(
                    f"environment: {self.environment}, consumer_key: {self.cred_kotak['CONSUMER_KEY']}, client_mobile: {mask_mobile_number(self.cred_kotak['CLIENT_MOBILE_NUMBER'])}, ucc: {self.cred_kotak['CLIENT_UCC']}, mpin: {'*' * len(self.cred_kotak['CLIENT_MPIN'])}"
                )
                client = NeoAPI(
                    environment=self.environment,
                    access_token=None,
                    neo_fin_key=None,
                    consumer_key=self.cred_kotak["CONSUMER_KEY"],
                )
                log.info(
                    "[Kotak] NeoAPI initialized successfully for '%s'.",
                    self.account_name,
                )
            finally:
                sys.stdout = original_stdout

            self.expiry = datetime.now() + timedelta(seconds=self.token_expires_in)
            mobile_number = self.cred_kotak["CLIENT_MOBILE_NUMBER"]
            log.info("[Kotak] Starting TOTP login process for '%s'.", self.account_name)
            for _totp_try in range(3):
                log.debug(
                    "[Kotak] TOTP attempt %d/3 for '%s'.",
                    _totp_try + 1,
                    self.account_name,
                )
                totp = (
                    getpass(
                        f"Enter the TOTP for '{self.account_name}' using gAuthenticator: "
                    )
                    .strip()
                    .zfill(6)
                )
                log.debug(
                    "[Kotak] TOTP entered, calling totp_login for '%s'.",
                    self.account_name,
                )

                for retry in range(5):
                    log.debug(
                        "[Kotak][totp_login] Retry %d/5 for '%s'.",
                        retry + 1,
                        self.account_name,
                    )
                    try:
                        resp = client.totp_login(
                            mobile_number=mobile_number,
                            ucc=self.cred_kotak["CLIENT_UCC"],
                            totp=totp,
                        )
                    except Exception as e:
                        if _is_ssl_verification_error(e):
                            raise RuntimeError(
                                "Kotak SSL certificate verification failed. "
                                "Set KOTAK_REQUESTS_CA_BUNDLE, REQUESTS_CA_BUNDLE, "
                                "or SSL_CERT_FILE to a CA file that trusts your "
                                "network root CA."
                            ) from e
                        raise
                    log.debug("[Kotak][totp_login] Response: %s", resp)

                    if isinstance(resp, dict) and "error" in resp:
                        err = cast(dict[str, Any], resp["error"][0])
                        if err.get("code") == 424 and "does not exist" in str(
                            err.get("message", "")
                        ):
                            log.warning(
                                "[Kotak][totp_login] consumer key not found, retrying..."
                            )
                            time.sleep(1)
                            continue
                        log.error("[Kotak][totp_login] Error response: %s", resp)
                        raise Exception(resp)

                    log.info("[Kotak][totp_login] Success for '%s'.", self.account_name)
                    break
                else:
                    log.warning(
                        "[Kotak][totp_login] 5 retries exhausted, asking for new TOTP for '%s'.",
                        self.account_name,
                    )
                    continue

                log.info(
                    "[Kotak][totp_login] TOTP login successful for '%s'.",
                    self.account_name,
                )
                break
            else:
                log.error(
                    "[Kotak] TOTP login failed after 3 TOTPs for '%s'.",
                    self.account_name,
                )
                raise Exception(
                    "Kotak totp_login failed after 3 TOTPs (5 retries each)"
                )

            log.info("[Kotak] Starting TOTP validation for '%s'.", self.account_name)
            max_retries = 5
            attempt = 1
            while max_retries > 0:
                try:
                    log.debug(
                        "[Kotak][totp_validate] Attempt %d/5 for '%s'.",
                        attempt,
                        self.account_name,
                    )
                    start = time.time()
                    resp = client.totp_validate(mpin=self.cred_kotak["CLIENT_MPIN"])
                    took = time.time() - start
                    log.debug("[Kotak][totp_validate] Response: %s", resp)

                    if isinstance(resp, dict) and "error" in resp:
                        log.warning(
                            f"[Kotak][totp_validate] attempt={attempt} "
                            f"took={took:.2f}s resp={resp}"
                        )
                        raise Exception(resp)

                    log.info(
                        f"[Kotak][totp_validate] attempt={attempt} "
                        f"took={took:.2f}s SUCCESS"
                    )
                    log.info(
                        "[Kotak] Authentication fully successful for '%s'.",
                        self.account_name,
                    )
                    return client

                except Exception as e:
                    log.warning(
                        "[Kotak][totp_validate] attempt=%d failed: %s",
                        attempt,
                        e,
                    )
                    max_retries -= 1
                    attempt += 1
                    if max_retries == 0:
                        log.error(
                            "[Kotak][totp_validate] All retries exhausted for '%s'.",
                            self.account_name,
                        )
                        raise
                    time.sleep(1)

            return client

        raise Exception(f"Authentication failed for user '{self.account_name}'.")

    def _save_client_safely(self, client: Any) -> None:
        """
        Save client cache robustly.
        - Try normal pickling first.
        - If that fails and it's a 5paisa client with an httpx session, strip the session,
          capture a tiny session_state, and pickle {'client': <stripped>, 'session_state': {...}}.
        - If even that fails, skip caching (do not fail login).
        """
        ok, payload, _err = _probe_pickle(client)
        if ok and payload:
            _atomic_pickle_dump_bytes(payload, self.client_session_file)
            log.info("Client session cache saved. file=%s", self.client_session_file)
            return

        if (
            _is_5paisa(client)
            and hasattr(client, "session")
            and isinstance(getattr(client, "session"), httpx.Client)
        ):
            try:
                sess: httpx.Client = getattr(client, "session")
                session_state = _extract_httpx_state(sess)

                original_session = sess
                setattr(client, "session", None)
                try:
                    wrapper = {
                        "client": client,
                        "session_state": session_state,
                        "format": "5paisa+httpx@v1",
                    }
                    ok2, payload2, _err2 = _probe_pickle(wrapper)
                    if ok2 and payload2:
                        _atomic_pickle_dump_bytes(payload2, self.client_session_file)
                        log.info(
                            "Client session cache saved with detached httpx state. file=%s",
                            self.client_session_file,
                        )
                        return
                    else:
                        log.warning(
                            "Skipping cache save because wrapper is not picklable: %s",
                            _err2,
                        )
                        pass
                finally:
                    setattr(client, "session", original_session)
            except Exception as e:
                log.warning("Skipping cache save; httpx detach failed: %s", e)
                return

        log.warning("Skipping cache save because client is not picklable: %s", _err)

    def _load_client(self) -> Any | None:
        """
        Load client from pickle. Supports two formats:
        - Legacy: the raw SDK client object.
        - New 5paisa wrapper: {'client': <FivePaisaClient with session=None>, 'session_state': {...}}
        Returns None on any load/corruption issue.
        """
        try:
            with open(self.client_session_file, "rb") as f:
                obj = pickle.load(f)
        except (EOFError, pickle.UnpicklingError) as e:
            log.warning(
                "Cached session is invalid or corrupt. file=%s error=%s",
                self.client_session_file,
                e,
            )
            return None
        except FileNotFoundError:
            return None
        except Exception as e:
            log.warning(
                "Failed to load cached session. file=%s error=%s",
                self.client_session_file,
                e,
            )
            return None

        if (
            isinstance(obj, dict)
            and obj.get("format") == "5paisa+httpx@v1"
            and _is_5paisa(obj.get("client"))
        ):
            client = obj["client"]
            state = obj.get("session_state") or {}
            try:
                rebuilt = _rebuild_httpx_from_state(state)
                setattr(client, "session", rebuilt)
            except Exception as e:
                log.warning("Failed to rebuild httpx.Client from cache state: %s", e)
                return None
            return client

        return obj

    def _delete_old_session(self) -> None:
        """Remove stale session cache files for this account."""
        for session_file in (
            self.client_session_file,
            _session_temp_path(self.client_session_file),
        ):
            try:
                if session_file.exists():
                    session_file.unlink()
                    log.info("Deleted stale cached session. file=%s", session_file)
            except Exception as e:
                log.warning(
                    "Failed to delete session file. file=%s error=%s", session_file, e
                )

    def _is_session_valid(self) -> bool:
        """
        Cache is valid if:
        - file exists,
        - size looks sane (> 16 bytes to avoid EOF on empty),
        - mtime within PICKLE_DATA_AGE hours.
        """
        p = self.client_session_file
        if not p.exists():
            return False
        try:
            if p.stat().st_size < 16:
                return False
            file_mod_time = datetime.fromtimestamp(p.stat().st_mtime)
            return datetime.now() - file_mod_time < timedelta(hours=PICKLE_DATA_AGE)
        except Exception:
            return False

    def _is_auth_valid(self, client: Any) -> bool:
        """
        FivePaisa: Login_check() returns '.ASPXAUTH=None' if unauthenticated.
        Kotak Neo: if 'NeoWebSocket' attr is present, client.limits() should raise if invalid.
        """
        try:
            if "NeoWebSocket" in dir(client):
                client.limits()
                return True
            if hasattr(client, "Login_check"):
                return client.Login_check() != ".ASPXAUTH=None"
            return False
        except Exception as e:
            log.warning("Cached authentication check failed: %s", e)
            return False
