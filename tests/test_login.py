import pickle
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import httpx

from src import program_login as login_mod


ACCOUNT_DETAILS = {
    "APP_NAME": "app",
    "APP_SOURCE": "source",
    "USER_ID": "user",
    "PASSWORD": "password",
    "USER_KEY": "user-key",
    "ENCRYPTION_KEY": "enc",
    "CONSUMER_KEY": "consumer",
    "CONSUMER_SECRET": "consumer-secret",
    "CLIENT_MOBILE_NUMBER": "9876543210",
    "CLIENT_PASSWORD": "client-password",
    "CLIENT_UCC": "ucc",
    "CLIENT_MPIN": "123456",
    "PIN": "111111",
    "CLIENT_CODE": "client-code",
}


class FakeFivePaisaClient:
    def __init__(self, cred):
        self.cred = cred
        self.session: httpx.Client | None = httpx.Client()
        self.totp_calls = []
        self.authenticated = False

    def get_totp_session(self, client_code, totp, pin):
        self.totp_calls.append((client_code, totp, pin))
        self.authenticated = True

    def Login_check(self):
        return ".ASPXAUTH=ok" if self.authenticated else ".ASPXAUTH=None"


FakeFivePaisaClient.__module__ = "py5paisa.fake"


class FakeNeoAPI:
    NeoWebSocket = object()

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.login_calls = []
        self.validate_calls = []

    def totp_login(self, mobile_number, ucc, totp):
        self.login_calls.append((mobile_number, ucc, totp))
        return {}

    def totp_validate(self, mpin):
        self.validate_calls.append(mpin)
        return {}

    def limits(self):
        return {"Net": "100000"}


class TestLoginHelpers(unittest.TestCase):
    def test_probe_pickle_and_atomic_dump(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.pkl"
            ok, payload, err = login_mod._probe_pickle({"a": 1})

            self.assertTrue(ok)
            self.assertIsNone(err)
            login_mod._atomic_pickle_dump_bytes(cast(bytes, payload), path)

            self.assertEqual(pickle.loads(path.read_bytes()), {"a": 1})
            self.assertFalse((Path(tmpdir) / "session.pkl.tmp").exists())

    def test_httpx_state_extract_and_rebuild(self):
        client = httpx.Client(headers={"X-Test": "yes"}, cookies={"sid": "abc"})

        state = login_mod._extract_httpx_state(client)
        rebuilt = login_mod._rebuild_httpx_from_state(state)

        self.assertEqual(rebuilt.headers["X-Test"], "yes")
        self.assertEqual(rebuilt.cookies["sid"], "abc")
        client.close()
        rebuilt.close()

    def test_ssl_error_detection(self):
        self.assertTrue(
            login_mod._is_ssl_verification_error(
                RuntimeError("SSLError CERTIFICATE_VERIFY_FAILED")
            )
        )
        self.assertFalse(login_mod._is_ssl_verification_error(RuntimeError("other")))


class TestLoginClass(unittest.TestCase):
    def make_login(self, tmpdir, account_name="ACCOUNT_5PAISA_PRIMARY"):
        login = login_mod.Login(account_name, ACCOUNT_DETAILS)
        login.client_session_file = Path(tmpdir) / account_name.lower() / "login.pkl"
        return login

    def test_session_validity_and_corrupt_cache_handling(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir)
            login.client_session_file.parent.mkdir(parents=True)
            login.client_session_file.write_bytes(b"bad")

            self.assertFalse(login._is_session_valid())
            login.client_session_file.write_bytes(b"not a pickle but long enough")
            self.assertIsNone(login._load_client())

    def test_save_and_load_plain_picklable_client(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir)
            client = SimpleNamespace(value=42)

            login._save_client_safely(client)
            loaded = cast(SimpleNamespace, login._load_client())

            self.assertEqual(loaded.value, 42)

    def test_delete_all_session_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            path = data_dir / "account" / "login_information.pkl"
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            path.parent.mkdir()
            path.write_bytes(b"123")
            tmp_path.write_bytes(b"partial")

            with patch.object(login_mod, "DATA_DIR", data_dir):
                login_mod.Login.delete_all_session_files(["ACCOUNT"])

            self.assertFalse(path.exists())
            self.assertFalse(tmp_path.exists())

    def test_delete_old_session_removes_partial_temp_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir)
            tmp_path = login.client_session_file.with_suffix(
                login.client_session_file.suffix + ".tmp"
            )
            login.client_session_file.parent.mkdir(parents=True)
            login.client_session_file.write_bytes(b"123")
            tmp_path.write_bytes(b"partial")

            login._delete_old_session()

            self.assertFalse(login.client_session_file.exists())
            self.assertFalse(tmp_path.exists())

    def test_load_wrapped_5paisa_session_rebuilds_httpx_for_duck_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir)
            login.client_session_file.parent.mkdir(parents=True)
            login.client_session_file.write_bytes(b"placeholder pickle bytes")
            client = FakeFivePaisaClient({})
            client.session = None
            wrapper = {
                "client": client,
                "format": "5paisa+httpx@v1",
                "session_state": {
                    "headers": {"X-Test": "yes"},
                    "cookies": {"sid": "abc"},
                    "base_url": "",
                },
            }

            with patch.object(login_mod.pickle, "load", return_value=wrapper):
                loaded = cast(FakeFivePaisaClient, login._load_client())

            self.assertIs(loaded, client)
            session = cast(httpx.Client, loaded.session)
            self.assertIsInstance(session, httpx.Client)
            self.assertEqual(session.headers["X-Test"], "yes")
            self.assertEqual(session.cookies["sid"], "abc")
            session.close()

    def test_auth_valid_for_5paisa_and_neo(self):
        login = login_mod.Login("ACCOUNT_5PAISA_PRIMARY", ACCOUNT_DETAILS)
        five = FakeFivePaisaClient({})
        neo = FakeNeoAPI()

        self.assertFalse(login._is_auth_valid(five))
        five.authenticated = True
        self.assertTrue(login._is_auth_valid(five))
        self.assertTrue(login._is_auth_valid(neo))

    def test_5paisa_authentication_is_fully_mocked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir, "ACCOUNT_5PAISA_PRIMARY")
            with (
                patch.object(login_mod, "FivePaisaClient", FakeFivePaisaClient),
                patch.object(login_mod, "getpass", return_value="654321"),
            ):
                client = login._authenticate()

        self.assertIsInstance(client, FakeFivePaisaClient)
        self.assertEqual(client.totp_calls, [("client-code", "654321", "111111")])

    def test_kotak_authentication_is_fully_mocked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir, "ACCOUNT_KOTAK_NEO_PRIMARY")
            with (
                patch.object(login_mod, "NeoAPI", FakeNeoAPI),
                patch.object(login_mod, "getpass", return_value="123456"),
                patch.object(
                    login_mod, "configure_requests_ca_bundle", return_value=None
                ),
            ):
                client = login._authenticate()

        self.assertIsInstance(client, FakeNeoAPI)
        self.assertEqual(client.login_calls, [("9876543210", "ucc", "123456")])
        self.assertEqual(client.validate_calls, ["123456"])

    def test_login_uses_valid_cache_without_reauth(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            login = self.make_login(tmpdir)
            cached = FakeFivePaisaClient({})
            cached.authenticated = True
            login._save_client_safely(
                SimpleNamespace(Login_check=lambda: ".ASPXAUTH=ok")
            )

            with (
                patch.object(login, "_is_session_valid", return_value=True),
                patch.object(login, "_load_client", return_value=cached),
                patch.object(login, "_authenticate") as mock_authenticate,
                patch.object(login_mod, "configure_requests_ca_bundle"),
            ):
                result = login.login()

        self.assertIs(result, cached)
        mock_authenticate.assert_not_called()
