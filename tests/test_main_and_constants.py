import io
import unittest
from unittest.mock import patch

import pandas as pd

import src.main as main_mod
from src import program_constants as constants


class FakeOrders:
    instances = []

    def __init__(self, client):
        self.client = client
        self.buy_calls = []
        self.sell_calls = []
        self.cancel_calls = 0
        FakeOrders.instances.append(self)

    def place_buy_order_bulk(self, payload, intraday):
        self.buy_calls.append((payload, intraday))

    def place_sell_order_all(self, intraday):
        self.sell_calls.append(intraday)

    def cancel_all_open_orders(self):
        self.cancel_calls += 1


class FakeLogin:
    def __init__(self, account_key, account_details):
        self.account_key = account_key
        self.account_details = account_details

    def login(self):
        return {"client": self.account_key}

    @staticmethod
    def delete_all_session_files(_account_list):
        return None


def run_sync(target, *args):
    target(*args)


class TestConstants(unittest.TestCase):
    def test_index_details_have_required_trading_fields(self):
        required = {
            "symbol",
            "weekly_expiry",
            "monthly_expiry",
            "lot_quantity",
            "max_lot_size",
            "step_size",
            "exchange_segment",
            "exchange_segment_fo",
        }

        self.assertIn("NIFTY", constants.INDEX_DETAILS_FNO)
        for details in constants.INDEX_DETAILS_FNO.values():
            self.assertTrue(required.issubset(details.keys()))

    def test_paths_are_inside_project_root(self):
        self.assertEqual(constants.DATA_DIR.parent, constants.PARENT_DIR)
        self.assertEqual(constants.LOGS_DIR.parent, constants.PARENT_DIR)
        self.assertTrue(str(constants.SCRIP_MASTER_FILE_URL).startswith("https://"))


class TestMain(unittest.TestCase):
    def setUp(self):
        self.original_sessions = main_mod.CLIENT_SESSIONS
        self.original_intraday = main_mod.INTRADAY
        main_mod.CLIENT_SESSIONS = {}
        main_mod.INTRADAY = "MIS"
        FakeOrders.instances = []

    def tearDown(self):
        main_mod.CLIENT_SESSIONS = self.original_sessions
        main_mod.INTRADAY = self.original_intraday

    def test_place_order_for_all_clients_buy_uses_mocked_kotak_engine(self):
        main_mod.CLIENT_SESSIONS = {
            "ACCOUNT_KOTAK_NEO_PRIMARY": object(),
            "ACCOUNT_5PAISA_PRIMARY": object(),
        }
        payload = [[{"trading_symbol": "NIFTY"}]]

        with (
            patch.object(main_mod, "Orders", FakeOrders),
            patch.object(main_mod, "run_as_background_thread", side_effect=run_sync),
        ):
            main_mod.place_order_for_all_clients(
                "buy", [{"ACCOUNT_5PAISA_PRIMARY": payload}]
            )

        self.assertEqual(
            FakeOrders.instances[0].client,
            main_mod.CLIENT_SESSIONS["ACCOUNT_KOTAK_NEO_PRIMARY"],
        )
        self.assertEqual(FakeOrders.instances[0].buy_calls, [(payload, "MIS")])

    def test_place_order_for_all_clients_sell_and_cancel_only_use_kotak_session(self):
        main_mod.CLIENT_SESSIONS = {
            "ACCOUNT_KOTAK_NEO_PRIMARY": "kotak",
            "ACCOUNT_5PAISA_PRIMARY": "five",
        }

        with (
            patch.object(main_mod, "Orders", FakeOrders),
            patch.object(main_mod, "run_as_background_thread", side_effect=run_sync),
        ):
            main_mod.place_order_for_all_clients("sell")
            main_mod.place_order_for_all_clients("cancel")

        self.assertEqual(len(FakeOrders.instances), 2)
        self.assertEqual(FakeOrders.instances[0].client, "kotak")
        self.assertEqual(FakeOrders.instances[0].sell_calls, ["MIS"])
        self.assertEqual(FakeOrders.instances[1].cancel_calls, 1)

    def test_place_order_for_all_clients_rejects_missing_or_invalid_state(self):
        with patch.object(main_mod, "wait_for_user_input") as mock_wait:
            main_mod.place_order_for_all_clients("buy", [])
            main_mod.CLIENT_SESSIONS = {"ACCOUNT_KOTAK_NEO_PRIMARY": object()}
            main_mod.place_order_for_all_clients("bad")

        self.assertEqual(mock_wait.call_count, 2)

    def test_login_to_accounts_uses_mocked_login_and_starts_background_tasks(self):
        secrets = {"ACC1": {"x": 1}, "ACC2": {"x": 2}}

        with (
            patch.object(main_mod, "SECRETS", secrets),
            patch.object(main_mod, "Login", FakeLogin),
            patch.object(main_mod, "clear_screen"),
            patch.object(main_mod, "wait_for_user_input"),
            patch.object(main_mod, "start_background_client_tasks") as mock_start,
            patch("builtins.input", return_value="1"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            main_mod.login_to_accounts()

        self.assertEqual(main_mod.CLIENT_SESSIONS, {"ACC1": {"client": "ACC1"}})
        mock_start.assert_called_once()

    def test_start_background_client_tasks_uses_kotak_as_additional_client(self):
        main_mod.CLIENT_SESSIONS = {
            "ACCOUNT_KOTAK_NEO_PRIMARY": "kotak-client",
            "ACCOUNT_5PAISA_PRIMARY": "five-client",
        }
        created = []

        class FakeBackground:
            def __init__(self, client, account_key, df_pd, additional_client=None):
                created.append((client, account_key, df_pd, additional_client))

            def start_background_client_tasks(self):
                return None

        with patch.object(main_mod, "ProgramBackground", FakeBackground):
            main_mod.start_background_client_tasks()

        self.assertEqual(created[0][3], "kotak-client")
        self.assertEqual(created[1][3], "kotak-client")

    def test_debug_client_interaction_eval_uses_selected_client_only(self):
        class FakeClient:
            def ping(self):
                return "pong"

        main_mod.CLIENT_SESSIONS = {"ACC1": FakeClient()}

        with (
            patch("builtins.input", side_effect=["1", "client.ping()", "exit"]),
            patch.object(main_mod, "clear_screen"),
            patch.object(main_mod, "wait_for_user_input"),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            main_mod.debug_client_interaction()

        self.assertIn("Command result: pong", stdout.getvalue())

    def test_main_menu_can_exit_without_real_startup_work(self):
        with (
            patch.object(main_mod, "get_scrip_master"),
            patch.object(
                main_mod,
                "create_data_frame_from_scrip_master_csv",
                return_value=pd.DataFrame(),
            ),
            patch.object(main_mod, "remove_old_logs"),
            patch.object(main_mod, "clear_screen"),
            patch.object(main_mod.resource, "getrlimit", return_value=(256, 4096)),
            patch.object(main_mod.resource, "setrlimit"),
            patch("builtins.input", return_value="11"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            main_mod.main_menu()

        self.assertTrue(True)
