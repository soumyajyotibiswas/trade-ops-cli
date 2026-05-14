import io
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

import pandas as pd

from src import program_background as background_mod
from src import program_display as display_mod
from src.program_background import ProgramBackground
from src.program_display import ProgramDisplay
from src.program_helpers import dump_data_to_file, read_data_from_file


class FakePlainClient:
    pass


class FakeNeoClient:
    NeoWebSocket = object()


class StopLoop(Exception):
    pass


def sleep_once_then_stop_factory():
    calls = {"count": 0}

    def fake_sleep(_seconds):
        calls["count"] += 1
        if calls["count"] >= 2:
            raise StopLoop()

    return fake_sleep


class TestProgramDisplay(unittest.TestCase):
    def make_data_dir(self, root):
        client_dir = Path(root) / "account_5paisa_primary"
        client_dir.mkdir(parents=True)
        dump_data_to_file(
            {
                "index": "NIFTY",
                "quote": 23456.7,
                "current_week_expiry_date": "2026-05-19",
            },
            client_dir / "NIFTY.json",
        )
        dump_data_to_file(
            [
                {
                    "Index_Symbol": "NIFTY 19 MAY 2026 CE 23400.00",
                    "OptionType": "CE",
                    "LastRate": 100,
                    "High": 110,
                    "Low": 90,
                    "Quantity_to_Purchase": 65,
                    "Client_Margin": 100000,
                    "BulkOrderList": [[{"trading_symbol": "NIFTY2651923400CE"}]],
                },
                {
                    "Index_Symbol": "NIFTY 19 MAY 2026 PE 23400.00",
                    "OptionType": "PE",
                    "LastRate": 95,
                    "High": 100,
                    "Low": 80,
                    "Quantity_to_Purchase": 65,
                    "Client_Margin": 100000,
                    "BulkOrderList": [[{"trading_symbol": "NIFTY2651923400PE"}]],
                },
            ],
            client_dir / "NIFTY_options.json",
        )

    def test_dynamic_table_and_bulk_order_lookup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.make_data_dir(tmpdir)
            display = ProgramDisplay(
                {"ACCOUNT_5PAISA_PRIMARY": object()}, {"NIFTY": {}}
            )

            with patch.object(display_mod, "DATA_DIR", Path(tmpdir)):
                df, index_quote = display.create_dynamic_table(
                    {"ACCOUNT_5PAISA_PRIMARY": object()}, "NIFTY"
                )
                bulk_lists = display.get_bulk_order_lists_by_serial_number(df, 1)

        self.assertEqual(index_quote, 23456.7)
        self.assertEqual(list(df["S.No."]), [1, 2])
        self.assertEqual(df.iloc[0]["Symbol"], "NIFTY 19 MAY 2026 CE 23400.00")
        self.assertEqual(
            bulk_lists[0]["ACCOUNT_5PAISA_PRIMARY"][0][0]["trading_symbol"],
            "NIFTY2651923400CE",
        )

    def test_dynamic_table_raises_for_missing_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            display = ProgramDisplay(
                {"ACCOUNT_5PAISA_PRIMARY": object()}, {"NIFTY": {}}
            )

            with patch.object(display_mod, "DATA_DIR", Path(tmpdir)):
                with self.assertRaises(ValueError):
                    display.create_dynamic_table(
                        {"ACCOUNT_5PAISA_PRIMARY": object()}, "NIFTY"
                    )

    def test_menu_helpers_do_not_change_choices(self):
        display = ProgramDisplay({}, {"NIFTY": {}, "SENSEX": {}})

        self.assertEqual(display.get_menu_options(["a", "b"]), [1, 2])
        self.assertTrue(display.validate_user_choice(1, ["a"]))

        with (
            patch.object(display, "take_user_input", return_value="2"),
            patch.object(display, "clear_screen"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertEqual(display.place_buy_order_choose_index_submenu(), "SENSEX")

        with (
            patch.object(display, "take_user_input", return_value="r"),
            patch.object(display, "clear_screen"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertEqual(display.place_buy_order_choose_index_submenu(), "r")

    def test_display_option_submenu_returns_refresh_or_bulk_list(self):
        display = ProgramDisplay({"ACCOUNT_5PAISA_PRIMARY": object()}, {"NIFTY": {}})
        df = pd.DataFrame(
            {
                "S.No.": [1],
                "Symbol": ["NIFTY"],
                "ACCOUNT_5PAISA_PRIMARY-BulkOrderList-Hidden": [[{"x": 1}]],
            }
        )

        with (
            patch.object(display, "create_dynamic_table", return_value=(df, 1)),
            patch.object(display, "clear_screen"),
            patch.object(display, "pretty_print_data_frame"),
            patch.object(display, "take_user_input", return_value=""),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertEqual(
                display.display_option_data_menu_to_user_submenu("NIFTY"), "r"
            )

        with (
            patch.object(display, "create_dynamic_table", return_value=(df, 1)),
            patch.object(display, "clear_screen"),
            patch.object(display, "pretty_print_data_frame"),
            patch.object(display, "take_user_input", return_value="1"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertEqual(
                display.display_option_data_menu_to_user_submenu("NIFTY"),
                [{"ACCOUNT_5PAISA_PRIMARY": [{"x": 1}]}],
            )


class TestProgramBackground(unittest.TestCase):
    def make_background(self, tmpdir):
        with patch.object(background_mod, "DATA_DIR", Path(tmpdir)):
            return ProgramBackground(
                FakePlainClient(),
                "ACCOUNT_5PAISA_PRIMARY",
                pd.DataFrame(),
                additional_client=FakeNeoClient(),
            )

    def test_constructor_creates_client_directory_and_selects_profile_client(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bg = self.make_background(tmpdir)

            self.assertTrue((Path(tmpdir) / "account_5paisa_primary").exists())
            self.assertIsInstance(bg.client, FakePlainClient)
            self.assertIsInstance(bg.additional_client, FakeNeoClient)

    def test_start_background_client_tasks_selects_expected_task_set(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plain_bg = self.make_background(tmpdir)
            for name in [
                "store_index_quotes_to_file",
                "store_client_margin_to_file",
                "store_completed_buy_order_count_to_file",
                "store_index_option_quotes_to_file",
                "store_client_open_positions_to_file",
                "store_symbol_price_lookup_to_file",
            ]:
                setattr(plain_bg, name, Mock())

            plain_bg.start_background_client_tasks()

            cast(Mock, plain_bg.store_index_quotes_to_file).assert_called_once()
            cast(Mock, plain_bg.store_symbol_price_lookup_to_file).assert_called_once()

            with patch.object(background_mod, "DATA_DIR", Path(tmpdir)):
                neo_bg = ProgramBackground(
                    FakeNeoClient(),
                    "ACCOUNT_KOTAK_NEO_PRIMARY",
                    pd.DataFrame(),
                    additional_client=FakeNeoClient(),
                )
            neo_bg.store_client_margin_to_file = Mock()
            neo_bg.store_index_quotes_to_file = Mock()

            neo_bg.start_background_client_tasks()

            cast(Mock, neo_bg.store_client_margin_to_file).assert_called_once()
            cast(Mock, neo_bg.store_index_quotes_to_file).assert_not_called()

    def test_background_store_methods_launch_daemon_targets_without_running_threads(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            bg = self.make_background(tmpdir)
            launched = []

            def capture(target, *args):
                launched.append((target.__name__, args))

            with (
                patch.object(
                    background_mod, "run_as_background_thread", side_effect=capture
                ),
                patch.object(background_mod, "INDEX_DETAILS_FNO", {"NIFTY": {}}),
            ):
                bg.store_client_open_positions_to_file()
                bg.store_index_quotes_to_file()
                bg.store_client_margin_to_file()
                bg.store_completed_buy_order_count_to_file()
                bg.store_index_option_quotes_to_file()
                bg.store_symbol_price_lookup_to_file()

        self.assertEqual(len(launched), 6)
        self.assertIn(("store_index_quotes_to_file_t", ("NIFTY",)), launched)

    def test_symbol_price_lookup_merges_option_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bg = self.make_background(tmpdir)
            dump_data_to_file(
                [
                    {
                        "timestamp": "2026-05-14 09:40:00 +5:30",
                        "BulkOrderList": [
                            [{"trading_symbol": "NIFTY2651923400CE", "price": "105"}]
                        ],
                    }
                ],
                bg.client_dir_path / "NIFTY_options.json",
            )
            launched = []

            with patch.object(
                background_mod,
                "run_as_background_thread",
                side_effect=lambda t, *a: launched.append(t),
            ):
                bg.store_symbol_price_lookup_to_file()

            with patch.object(
                background_mod.time, "sleep", side_effect=sleep_once_then_stop_factory()
            ):
                with self.assertRaises(StopLoop):
                    launched[0]()

            lookup = cast(
                dict[str, dict[str, Any]],
                read_data_from_file(bg.client_dir_path / "symbol_price_lookup.json"),
            )

        self.assertEqual(lookup["NIFTY2651923400CE"]["price"], 105.0)
        self.assertEqual(lookup["NIFTY2651923400CE"]["index"], "NIFTY")

    def test_option_symbol_transform_uses_weekly_month_code_before_last_week(self):
        self.assertEqual(
            background_mod._transform_symbol("NIFTY 19 MAY 2026 CE 23400.00"),
            "NIFTY2651923400CE",
        )
        self.assertEqual(
            background_mod._transform_symbol("NIFTY 02 JUN 2026 PE 23000.00"),
            "NIFTY2660223000PE",
        )
        self.assertEqual(
            background_mod._transform_symbol("NIFTY 19 OCT 2026 CE 25000.00"),
            "NIFTY26O1925000CE",
        )

    def test_option_symbol_transform_uses_month_name_in_last_week(self):
        self.assertEqual(
            background_mod._transform_symbol("NIFTY 26 MAY 2026 CE 23400.00"),
            "NIFTY26MAY23400CE",
        )
        self.assertEqual(
            background_mod._transform_symbol("SENSEX 27 MAY 2026 CE 80000.00"),
            "SENSEX26MAY80000CE",
        )
        self.assertEqual(
            background_mod._transform_symbol("NIFTY 30 JUN 2026 PE 23000.00"),
            "NIFTY26JUN23000PE",
        )

    def test_index_option_quote_writer_builds_current_options_file_offline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(
                {
                    "Name": [
                        "NIFTY 19 MAY 2026 CE 23400.00",
                        "NIFTY 19 MAY 2026 PE 23400.00",
                    ],
                    "ScripCode": [111, 222],
                }
            ).set_index("Name")
            with patch.object(background_mod, "DATA_DIR", Path(tmpdir)):
                bg = ProgramBackground(
                    FakePlainClient(),
                    "ACCOUNT_5PAISA_PRIMARY",
                    df,
                    additional_client=FakeNeoClient(),
                )
            dump_data_to_file(
                {"quote": 23456.7, "current_week_expiry_date": "2026-05-19"},
                bg.client_dir_path / "NIFTY.json",
            )
            dump_data_to_file({"available_margin": 100000}, bg.client_margin_file_path)
            bg.quotes.get_opt_strike_price_list = Mock(return_value=["23400"])
            bg.quotes.get_ltp_for_opt_strike_price = Mock(
                return_value={
                    "Data": [
                        {"LastRate": 100, "High": 110, "Low": 90, "Symbol": "CE"},
                        {"LastRate": 120, "High": 125, "Low": 100, "Symbol": "PE"},
                    ]
                }
            )
            launched = []

            with (
                patch.object(
                    background_mod,
                    "run_as_background_thread",
                    side_effect=lambda t, *a: launched.append((t, a)),
                ),
                patch.object(background_mod, "INDEX_DETAILS_FNO", {"NIFTY": {}}),
            ):
                bg.store_index_option_quotes_to_file()

            target, args = launched[0]
            with (
                patch.object(background_mod, "disable_loguru_to_devnull"),
                patch.object(
                    background_mod.time,
                    "sleep",
                    side_effect=sleep_once_then_stop_factory(),
                ),
            ):
                with self.assertRaises(StopLoop):
                    target(*args)

            options = cast(
                list[dict[str, Any]],
                read_data_from_file(bg.client_dir_path / "NIFTY_options.json"),
            )

        self.assertEqual(len(options), 2)
        self.assertEqual(options[0]["ScripCode"], 111)
        self.assertEqual(options[0]["Quantity_to_Purchase"], 975)
        self.assertEqual(options[0]["BulkOrderList"][0][0]["transaction_type"], "B")
