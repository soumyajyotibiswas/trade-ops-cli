import io
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from src import program_helpers as helpers


class TestProgramHelpers(unittest.TestCase):
    def test_configure_requests_ca_bundle_prefers_explicit_override(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ca_file = Path(tmpdir) / "corp-ca.pem"
            ca_file.write_text("certificate", encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "CUSTOM_CA": str(ca_file),
                    "REQUESTS_CA_BUNDLE": "",
                    "SSL_CERT_FILE": "",
                    "CURL_CA_BUNDLE": "",
                },
                clear=False,
            ):
                selected = helpers.configure_requests_ca_bundle("CUSTOM_CA")
                self.assertEqual(os.environ["REQUESTS_CA_BUNDLE"], str(ca_file))
                self.assertEqual(os.environ["SSL_CERT_FILE"], str(ca_file))

            self.assertEqual(selected, str(ca_file))

    def test_account_config_helpers(self):
        config = {"A": {"token": "1"}, "B": {"token": "2"}}

        self.assertEqual(helpers.get_account_config("A", config), {"token": "1"})
        self.assertEqual(helpers.get_account_config("Z", config), "Account not found")
        self.assertEqual(helpers.get_account_names_from_config(config), ["A", "B"])

    def test_get_scrip_master_skips_fresh_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scrip_master = Path(tmpdir) / "ScripMaster.csv"
            scrip_master.write_text("Name,ScripCode\nNIFTY,1\n", encoding="utf-8")

            with (
                patch.object(helpers, "SCRIP_MASTER_FILE_PATH", scrip_master),
                patch.object(helpers.requests, "get") as mock_get,
                patch("sys.stdout", new_callable=io.StringIO),
            ):
                helpers.get_scrip_master()

        mock_get.assert_not_called()

    def test_get_scrip_master_downloads_when_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            scrip_master = data_dir / "ScripMaster.csv"
            response = Mock()
            response.content = b"Name,ScripCode\nNIFTY,1\n"
            response.raise_for_status.return_value = None

            with (
                patch.object(helpers, "DATA_DIR", data_dir),
                patch.object(helpers, "SCRIP_MASTER_FILE_PATH", scrip_master),
                patch.object(
                    helpers, "configure_requests_ca_bundle", return_value=None
                ),
                patch.object(
                    helpers.requests, "get", return_value=response
                ) as mock_get,
                patch("sys.stdout", new_callable=io.StringIO),
            ):
                helpers.get_scrip_master()

            self.assertEqual(scrip_master.read_bytes(), response.content)
            mock_get.assert_called_once_with(
                helpers.SCRIP_MASTER_FILE_URL,
                timeout=300,
                verify=True,
            )

    def test_get_scrip_master_prints_request_error_without_raising(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scrip_master = Path(tmpdir) / "ScripMaster.csv"

            with (
                patch.object(helpers, "SCRIP_MASTER_FILE_PATH", scrip_master),
                patch.object(helpers, "DATA_DIR", Path(tmpdir)),
                patch.object(
                    helpers, "configure_requests_ca_bundle", return_value=None
                ),
                patch.object(
                    helpers.requests,
                    "get",
                    side_effect=requests.RequestException("boom"),
                ),
                patch("builtins.print") as mock_print,
            ):
                helpers.get_scrip_master()

            self.assertTrue(
                any("boom" in str(call) for call in mock_print.call_args_list)
            )

    def test_continue_or_back_and_wait_for_user_input(self):
        with (
            patch("builtins.input", return_value="Y"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertEqual(helpers.continue_or_back(), "Y")

        with (
            patch("builtins.input", return_value="bad"),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertFalse(helpers.continue_or_back())

        with patch("builtins.input", return_value="") as mock_input:
            helpers.wait_for_user_input()
        mock_input.assert_called_once()

    def test_clear_screen_uses_platform_command(self):
        with (
            patch.object(helpers.os, "name", "posix"),
            patch.object(helpers.os, "system", return_value=0) as mock_system,
        ):
            helpers.clear_screen()
        mock_system.assert_called_once_with("clear")

        with (
            patch.object(helpers.os, "name", "nt"),
            patch.object(helpers.os, "system", return_value=0) as mock_system,
        ):
            helpers.clear_screen()
        mock_system.assert_called_once_with("cls")

    def test_setup_logging_creates_log_file_and_logger(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(helpers, "LOGS_DIR", Path(tmpdir)):
                logger = helpers.setup_logging("unit_test_logger")
                logger_again = helpers.setup_logging("unit_test_logger")

            logger.info("hello")
            log_files = list((Path(tmpdir) / "unit_test_logger").glob("*.log"))

        self.assertEqual(logger, logger_again)
        self.assertEqual(len(logger.handlers), 1)
        self.assertGreaterEqual(len(log_files), 1)
        self.assertEqual(logger.name, "unit_test_logger")

    def test_create_index_json_files_uses_path_objects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir) / "indexes"

            with patch("sys.stdout", new_callable=io.StringIO):
                helpers.create_index_json_files({"NIFTY": {}, "SENSEX": {}}, directory)
                helpers.create_index_json_files({"NIFTY": {}}, directory)

            self.assertTrue((directory / "NIFTY_details.json").exists())
            self.assertTrue((directory / "SENSEX_details.json").exists())

    def test_create_dataframe_and_fetch_scrip_code(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "scrip.csv"
            csv_path.write_text(
                "Name,ScripCode\nNIFTY 19 MAY 2026 CE 23400.00,12345\n",
                encoding="utf-8",
            )

            with patch("sys.stdout", new_callable=io.StringIO):
                df = helpers.create_data_frame_from_scrip_master_csv(csv_path)

        self.assertEqual(
            helpers.fetch_scrip_code_from_csv(df, "NIFTY 19 MAY 2026 CE 23400.00"),
            12345,
        )
        with self.assertRaises(ValueError):
            helpers.fetch_scrip_code_from_csv(df, "MISSING")

    def test_json_file_helpers_round_trip_atomically(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "nested" / "data.json"

            self.assertTrue(helpers.is_file_not_present_or_empty(file_path))
            helpers.create_empty_file_if_not_exists(file_path)
            self.assertTrue(helpers.is_file_not_present_or_empty(file_path))

            helpers.dump_data_to_file({"a": [1, 2]}, file_path)

            self.assertEqual(helpers.read_data_from_file(file_path), {"a": [1, 2]})
            self.assertFalse((file_path.parent / "data.json.tmp").exists())
            self.assertEqual(list(file_path.parent.glob(".*.tmp")), [])

    def test_read_data_from_file_returns_none_for_bad_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "bad.json"
            file_path.write_text("{", encoding="utf-8")

            self.assertIsNone(helpers.read_data_from_file(file_path))

    def test_json_dump_failure_cleans_temp_file_and_preserves_existing_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "data.json"
            helpers.dump_data_to_file({"good": True}, file_path)

            with self.assertRaises(TypeError):
                helpers.dump_data_to_file({"bad": object()}, file_path)

            self.assertEqual(helpers.read_data_from_file(file_path), {"good": True})
            self.assertEqual(list(file_path.parent.glob(".*.tmp")), [])

    def test_thread_and_signal_helpers(self):
        marker = []

        def target(value):
            marker.append(value)

        thread = helpers.run_as_background_thread(target, "done")
        thread.join(timeout=1)

        self.assertEqual(marker, ["done"])
        self.assertTrue(thread.daemon)
        self.assertIn("target", thread.name)

        with patch.object(helpers.signal, "signal") as mock_signal:
            helpers.setup_signal_handlers()
        mock_signal.assert_any_call(signal.SIGINT, helpers.signal_handler)
        mock_signal.assert_any_call(signal.SIGTERM, helpers.signal_handler)

        with patch.object(helpers.sys, "exit", side_effect=SystemExit(0)):
            with patch("sys.stdout", new_callable=io.StringIO):
                with self.assertRaises(SystemExit):
                    helpers.signal_handler(signal.SIGTERM, None)

    def test_background_thread_logs_uncaught_exceptions(self):
        def failing_target():
            raise RuntimeError("boom")

        with self.assertLogs("src.program_helpers", level="ERROR") as logs:
            thread = helpers.run_as_background_thread(failing_target)
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertTrue(
            any(
                "Background thread target failing_target failed" in item
                for item in logs.output
            )
        )

    def test_scrip_code_match_and_mask_mobile_number(self):
        self.assertEqual(
            helpers.create_scrip_code_match("NIFTY", "2026-05-19", "CE", 23400),
            "NIFTY 19 May 2026 CE 23400.00",
        )
        self.assertEqual(helpers.mask_mobile_number("9876543210"), "9876xxxx10")

    def test_remove_old_logs_keeps_latest_three_per_subdir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logs_dir = Path(tmpdir)
            subdir = logs_dir / "program"
            subdir.mkdir()
            old_file = logs_dir / "old.log"
            old_file.write_text("old", encoding="utf-8")
            old_time = time.time() - 5 * 86400
            os.utime(old_file, (old_time, old_time))

            for index in range(5):
                path = subdir / f"{index}.log"
                path.write_text(str(index), encoding="utf-8")
                ts = time.time() + index
                os.utime(path, (ts, ts))

            helpers.remove_old_logs(logs_dir, days=2)

            remaining = sorted(path.name for path in subdir.glob("*.log"))

        self.assertFalse(old_file.exists())
        self.assertEqual(remaining, ["2.log", "3.log", "4.log"])
