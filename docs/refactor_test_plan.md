# Engine Refactor Test Plan

Backup snapshot before engine refactors:

```text
backups/pre_engine_refactor_2026-05-14_1009/
```

Global verification after every file-level change:

```bash
python3 -m unittest discover -v
ruff check --no-cache src tests
ruff format --check src tests
python3 -m compileall -q src tests
```

## Refactor Order

| Step | File | Goal | Focused Test Path Before/After |
|---:|---|---|---|
| 1 | `src/program_helpers.py` | Make shared IO/log/network helpers safer and quieter without UI changes | `python3 -m unittest tests.test_helpers -v` |
| 2 | `src/program_login.py` | Harden session/login cache code and keep broker SDK calls mocked in tests | `python3 -m unittest tests.test_login -v` |
| 3 | `src/program_orders.py` | Make order payload/chunking logic safer while preserving current order behavior | `python3 -m unittest tests.test_orders -v` |
| 4 | `src/program_quotes.py` | Improve quote/expiry helpers and payload validation | `python3 -m unittest tests.test_quotes_profile.TestQuotes -v` |
| 5 | `src/program_client_profile.py` | Tighten margin/count handling | `python3 -m unittest tests.test_quotes_profile.TestClientProfile -v` |
| 6 | `src/program_background.py` | Reduce hidden background failures, stale files, and thread/file IO risk | `python3 -m unittest tests.test_display_background.TestProgramBackground -v` |
| 7 | `src/program_display.py` | Preserve terminal UI while validating data table safety | `python3 -m unittest tests.test_display_background.TestProgramDisplay -v` |
| 8 | `src/main.py` | Preserve menu/UI while cleaning orchestration and startup flow | `python3 -m unittest tests.test_main_and_constants.TestMain -v` |
| 9 | `src/program_constants.py` | Move/clean config constants only after behavior is protected | `python3 -m unittest tests.test_main_and_constants.TestConstants -v` |

## Rules

- Run the focused test path before touching the file.
- Make the smallest behavior-preserving change.
- Run the same focused test path immediately after the change.
- Run the global verification gate before moving to the next file.
- Broker calls must stay mocked in tests: no real login, no real order placement, no real cancel, no real network.
- User-facing menu text and workflow should not change unless explicitly approved.
