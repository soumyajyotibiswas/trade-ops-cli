# Security Policy

This repository is a sanitized public version of a personal trading operations CLI.

## Do Not Commit

- Real broker credentials or account identifiers.
- TOTP seeds, PINs, MPINs, passwords, API keys, consumer keys, or consumer secrets.
- Broker session cache files such as `login_information.pkl`.
- Runtime `data/`, `logs/`, or `backups/` output.
- Real order books, positions, margins, or trade history.

Use `src/program_secrets.example.py` as the template and keep the real
`src/program_secrets.py` file local only.

## Live Trading Warning

Normal mode can place and cancel real orders after successful broker login.
Use dry-run mode for development:

```bash
TRADING_PROGRAM_DRY_RUN_ORDERS=1 python3 src/main.py
```

Unit tests mock broker SDKs and must not invoke live login, live quote, or live order APIs.
