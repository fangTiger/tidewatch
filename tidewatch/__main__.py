"""命令行入口：python -m tidewatch [--only news hyperliquid web] [--dry-run]"""

from tidewatch.app import main

if __name__ == "__main__":
    raise SystemExit(main())
