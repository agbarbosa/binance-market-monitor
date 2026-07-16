#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.request

APPROVED_URL = "https://data-api.binance.vision/api/v3/ping"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Opt-in no-auth Binance public connectivity check."
    )
    parser.add_argument("--live", action="store_true", help="Actually perform the public request.")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    if not args.live:
        print(json.dumps({"status": "skipped", "reason": "pass --live to opt in"}))
        return 0
    with urllib.request.urlopen(APPROVED_URL, timeout=args.timeout) as response:  # noqa: S310 - opt-in public HTTPS ping.
        print(json.dumps({"status": "ok", "code": response.status, "url": APPROVED_URL}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
