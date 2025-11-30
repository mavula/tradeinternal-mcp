"""Quick script to exercise the FastAPI candle endpoint."""

from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request


def fetch_candles(
    symbol: str,
    time_frame: str,
    limit: int = 5,
    exchange: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    base_url: str = "http://127.0.0.1:9000",
) -> list[dict]:
    """Call the running FastAPI service and return parsed JSON."""

    params: dict[str, str] = {"symbol": symbol, "time_frame": time_frame, "limit": str(limit)}
    if exchange:
        params["exchange"] = exchange
    if start_timestamp is not None:
        params["start_timestamp"] = str(start_timestamp)
    if end_timestamp is not None:
        params["end_timestamp"] = str(end_timestamp)

    url = f"{base_url.rstrip('/')}/candles?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url) as response:
        payload = response.read().decode("utf-8")
        return json.loads(payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Call the FastAPI candle endpoint for quick checks.")
    parser.add_argument("--symbol", default="BANKNIFTY1!", help="Symbol exactly as stored in the DB.")
    parser.add_argument("--time-frame", default="30m", help="Time frame value (e.g., 1, 5, 1D).")
    parser.add_argument("--limit", type=int, default=3, help="Number of rows to fetch (default: 3).")
    parser.add_argument("--exchange", help="Optional exchange filter.")
    parser.add_argument("--start-timestamp", type=int, help="Optional unix start timestamp.")
    parser.add_argument("--end-timestamp", type=int, help="Optional unix end timestamp.")
    parser.add_argument("--base-url", default="http://127.0.0.1:9000", help="FastAPI base URL.")

    args = parser.parse_args()

    candles = fetch_candles(
        symbol=args.symbol,
        time_frame=args.time_frame,
        limit=args.limit,
        exchange=args.exchange,
        start_timestamp=args.start_timestamp,
        end_timestamp=args.end_timestamp,
        base_url=args.base_url,
    )

    print(json.dumps(candles, indent=2))
