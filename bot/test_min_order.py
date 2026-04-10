"""
test_min_order.py — Probe the Polymarket CLOB FOK minimum order size.

Finds a live weather market with a cheap ask price (≤ $0.10/share), then
attempts FOK buy orders of increasing sizes to determine the smallest order
the CLOB will actually accept.

Usage (from bot/ directory):
    python test_min_order.py

The script will NOT leave any open positions — FOK orders either fill
immediately or are auto-cancelled. If an order fills, it will be logged
clearly so you can manually exit it.

Set DRY_RUN = True below to only find a candidate market without placing orders.
"""

import os
import sys
import asyncio
import json
import time
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
DRY_RUN          = False   # Set True to skip actual order placement
MAX_ASK_PRICE    = 0.10    # Only use markets with ask ≤ this (cheap tokens)
PROBE_SIZES      = [1, 2, 5, 10, 20]  # shares to try in order
PROBE_DELAY_SECS = 2       # seconds to wait between probes

# ── Bootstrap path ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).parent.parent / ".env")

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from config import (
    CLOB_HOST, CHAIN_ID, PRIVATE_KEY, FUNDER_ADDRESS,
    API_KEY, API_SECRET, API_PASSPHRASE,
)


def build_client() -> ClobClient:
    creds = ApiCreds(
        api_key=API_KEY,
        api_secret=API_SECRET,
        api_passphrase=API_PASSPHRASE,
    )
    return ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PRIVATE_KEY,
        creds=creds,
        signature_type=2,
        funder=FUNDER_ADDRESS,
    )


def find_cheap_weather_market(client: ClobClient) -> dict | None:
    """
    Fetch open weather markets from Gamma and return the first one
    with a YES ask price <= MAX_ASK_PRICE.
    """
    import requests
    print(f"\n[1] Searching for open weather markets with ask <= ${MAX_ASK_PRICE:.2f}...")

    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "active": "true",
        "closed": "false",
        "tag": "weather",
        "limit": 100,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        markets = resp.json()
    except Exception as e:
        print(f"    ERROR fetching markets from Gamma: {e}")
        return None

    print(f"    Found {len(markets)} open weather markets.")

    for m in markets:
        # Gamma embeds best_ask / tokens
        tokens = m.get("tokens") or m.get("outcomes") or []
        question = m.get("question", "")

        # Try to find the YES token with a cheap ask
        for token in tokens:
            outcome = (token.get("outcome") or "").upper()
            if outcome != "YES":
                continue
            token_id = token.get("token_id") or token.get("clobTokenId") or ""
            if not token_id:
                continue

            # Price can be in different fields depending on Gamma version
            price = (
                token.get("price")
                or token.get("best_ask")
                or m.get("bestAsk")
                or None
            )
            if price is None:
                continue
            price = float(price)

            if 0.0 < price <= MAX_ASK_PRICE:
                print(f"    Found: {question[:80]}")
                print(f"           token_id={token_id[:16]}... ask=${price:.4f}")
                return {
                    "question": question,
                    "token_id": token_id,
                    "ask": price,
                    "market_id": m.get("id", ""),
                }

    print("    No cheap weather market found. Try raising MAX_ASK_PRICE.")
    return None


def get_live_ask(client: ClobClient, token_id: str) -> float | None:
    """Fetch the current best ask from the CLOB order book."""
    try:
        book = client.get_order_book(token_id)
        asks = sorted(book.asks, key=lambda x: float(x.price))
        if asks:
            return float(asks[0].price)
    except Exception as e:
        print(f"    WARNING: could not fetch live ask: {e}")
    return None


def place_fok(client: ClobClient, token_id: str, price: float, shares: int) -> dict:
    """
    Place a FOK BUY order. Returns a dict with keys:
        success: bool
        order_id: str | None
        status: str | None
        error: str | None
        raw: any
    """
    args = OrderArgs(
        token_id=token_id,
        price=price,
        size=float(shares),
        side="BUY",
    )
    try:
        signed = client.create_order(args)
        result = client.post_order(signed, OrderType.FOK)
        order_id = (result or {}).get("orderID") or (result or {}).get("order_id", "")
        status   = (result or {}).get("status", "")
        return {
            "success": True,
            "order_id": order_id,
            "status": status,
            "error": None,
            "raw": result,
        }
    except Exception as e:
        return {
            "success": False,
            "order_id": None,
            "status": None,
            "error": str(e),
            "raw": None,
        }


def main():
    print("=" * 70)
    print("  CLOB FOK minimum order size probe")
    print("=" * 70)

    if not PRIVATE_KEY:
        print("ERROR: PRIVATE_KEY not set. Check your .env file.")
        sys.exit(1)

    client = build_client()
    print(f"\n[0] CLOB client ready. DRY_RUN={DRY_RUN}")

    market = find_cheap_weather_market(client)
    if not market:
        sys.exit(1)

    token_id = market["token_id"]
    ask_from_gamma = market["ask"]

    # Refresh price from CLOB book for accuracy
    live_ask = get_live_ask(client, token_id)
    ask = live_ask if live_ask and live_ask > 0 else ask_from_gamma
    print(f"\n[2] Live CLOB ask: ${ask:.4f}  (Gamma quoted ${ask_from_gamma:.4f})")

    print(f"\n[3] Question: {market['question'][:80]}")
    print(f"    Market ID: {market['market_id']}")
    print(f"    Token ID:  {token_id}")

    if DRY_RUN:
        print("\nDRY_RUN=True — skipping order placement.")
        print("Estimated order values at probe sizes:")
        for s in PROBE_SIZES:
            print(f"  {s:3d} shares × ${ask:.4f} = ${s * ask:.4f}")
        return

    print(f"\n[4] Probing FOK orders (price=${ask:.4f})...")
    print(f"    {'Shares':>6}  {'Est. Value':>12}  {'Result'}")
    print(f"    {'------':>6}  {'----------':>12}  {'------'}")

    first_success = None
    first_failure_below = None

    for shares in PROBE_SIZES:
        value = shares * ask
        result = place_fok(client, token_id, ask, shares)

        if result["success"]:
            status_str = f"OK  status={result['status']}  order_id={result['order_id'][:12] if result['order_id'] else 'none'}..."
            if first_success is None:
                first_success = (shares, value)
        else:
            status_str = f"FAIL  error={result['error'][:100]}"
            if first_success is None:
                first_failure_below = (shares, value, result["error"])

        print(f"    {shares:>6}  ${value:>10.4f}  {status_str}")
        time.sleep(PROBE_DELAY_SECS)

    print("\n" + "=" * 70)
    print("  RESULTS")
    print("=" * 70)
    if first_success:
        s, v = first_success
        print(f"  Smallest SUCCESSFUL order: {s} shares  (~${v:.4f})")
    if first_failure_below:
        s, v, err = first_failure_below
        print(f"  Smallest REJECTED order:   {s} shares  (~${v:.4f})")
        print(f"  CLOB rejection message: {err}")

    if first_success and first_failure_below:
        fs, fv = first_success
        fr, frv, _ = first_failure_below
        print(f"\n  => CLOB minimum is between ${frv:.4f} and ${fv:.4f}")
    elif first_success:
        s, v = PROBE_SIZES[0], PROBE_SIZES[0] * ask
        print(f"\n  => CLOB accepted even {PROBE_SIZES[0]} share(s) at ${v:.4f}")
        print(f"     The $1 minimum may NOT apply to FOK orders.")
    elif first_failure_below:
        print(f"\n  => All probe orders were rejected. The minimum may be above ${PROBE_SIZES[-1] * ask:.4f}")

    print()


if __name__ == "__main__":
    main()
