"""
reopen_premature.py — Fix positions prematurely marked RESOLVED due to old
midnight-start-of-day time calculation.

The old _hours_to_resolution used end_date_iso as midnight start-of-day UTC,
causing markets on the *current* day to read hours_left <= 0 immediately.
The fix adds timedelta(days=1), making them expire at midnight end-of-day.

This script re-opens any RESOLVED positions whose resolution date is today
or in the future (i.e., still within the valid window under the corrected calc).

Run on the VPS:
    python -m bonding.reopen_premature [--dry-run]
"""
import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import config as _config

BOND_LEDGER   = Path(_config.BOND_LEDGER_FILE)
PAPER_LEDGER  = BOND_LEDGER.parent / "paper_positions.json"


def _end_of_day_utc(date_str: str) -> datetime:
    """Return midnight UTC that closes the given calendar date (date + 1 day)."""
    return datetime.fromisoformat(date_str[:10] + "T00:00:00+00:00") + timedelta(days=1)


def _is_still_open(resolution_time: str) -> bool:
    """True if the market is still running under the corrected time calculation."""
    try:
        eod = _end_of_day_utc(resolution_time)
        return eod > datetime.now(timezone.utc)
    except Exception:
        return False


def _reopen_bond_ledger(dry_run: bool) -> int:
    if not BOND_LEDGER.exists():
        print(f"Bond ledger not found: {BOND_LEDGER}")
        return 0

    data = json.loads(BOND_LEDGER.read_text(encoding="utf-8"))
    positions = data.get("positions", [])
    reopened = 0

    for p in positions:
        if p.get("status") != "RESOLVED":
            continue
        res_time = p.get("resolution_time", "")
        if not _is_still_open(res_time):
            continue

        print(
            f"  REOPEN [{p.get('city')}] {p.get('tier')} {p.get('market_id', '')[:10]}... "
            f"resolution={res_time[:10]}  exit_time={p.get('exit_time', '')[:16]}"
        )
        if not dry_run:
            p["status"]     = "OPEN"
            p["exit_price"] = None
            p["exit_time"]  = None
        reopened += 1

    if reopened and not dry_run:
        tmp = Path(str(BOND_LEDGER) + ".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(BOND_LEDGER)

    return reopened


def _reopen_paper_ledger(dry_run: bool) -> tuple[int, set[str]]:
    """Returns (count_reopened, set_of_market_ids_reopened)."""
    if not PAPER_LEDGER.exists():
        print(f"Paper ledger not found: {PAPER_LEDGER}")
        return 0, set()

    data = json.loads(PAPER_LEDGER.read_text(encoding="utf-8"))
    positions = data.get("positions", [])
    reopened = 0
    reopened_ids: set[str] = set()

    for p in positions:
        if p.get("status") not in ("SOLD", "RESOLVED"):
            continue
        res_time = p.get("resolution_time", "")
        if not _is_still_open(res_time):
            continue

        print(
            f"  PAPER REOPEN [{p.get('city')}] {p.get('tier')} {p.get('market_id', '')[:10]}... "
            f"resolution={res_time[:10]}  exit_ts={p.get('exit_ts', '')[:16]}"
        )
        if not dry_run:
            p["status"]     = "OPEN"
            p["exit_price"] = None
            p["exit_ts"]    = None
            p["pnl"]        = None
        reopened_ids.add(p["market_id"])
        reopened += 1

    if reopened and not dry_run:
        tmp = Path(str(PAPER_LEDGER) + ".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(PAPER_LEDGER)

    return reopened, reopened_ids


def _unpatch_paper_jsonl(market_ids: set[str], paper_log: Path, dry_run: bool) -> int:
    """
    Revert WOULD_BUY entries in the JSONL for markets that were re-opened.
    _record_sell() patches outcome='SOLD' into the original WOULD_BUY line;
    we need to undo that so the entry is live again.
    """
    if not market_ids or not paper_log.exists():
        return 0

    lines = paper_log.read_text(encoding="utf-8").splitlines()
    patched_count = 0
    out_lines = []

    for line in lines:
        if not line.strip():
            out_lines.append(line)
            continue
        try:
            rec = json.loads(line)
        except Exception:
            out_lines.append(line)
            continue

        if (
            rec.get("event") == "WOULD_BUY"
            and rec.get("market_id") in market_ids
            and rec.get("outcome") == "SOLD"
        ):
            print(
                f"  UNPATCH WOULD_BUY [{rec.get('city')}] market={rec.get('market_id', '')[:10]}..."
            )
            if not dry_run:
                rec["outcome"]    = None
                rec["exit_price"] = None
                rec["pnl"]        = None
                line = json.dumps(rec)
            patched_count += 1

        out_lines.append(line)

    if patched_count and not dry_run:
        tmp = Path(str(paper_log) + ".tmp")
        tmp.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
        tmp.replace(paper_log)

    return patched_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-open prematurely resolved positions")
    parser.add_argument("--dry-run", action="store_true", help="Print what would change without writing")
    args = parser.parse_args()

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"=== reopen_premature [{mode}] ===")
    print(f"Now (UTC): {datetime.now(timezone.utc).isoformat()}")
    print()

    print(f"Bond ledger: {BOND_LEDGER}")
    n_bond = _reopen_bond_ledger(args.dry_run)
    print(f"  → {n_bond} position(s) {'would be' if args.dry_run else ''} re-opened")
    print()

    paper_log = BOND_LEDGER.parent / "paper_trades.jsonl"
    print(f"Paper ledger: {PAPER_LEDGER}")
    n_paper, reopened_ids = _reopen_paper_ledger(args.dry_run)
    print(f"  → {n_paper} position(s) {'would be' if args.dry_run else ''} re-opened")
    if reopened_ids:
        print(f"Paper JSONL: {paper_log}")
        n_unpatched = _unpatch_paper_jsonl(reopened_ids, paper_log, args.dry_run)
        print(f"  → {n_unpatched} WOULD_BUY entry/entries {'would be' if args.dry_run else ''} un-patched")
    print()

    total = n_bond + n_paper
    if total == 0:
        print("Nothing to fix — no prematurely resolved positions found.")
    elif args.dry_run:
        print(f"Dry run complete. Run without --dry-run to apply changes.")
    else:
        print(f"Done. {total} position(s) re-opened and ledgers saved.")


if __name__ == "__main__":
    main()
