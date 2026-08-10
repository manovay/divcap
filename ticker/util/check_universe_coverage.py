#!/usr/bin/env python3
"""
Check how well the price-side SIP ticker list is covered by the reference pull.

The SIP list defines universe membership: a ticker appears because it actually
traded, which makes it correctly dated and free of survivorship bias. The
reference pull supplies attributes (type, primary_exchange, delisted_utc).
This reports how cleanly the two line up and what falls in the gaps.

Reads the single combined file written by gen_all_tickers.py, splitting the
two phases on each record's own `active` flag.

Run (from anywhere):
    python3 ticker/util/check_universe_coverage.py
    python3 ticker/util/check_universe_coverage.py <sip.csv> <tickers.jsonl>

Defaults resolve relative to ticker/, not the working directory.
"""

import os
import sys
import json

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.dirname(HERE)

DEFAULT_SIP = os.path.join(DATA, "ticker_sip_list_2026-07_2026-08.csv")
DEFAULT_REF = os.path.join(DATA, "tickers_all_5y.jsonl")


def load_sip(path):
    out = set()
    for line in open(path):
        t = line.strip()
        if t:
            out.add(t)
    return out


def load_ref(path):
    """
    Returns (active, delisted) as {ticker: record}. Split on the record's own
    `active` flag, which is the only thing distinguishing the two phases in
    the combined file.
    """
    active = {}
    delisted = {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("active"):
            active[r["ticker"]] = r
        else:
            delisted[r["ticker"]] = r
    return active, delisted


def pct(a, b):
    if b == 0:
        return 0.0
    return 100.0 * a / b


def tally(records, tickers, field):
    """Count `field` across `tickers`, bucketing absent values explicitly."""
    counts = {}
    for t in tickers:
        v = records[t].get(field)
        if v is None:
            v = "(missing)"
        if v not in counts:
            counts[v] = 0
        counts[v] += 1
    return counts


def show(title, counts, total, limit=None):
    print(title)
    pairs = []
    for k in counts:
        pairs.append((counts[k], k))
    pairs.sort(reverse=True)
    if limit is not None:
        pairs = pairs[:limit]
    for n, k in pairs:
        print(f"  {k:12s} {n:6d}  {pct(n, total):5.1f}%")
    print()


def main():
    if len(sys.argv) > 1:
        sip_path = sys.argv[1]
    else:
        sip_path = DEFAULT_SIP

    if len(sys.argv) > 2:
        ref_path = sys.argv[2]
    else:
        ref_path = DEFAULT_REF

    for p in (sip_path, ref_path):
        if not os.path.exists(p):
            sys.exit(f"missing: {p}")

    sip = load_sip(sip_path)
    active, delisted = load_ref(ref_path)

    print(f"sip list      {len(sip)}   {os.path.basename(sip_path)}")
    print(f"active ref    {len(active)}   {os.path.basename(ref_path)}")
    print(f"delisted ref  {len(delisted)}")
    print()

    both = sip & set(active)
    sip_only = sip - set(active)
    active_only = set(active) - sip

    print(f"in both              {len(both)}  ({pct(len(both), len(sip)):.1f}% of sip)")
    print(f"sip, not in active   {len(sip_only)}")
    print(f"active, not in sip   {len(active_only)}")
    print()

    # Traded during the window but already delisted. This is the survivorship
    # exposure, and the reason the delisted phase is not optional.
    rescued = sip_only & set(delisted)
    orphans = sip_only - set(delisted)
    print(f"of the {len(sip_only)} sip-only, {len(rescued)} are in the delisted pull")
    print(f"leaving {len(orphans)} with no reference record at all")
    print()

    # Type mix of the matched set. If CS is a minority, the analysis is
    # largely measuring fund distributions rather than corporate dividends.
    show("type breakdown of matched tickers",
         tally(active, both, "type"), len(both))

    show("exchange breakdown of matched tickers",
         tally(active, both, "primary_exchange"), len(both), limit=10)

    if rescued:
        show("type breakdown of delisted-but-traded",
             tally(delisted, rescued, "type"), len(rescued))

    if orphans:
        print("sip tickers with no reference record:")
        print(" ", sorted(orphans)[:30])


if __name__ == "__main__":
    main()