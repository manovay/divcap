#!/usr/bin/env python3
"""
Cut a small, committed sample of the metadata file.

The full file is gitignored (multi-MB vendor data). This sample exists so
anyone can see the record shape without running the pullers or holding
credentials.

Selection is deliberate, not random: the sample shows every shape a consumer
will hit, because a random slice would be almost all ETFs and delisted 404s
and would misrepresent what a populated record looks like.

  6  active CS with sic_description populated  -- the fully-populated case
  2  active ETF                                -- null sector/employees
  2  active PFD or WARRANT                     -- other instrument types
  5  delisted                                  -- the 404 case, all nulls

Deterministic: sorted by ticker, first N of each bucket.

Run:
    python3 ticker/util/make_sample.py
    python3 ticker/util/make_sample.py <metadata.jsonl> <out.jsonl>
"""

import os
import sys
import json

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.dirname(HERE)

DEFAULT_IN = os.path.join(DATA, "tickers_all_5y_metadata.jsonl")
DEFAULT_OUT = os.path.join(DATA, "sample_tickers_metadata.jsonl")


def main():
    if len(sys.argv) > 1:
        inp = sys.argv[1]
    else:
        inp = DEFAULT_IN

    if len(sys.argv) > 2:
        out = sys.argv[2]
    else:
        out = DEFAULT_OUT

    if not os.path.exists(inp):
        sys.exit(f"missing: {inp}")

    rows = []
    for line in open(inp):
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))

    rows.sort(key=lambda r: r["ticker"])

    cs = []
    etf = []
    other = []
    dead = []

    for r in rows:
        if not r.get("active"):
            if len(dead) < 5:
                dead.append(r)
            continue
        t = r.get("type")
        if t == "CS" and r.get("sic_description") is not None:
            if len(cs) < 6:
                cs.append(r)
        elif t == "ETF":
            if len(etf) < 2:
                etf.append(r)
        elif t in ("PFD", "WARRANT"):
            if len(other) < 2:
                other.append(r)

    picked = cs + etf + other + dead

    with open(out, "w") as f:
        for r in picked:
            f.write(json.dumps(r) + "\n")

    print(f"wrote {len(picked)} rows to {out}")
    print(f"  {len(cs)} active CS (populated)")
    print(f"  {len(etf)} active ETF")
    print(f"  {len(other)} active PFD/WARRANT")
    print(f"  {len(dead)} delisted")
    print()
    for r in picked:
        print(f"  {r['ticker']:10s} {str(r.get('type')):8s} "
              f"active={str(r.get('active')):5s} "
              f"sic={r.get('sic_description')}")


if __name__ == "__main__":
    main()