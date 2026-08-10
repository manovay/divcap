#!/usr/bin/env python3
"""
Cut a small, committed sample of the dividends file.

The full file is gitignored (multi-MB vendor data). This sample exists so
anyone can see the record shape without running the puller or holding
credentials.

Selection is deliberate, not random: a random slice would be almost entirely
monthly-paying ETFs and would misrepresent the spread. The sample covers the
frequency and distribution_type values a consumer will actually branch on.

  6  frequency 4  (quarterly)   -- the operating-company case
  3  frequency 12 (monthly)     -- overwhelmingly funds
  2  frequency 52 (weekly)      -- weekly-distributing ETFs
  2  frequency 0  (non-recurring)
  3  distribution_type != recurring

Deterministic: sorted by (ex_dividend_date, ticker), first N of each bucket.

Run:
    python3 dividends/util/make_sample.py
    python3 dividends/util/make_sample.py <dividends.jsonl> <out.jsonl>
"""

import os
import sys
import json

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.dirname(HERE)

DEFAULT_IN = os.path.join(DATA, "divs_5y", "dividends_5y.jsonl")
DEFAULT_OUT = os.path.join(DATA, "sample_dividends.jsonl")


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

    rows.sort(key=lambda r: (r.get("ex_dividend_date") or "",
                             r.get("ticker") or ""))

    quarterly = []
    monthly = []
    weekly = []
    nonrec = []
    odd_type = []

    for r in rows:
        f = r.get("frequency")
        d = r.get("distribution_type")

        if d is not None and d != "recurring" and len(odd_type) < 3:
            odd_type.append(r)
            continue
        if f == 4 and len(quarterly) < 6:
            quarterly.append(r)
        elif f == 12 and len(monthly) < 3:
            monthly.append(r)
        elif f == 52 and len(weekly) < 2:
            weekly.append(r)
        elif f == 0 and len(nonrec) < 2:
            nonrec.append(r)

    picked = quarterly + monthly + weekly + nonrec + odd_type

    with open(out, "w") as f:
        for r in picked:
            f.write(json.dumps(r) + "\n")

    print(f"wrote {len(picked)} rows to {out}")
    print(f"  {len(quarterly)} quarterly (freq 4)")
    print(f"  {len(monthly)} monthly (freq 12)")
    print(f"  {len(weekly)} weekly (freq 52)")
    print(f"  {len(nonrec)} non-recurring (freq 0)")
    print(f"  {len(odd_type)} distribution_type != recurring")
    print()
    for r in picked:
        print(f"  {r.get('ticker'):8s} {r.get('ex_dividend_date')}  "
              f"freq={str(r.get('frequency')):4s} "
              f"{str(r.get('distribution_type')):12s} "
              f"${r.get('cash_amount')}")


if __name__ == "__main__":
    main()