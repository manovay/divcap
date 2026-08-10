#!/usr/bin/env python3
"""
Bulk-pull the stocks reference table from /v3/reference/tickers.

The ticker parameter is optional and defaults to empty, which queries all
tickers -- so this is ~30-60 paginated requests, not one call per symbol.

Pulls both phases in one run. active=true is the current listing; active=false
is a SEPARATE query returning only delisted names, not a superset. The
delisted phase is not optional: 158 of the 13,192 tickers that traded in
July-August 2026 were already delisted by the time the reference table was
pulled. Over five years that gap becomes survivorship bias in the event table.

The delisted phase is filtered to a lookback window, since the full pull
reaches back decades and those names cannot appear in any price window we
hold. The endpoint has no date filter, so the filter runs client-side on
delisted_utc -- every page is still fetched, only the kept rows shrink.

Records with no delisted_utc are KEPT regardless of window. They cannot be
dated, and dropping undateable records would silently narrow the universe.

Every record carries its own `active` flag, so the two phases can be told
apart downstream despite sharing one file. Many older delisted records have
no `type` field at all, so downstream joins need a left join with an explicit
unknown bucket rather than an inner join.

Pages are appended as they arrive rather than accumulated in memory, so a
mid-pull failure keeps what was already fetched. next_url is an opaque
cursor: pages must be walked in order and a dead pull restarts from the top.

Run:
    python3 ticker/gen_all_tickers.py            # default 5 years
    python3 ticker/gen_all_tickers.py 3
    python3 ticker/gen_all_tickers.py 3 out.jsonl

    hdfs dfs -mkdir -p $TEAM/reference
    hdfs dfs -put tickers_all_5y.jsonl $TEAM/reference/

Needs MASSIVE_API_KEY (REST key, not the S3 credential).
"""

import os
import sys
import json
import time
import datetime
import urllib.request
import urllib.error

BASE = "https://api.massive.com/v3/reference/tickers"

if "MASSIVE_API_KEY" not in os.environ:
    sys.exit("MASSIVE_API_KEY is not set -- see the shell profile setup")
KEY = os.environ["MASSIVE_API_KEY"]


def cutoff_date(years):
    """
    ISO date `years` back from today. The replace() guards Feb 29, which has
    no counterpart in a non-leap year.
    """
    today = datetime.date.today()
    try:
        return today.replace(year=today.year - years).isoformat()
    except ValueError:
        return today.replace(year=today.year - years, day=28).isoformat()


def fetch(url, tries=5):
    """
    One page, with backoff. 429 is the expected failure on a rate-limited
    plan; sleeping and retrying is cheaper than restarting the whole walk,
    since the cursor cannot be resumed from the middle.
    """
    wait = 2
    n = 0
    while True:
        n += 1
        try:
            with urllib.request.urlopen(url) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or n >= tries:
                raise
            print(f"  http {e.code}, sleeping {wait}s", flush=True)
            time.sleep(wait)
            wait *= 2


def keep(rec, cutoff):
    """
    True if this delisted record falls inside the window. delisted_utc is
    ISO-8601 ("2008-02-11T05:00:00Z"), so a lexical compare on the first ten
    characters is a correct date compare.
    """
    d = rec.get("delisted_utc")
    if d is None:
        return True
    return d[:10] >= cutoff


def pull(f, active, cutoff):
    """Walk one phase to exhaustion, appending to an already-open file."""
    url = f"{BASE}?market=stocks&active={active}&limit=1000&apiKey={KEY}"
    seen = 0
    kept = 0
    undated = 0
    page = 0
    while url:
        d = fetch(url)
        page += 1
        for x in d.get("results", []):
            seen += 1
            if cutoff is not None:
                if not keep(x, cutoff):
                    continue
                if x.get("delisted_utc") is None:
                    undated += 1
            f.write(json.dumps(x) + "\n")
            kept += 1
        f.flush()
        print(f"  active={active} page {page}: {kept} kept / {seen} seen",
              flush=True)

        nxt = d.get("next_url")
        if nxt:
            url = f"{nxt}&apiKey={KEY}"
        else:
            url = None
    return seen, kept, undated


def main():
    if len(sys.argv) > 1:
        years = int(sys.argv[1])
    else:
        years = 5

    if len(sys.argv) > 2:
        out = sys.argv[2]
    else:
        out = f"tickers_all_{years}y.jsonl"

    cutoff = cutoff_date(years)
    print(f"delisted window: {years}y, keeping delisted_utc >= {cutoff}\n")

    with open(out, "w") as f:
        print("pulling active...", flush=True)
        seen_a, kept_a, _ = pull(f, "true", None)
        print("pulling delisted...", flush=True)
        seen_d, kept_d, undated = pull(f, "false", cutoff)

    print()
    print(f"active              {kept_a}")
    print(f"delisted in window  {kept_d} of {seen_d} "
          f"({undated} kept with no delisted_utc)")
    print(f"wrote {kept_a + kept_d} rows to {out}")


if __name__ == "__main__":
    main()