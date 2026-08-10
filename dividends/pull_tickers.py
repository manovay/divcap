#!/usr/bin/env python3
"""
Bulk-pull the stocks reference table from /v3/reference/tickers.

The ticker parameter is optional and defaults to empty, which queries all
tickers -- so this is ~30-60 paginated requests, not one call per symbol.
Run it twice: active=true, then active=false. The delisted pull is not
optional. Over a five-year window a meaningful share of dividend payers have
since been acquired or delisted, and skipping them reintroduces survivorship
bias into the event table.

Pages are appended to the output file as they arrive rather than accumulated
in memory, so a mid-pull failure keeps everything already fetched. next_url
is an opaque cursor: pages must be walked in order and a dead pull restarts
from the top.

Run:
    python3 ingest/pull_tickers.py true  tickers_active.jsonl
    python3 ingest/pull_tickers.py false tickers_delisted.jsonl

    cat tickers_active.jsonl tickers_delisted.jsonl > tickers.jsonl
    hdfs dfs -mkdir -p $TEAM/reference
    hdfs dfs -put tickers.jsonl $TEAM/reference/

Needs MASSIVE_API_KEY (REST key, not the S3 credential).
"""

import os
import sys
import json
import time
import urllib.request
import urllib.error

KEY = os.environ["MASSIVE_API_KEY"]


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


def main():
    active = sys.argv[1]
    out = sys.argv[2]

    url = ("https://api.massive.com/v3/reference/tickers"
           f"?market=stocks&active={active}&limit=1000&apiKey={KEY}")

    n = 0
    page = 0
    with open(out, "w") as f:
        while url:
            d = fetch(url)
            page += 1
            for x in d.get("results", []):
                f.write(json.dumps(x) + "\n")
                n += 1
            f.flush()
            print(f"page {page}: {n} rows", flush=True)

            nxt = d.get("next_url")
            if nxt:
                url = f"{nxt}&apiKey={KEY}"
            else:
                url = None

    print(f"wrote {n} rows to {out}")


if __name__ == "__main__":
    main()
