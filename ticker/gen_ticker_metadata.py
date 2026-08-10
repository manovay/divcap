#!/usr/bin/env python3
"""
Join Ticker Overview detail onto the bulk reference table.

/v3/reference/tickers gives the universe cheaply but carries no sector,
size, or company detail. Those live on /v3/reference/tickers/{ticker}, which
is one call per symbol -- so this is ~13k requests where the bulk pull was
~40. Measured latency is ~290ms sequential, hence the batching.

Resumability is by TICKER SET, not by row count. Requests are fired in
parallel so completion order does not match input order, and a count-based
resume would silently skip and duplicate rows. On restart the output is read
back, already-done tickers are skipped, and the run continues.

Failed lookups are WRITTEN, with null overview fields and an overview_error
string. Delisted names in particular may have no Overview record. Writing the
failure means a restart does not retry it forever; to retry, delete those
lines from the output and re-run.

Run:
    python3 ticker/gen_ticker_metadata.py tickers_all_5y.jsonl
    python3 ticker/gen_ticker_metadata.py tickers_all_5y.jsonl 500

The optional second argument caps how many NEW tickers to fetch this run,
which is useful for a proof of concept without committing to the full pull.

Output: <input stem>_metadata.jsonl, alongside the input.

Needs MASSIVE_API_KEY (REST key, not the S3 credential).
"""

import os
import sys
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

BASE = "https://api.massive.com/v3/reference/tickers"
BATCH = 20

# Carried through from the bulk reference row.
KEEP = ["ticker", "market", "locale", "type", "active", "last_updated_utc"]

# Pulled from Ticker Overview.
WANT = ["address", "description", "list_date", "sic_code", "sic_description",
        "market_cap", "total_employees", "weighted_shares_outstanding"]

if "MASSIVE_API_KEY" not in os.environ:
    sys.exit("MASSIVE_API_KEY is not set -- see the shell profile setup")
KEY = os.environ["MASSIVE_API_KEY"]


def out_path(inp):
    """tickers_all_5y.jsonl -> tickers_all_5y_metadata.jsonl, same directory."""
    d = os.path.dirname(os.path.abspath(inp))
    base = os.path.basename(inp)
    if base.endswith(".jsonl"):
        stem = base[:-len(".jsonl")]
    else:
        stem = base
    return os.path.join(d, f"{stem}_metadata.jsonl")


def load_done(path):
    """
    Tickers already written. Unparseable lines are skipped rather than fatal:
    a run killed mid-write leaves a truncated final line, and that should not
    block a restart.
    """
    done = set()
    if not os.path.exists(path):
        return done
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if "ticker" in r:
            done.add(r["ticker"])
    return done


def fetch(ticker, tries=4):
    """
    One Overview call. Tickers contain dots and slashes (AACT.WS, ZTLD.C), so
    the symbol must be percent-encoded into the path.

    Returns (results_dict, None) or (None, error_string). 404 is a real
    answer -- no Overview record exists -- so it is not retried.
    """
    url = f"{BASE}/{urllib.parse.quote(ticker, safe='')}?apiKey={KEY}"
    wait = 2
    n = 0
    while True:
        n += 1
        try:
            with urllib.request.urlopen(url) as r:
                d = json.load(r)
            return d.get("results", {}), None
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None, "404"
            if e.code not in (429, 500, 502, 503, 504) or n >= tries:
                return None, f"http {e.code}"
            time.sleep(wait)
            wait *= 2
        except Exception as e:
            if n >= tries:
                return None, str(e)
            time.sleep(wait)
            wait *= 2


def enrich(row):
    """Base fields plus Overview fields, always the same key set."""
    out = {}
    for k in KEEP:
        out[k] = row.get(k)

    res, err = fetch(row["ticker"])
    if res is None:
        for k in WANT:
            out[k] = None
        out["overview_error"] = err
    else:
        for k in WANT:
            out[k] = res.get(k)
        out["overview_error"] = None
    return out


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: gen_ticker_metadata.py <tickers_all_Ny.jsonl> [max_new]")
    inp = sys.argv[1]

    if len(sys.argv) > 2:
        cap = int(sys.argv[2])
    else:
        cap = None

    if not os.path.exists(inp):
        sys.exit(f"missing: {inp}")

    out = out_path(inp)
    done = load_done(out)

    todo = []
    total = 0
    for line in open(inp):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        total += 1
        if r["ticker"] in done:
            continue
        todo.append(r)

    if cap is not None:
        todo = todo[:cap]

    print(f"input     {total} tickers   {os.path.basename(inp)}")
    print(f"done      {len(done)} already in {os.path.basename(out)}")
    print(f"to fetch  {len(todo)}\n")

    if not todo:
        print("nothing to do")
        return

    n = 0
    t0 = time.time()
    errs = 0
    # Append, never truncate: the output is the resume state.
    with open(out, "a") as f:
        pool = ThreadPoolExecutor(max_workers=BATCH)
        i = 0
        while i < len(todo):
            batch = todo[i:i + BATCH]
            for rec in pool.map(enrich, batch):
                f.write(json.dumps(rec) + "\n")
                n += 1
                if rec["overview_error"] is not None:
                    errs += 1
            # Flush per batch so a kill loses at most one batch.
            f.flush()
            os.fsync(f.fileno())
            i += BATCH

            el = time.time() - t0
            rate = n / el if el > 0 else 0
            left = (len(todo) - n) / rate if rate > 0 else 0
            print(f"  {n}/{len(todo)}  {rate:.1f}/s  ~{left/60:.1f} min left "
                  f"({errs} errors)", flush=True)
        pool.shutdown()

    print(f"\nwrote {n} rows ({errs} with overview_error) to {out}")


if __name__ == "__main__":
    main()
