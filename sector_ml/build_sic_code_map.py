#!/usr/bin/env python3
"""
Build a sic_description -> sic_code lookup table.

The Overview endpoint returns sic_code, but gen_ticker_metadata.py's WANT
whitelist discarded it, so the metadata file on HDFS has only the 373 distinct
description strings. Re-pulling all 20,729 tickers to recover the code costs
~7 hours against the observed rate limit (~100 fast requests, then ~0.8/s).

sic_code and sic_description are 1:1. So fetch ONE representative ticker per
distinct description -- ~373 requests, ~8 minutes -- and join the map back onto
every row by description string. The map is a small derived artifact: commit it
and never pull it again.

Output: sector_ml/sic_code_map.json  {sic_description: sic_code}

Run:
    # from the existing metadata file (local or piped from HDFS)
    hdfs dfs -text $TEAM/reference/tickers_all_5y_metadata.jsonl > /tmp/meta.jsonl
    python3 sector_ml/build_sic_code_map.py /tmp/meta.jsonl

Resumable: re-run to retry only the descriptions still missing.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://api.massive.com/v3/reference/tickers"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sic_code_map.json")

# Deliberately low. At BATCH=20 the vendor allows ~100 requests then throttles
# to ~0.8/s, and 20 workers all backing off exponentially makes it worse.
BATCH = 4

if "MASSIVE_API_KEY" not in os.environ:
    sys.exit("MASSIVE_API_KEY is not set -- see README_TICKER.md section 8")
KEY = os.environ["MASSIVE_API_KEY"]


def fetch(ticker, tries=5):
    """Returns (sic_code, sic_description, error). Backs off on 429/5xx."""
    url = f"{BASE}/{ticker}?apiKey={KEY}"
    wait = 2.0
    for n in range(1, tries + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                res = json.load(r).get("results") or {}
            return res.get("sic_code"), res.get("sic_description"), None
        except urllib.error.HTTPError as e:
            if e.code == 404 or n >= tries:
                return None, None, f"http {e.code}"
            time.sleep(wait)
            wait *= 2
        except Exception as e:
            if n >= tries:
                return None, None, str(e)
            time.sleep(wait)
            wait *= 2
    return None, None, "exhausted"


def representatives(path):
    """One ticker per distinct sic_description. Prefers active rows."""
    best = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            sic = (d.get("sic_description") or "").strip()
            tic = d.get("ticker")
            if not sic or not tic:
                continue
            # Overview 404s on delisted names, so an active representative is
            # the only one that can actually be fetched.
            if sic not in best or (d.get("active") and not best[sic][1]):
                best[sic] = (tic, bool(d.get("active")))
    return {sic: tic for sic, (tic, _) in best.items()}


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: build_sic_code_map.py <tickers_all_5y_metadata.jsonl>")

    reps = representatives(sys.argv[1])
    print(f"distinct sic_description values: {len(reps)}")

    done = {}
    if os.path.exists(OUT):
        with open(OUT) as f:
            done = json.load(f)
        print(f"already mapped: {len(done)}")

    todo = [(sic, tic) for sic, tic in reps.items() if sic not in done]
    print(f"to fetch: {len(todo)}\n")

    mismatched, failed, n = [], [], 0
    t0 = time.time()

    def work(pair):
        sic, tic = pair
        code, got_desc, err = fetch(tic)
        return sic, tic, code, got_desc, err

    with ThreadPoolExecutor(max_workers=BATCH) as pool:
        for sic, tic, code, got_desc, err in pool.map(work, todo):
            n += 1
            if err or not code:
                failed.append((sic, tic, err or "no sic_code"))
            else:
                done[sic] = code
                # The vendor's current description for this ticker should match
                # the one in our file. If it drifted, the mapping is suspect.
                if got_desc and got_desc.strip() != sic:
                    mismatched.append((sic, got_desc.strip(), tic))
            if n % 20 == 0:
                rate = n / max(time.time() - t0, 1e-6)
                print(f"  {n}/{len(todo)}  {rate:.1f}/s  "
                      f"~{(len(todo) - n) / max(rate, 1e-6) / 60:.1f} min left  "
                      f"({len(failed)} failed)")
                with open(OUT, "w") as f:
                    json.dump(done, f, indent=1, sort_keys=True)

    with open(OUT, "w") as f:
        json.dump(done, f, indent=1, sort_keys=True)
    print(f"\nwrote {len(done)} mappings to {OUT}")

    # A 2-digit major group with only one 4-digit code under it is a hint that
    # the collapse will not merge much; a lot of them means it will.
    groups = {}
    for sic, code in done.items():
        groups.setdefault(str(code)[:2], []).append(sic)
    print(f"distinct 2-digit major groups: {len(groups)}")
    print("largest merges:")
    for g, members in sorted(groups.items(), key=lambda kv: -len(kv[1]))[:8]:
        print(f"  {g}: {len(members)} descriptions")

    if mismatched:
        print(f"\n{len(mismatched)} description mismatches (vendor text drifted):")
        for ours, theirs, tic in mismatched[:10]:
            print(f"  {tic}: ours={ours!r} vendor={theirs!r}")
    if failed:
        print(f"\n{len(failed)} unmapped -- re-run to retry:")
        for sic, tic, err in failed[:10]:
            print(f"  {tic}  {err}  ({sic})")


if __name__ == "__main__":
    main()
