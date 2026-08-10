#!/usr/bin/env python3
"""
Pull five years of cash dividends, filtered to our ticker universe.

NOTE: this uses /stocks/v1/dividends, NOT /v3/reference/dividends. The latter
is deprecated. Field names differ: the current endpoint returns
`distribution_type` (recurring / special / supplemental / irregular /
unknown) rather than `dividend_type` (CD / SC), and adds
`split_adjusted_cash_amount` and `historical_adjustment_factor`.

Use the RAW `cash_amount`, not `split_adjusted_cash_amount`, when dividing by
flat-file prices. The flat files are unadjusted, so the dividend has to be in
the same share basis as the price. Mixing an adjusted dividend with an
unadjusted price is the corruption M2 exists to avoid.

The ticker parameter is optional, so the endpoint returns the whole market --
and ~70% of that is mutual funds with no exchange tape, which can never join
to a price. Rows are filtered against the ticker metadata file as they
arrive. Per-month seen/kept counts are printed so the exclusion rate is still
reportable without storing ~500K unusable rows.

The metadata file is the right filter, not the SIP ticker list: it includes
delisted names, whereas the SIP list only covers the months of price data
actually loaded and would drop every event from earlier years.

Chunked by MONTH, one file per month under the output directory. A single
five-year cursor walk is ~750 pages and a crash midway loses all of it;
per-month files make resume trivial (skip months whose file exists). Each
month writes to .part and renames on success, so a half-written month is
never mistaken for a finished one.

Run:
    python3 dividends/gen_all_dividends.py                 # default 5 years
    python3 dividends/gen_all_dividends.py 2
    python3 dividends/gen_all_dividends.py 5 divs_out
    python3 dividends/gen_all_dividends.py 5 divs_out <metadata.jsonl>

Output: <outdir>/divs_YYYY-MM.jsonl per month, plus <outdir>/dividends_<N>y.jsonl

If a previous run pulled UNFILTERED, delete the output directory first --
existing month files are skipped, not re-filtered.

Needs MASSIVE_API_KEY (REST key, not the S3 credential).
"""

import os
import sys
import json
import time
import datetime
import urllib.request
import urllib.error

BASE = "https://api.massive.com/stocks/v1/dividends"

GTE = "ex_dividend_date.gte"
LTE = "ex_dividend_date.lte"
LIMIT = 1000

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
DEFAULT_TICKERS = os.path.join(REPO, "ticker", "tickers_all_5y_metadata.jsonl")

if "MASSIVE_API_KEY" not in os.environ:
    sys.exit("MASSIVE_API_KEY is not set -- see the shell profile setup")
KEY = os.environ["MASSIVE_API_KEY"]


def load_tickers(path):
    """
    Distinct ticker symbols from the metadata file. Symbol reuse means the
    same ticker can appear twice (active + delisted); a set collapses that,
    which is correct here since we only need membership.
    """
    out = set()
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if "ticker" in r:
            out.add(r["ticker"])
    return out


def months_back(years):
    """List of YYYY-MM strings from `years` ago through the current month."""
    today = datetime.date.today()
    y = today.year - years
    m = today.month
    out = []
    while (y, m) <= (today.year, today.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def month_bounds(ym):
    """('2024-02') -> ('2024-02-01', '2024-02-29')."""
    y, m = ym.split("-")
    y = int(y)
    m = int(m)
    first = datetime.date(y, m, 1)
    if m == 12:
        nxt = datetime.date(y + 1, 1, 1)
    else:
        nxt = datetime.date(y, m + 1, 1)
    last = nxt - datetime.timedelta(days=1)
    return first.isoformat(), last.isoformat()


def fetch(url, tries=5):
    """
    One page, with backoff. 429 is the expected failure on a rate-limited
    plan; sleeping and retrying is cheaper than restarting the walk, since
    the cursor cannot be resumed from the middle.
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
                body = ""
                try:
                    body = e.read().decode("utf-8", "replace")[:400]
                except Exception:
                    pass
                raise RuntimeError(f"http {e.code} on {url[:120]}...\n{body}")
            print(f"    http {e.code}, sleeping {wait}s", flush=True)
            time.sleep(wait)
            wait *= 2


def pull_month(ym, path, keep):
    """Walk one month to exhaustion, keeping only known tickers."""
    lo, hi = month_bounds(ym)
    url = f"{BASE}?{GTE}={lo}&{LTE}={hi}&limit={LIMIT}&apiKey={KEY}"

    tmp = path + ".part"
    seen = 0
    kept = 0
    page = 0
    with open(tmp, "w") as f:
        while url:
            d = fetch(url)
            page += 1
            for x in d.get("results", []):
                seen += 1
                if x.get("ticker") not in keep:
                    continue
                f.write(json.dumps(x) + "\n")
                kept += 1
            f.flush()

            nxt = d.get("next_url")
            if nxt:
                url = f"{nxt}&apiKey={KEY}"
            else:
                url = None
    os.replace(tmp, path)
    return seen, kept, page


def main():
    if len(sys.argv) > 1:
        years = int(sys.argv[1])
    else:
        years = 5

    if len(sys.argv) > 2:
        outdir = sys.argv[2]
    else:
        outdir = os.path.join(HERE, f"divs_{years}y")

    if len(sys.argv) > 3:
        tick_path = sys.argv[3]
    else:
        tick_path = DEFAULT_TICKERS

    if not os.path.exists(tick_path):
        sys.exit(f"missing ticker metadata: {tick_path}")

    keep = load_tickers(tick_path)
    os.makedirs(outdir, exist_ok=True)
    months = months_back(years)

    print(f"universe: {len(keep)} tickers from "
          f"{os.path.basename(tick_path)}")
    print(f"window:   {months[0]} .. {months[-1]}  ({len(months)} months)")
    print(f"outdir:   {outdir}\n")

    tot_seen = 0
    tot_kept = 0
    done = 0
    t0 = time.time()
    for ym in months:
        path = os.path.join(outdir, f"divs_{ym}.jsonl")
        if os.path.exists(path):
            n = 0
            for _ in open(path):
                n += 1
            tot_kept += n
            done += 1
            print(f"  {ym}  skip ({n} rows already)", flush=True)
            continue

        seen, kept, page = pull_month(ym, path, keep)
        tot_seen += seen
        tot_kept += kept
        done += 1
        el = time.time() - t0
        if seen > 0:
            share = 100.0 * kept / seen
        else:
            share = 0.0
        print(f"  {ym}  {kept:6d} kept / {seen:6d} seen ({share:4.1f}%), "
              f"{page} pages   [{done}/{len(months)}, {el/60:.1f} min]",
              flush=True)

    # Combine. Rewritten from scratch each run so it always matches the parts.
    combined = os.path.join(outdir, f"dividends_{years}y.jsonl")
    n = 0
    with open(combined, "w") as out:
        for ym in months:
            path = os.path.join(outdir, f"divs_{ym}.jsonl")
            if not os.path.exists(path):
                continue
            for line in open(path):
                out.write(line)
                n += 1

    print()
    if tot_seen > 0:
        print(f"fetched {tot_seen} rows, kept {tot_kept} "
              f"({100.0 * tot_kept / tot_seen:.1f}%)")
    print(f"wrote {n} rows to {combined}")


if __name__ == "__main__":
    main()