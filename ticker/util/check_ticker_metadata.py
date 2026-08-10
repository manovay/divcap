#!/usr/bin/env python3
"""
Validate the enriched ticker metadata file.

Checks structural integrity (every input row present exactly once, uniform
key set, parseable JSON) and reports field coverage so that missing data is
a known quantity rather than something discovered mid-analysis.

Exits non-zero if a structural check fails. Coverage gaps are reported but
are not failures -- Ticker Overview genuinely has no record for most
delisted names, and that is expected, not broken.

Run (from anywhere):
    python3 ticker/util/check_ticker_metadata.py
    python3 ticker/util/check_ticker_metadata.py <input.jsonl> <metadata.jsonl>

Defaults resolve relative to ticker/, not the working directory.
"""

import os
import sys
import json

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.dirname(HERE)

DEFAULT_IN = os.path.join(DATA, "tickers_all_5y.jsonl")
DEFAULT_META = os.path.join(DATA, "tickers_all_5y_metadata.jsonl")

BASE_FIELDS = ["ticker", "market", "locale", "type", "active",
               "last_updated_utc"]
OVERVIEW_FIELDS = ["address", "description", "list_date", "sic_description",
                   "market_cap", "total_employees",
                   "weighted_shares_outstanding"]


def pct(a, b):
    if b == 0:
        return 0.0
    return 100.0 * a / b


def load_jsonl(path):
    """Returns (records, bad_line_numbers)."""
    rows = []
    bad = []
    n = 0
    for line in open(path):
        n += 1
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            bad.append(n)
    return rows, bad


def coverage(rows, fields, label):
    print(f"field coverage -- {label} ({len(rows)} rows)")
    for f in fields:
        have = 0
        for r in rows:
            if r.get(f) is not None:
                have += 1
        flag = ""
        if have == 0:
            flag = "   <- always null"
        print(f"  {f:30s} {have:6d}  {pct(have, len(rows)):5.1f}%{flag}")
    print()


def main():
    if len(sys.argv) > 1:
        in_path = sys.argv[1]
    else:
        in_path = DEFAULT_IN

    if len(sys.argv) > 2:
        meta_path = sys.argv[2]
    else:
        meta_path = DEFAULT_META

    for p in (in_path, meta_path):
        if not os.path.exists(p):
            sys.exit(f"missing: {p}")

    fails = []

    src, src_bad = load_jsonl(in_path)
    meta, meta_bad = load_jsonl(meta_path)

    print(f"input     {len(src):6d} rows   {os.path.basename(in_path)}")
    print(f"metadata  {len(meta):6d} rows   {os.path.basename(meta_path)}")
    print()

    if src_bad:
        fails.append(f"{len(src_bad)} unparseable lines in input: {src_bad[:5]}")
    if meta_bad:
        # Most likely a truncated final line from a killed run. The puller
        # tolerates this on resume, but the file should be repaired before use.
        fails.append(f"{len(meta_bad)} unparseable lines in metadata: "
                     f"{meta_bad[:5]}")

    # --- completeness -------------------------------------------------
    src_t = set()
    for r in src:
        src_t.add(r["ticker"])

    meta_t = []
    for r in meta:
        meta_t.append(r["ticker"])
    meta_set = set(meta_t)

    missing = src_t - meta_set
    extra = meta_set - src_t

    # A ticker can legitimately appear twice: once delisted under the company
    # that used to hold the symbol, once active under whoever holds it now.
    # That is symbol reuse, not a broken write -- but it does mean ticker
    # alone is not a unique key for joining. Two of the SAME active status is
    # a real duplicate.
    by_ticker = {}
    for r in meta:
        t = r["ticker"]
        if t not in by_ticker:
            by_ticker[t] = []
        by_ticker[t].append(r)

    reuse = []
    dupes = []
    for t in by_ticker:
        rs = by_ticker[t]
        if len(rs) < 2:
            continue
        states = set()
        for r in rs:
            states.add(bool(r.get("active")))
        if len(states) == len(rs):
            reuse.append(t)
        else:
            dupes.append(t)

    print(f"tickers in input     {len(src_t)}")
    print(f"tickers in metadata  {len(meta_set)}")
    print(f"missing              {len(missing)}")
    print(f"extra                {len(extra)}")
    print(f"symbol reuse         {len(reuse)}  (active + delisted, same symbol)")
    print(f"true duplicates      {len(dupes)}")
    print()

    if reuse:
        print("symbol reuse means ticker is NOT a unique join key --")
        print("joins need ticker + a date bound (list_date / delisted_utc).")
        print(" ", sorted(reuse)[:15])
        print()

    if missing:
        fails.append(f"{len(missing)} input tickers absent from metadata: "
                     f"{sorted(missing)[:5]}")
    if extra:
        fails.append(f"{len(extra)} metadata tickers not in input: "
                     f"{sorted(extra)[:5]}")
    if dupes:
        fails.append(f"{len(dupes)} tickers written twice with the same "
                     f"active status: {sorted(dupes)[:5]}")

    # --- uniform key set ----------------------------------------------
    expect = set(BASE_FIELDS + OVERVIEW_FIELDS + ["overview_error"])
    odd = 0
    for r in meta:
        if set(r.keys()) != expect:
            odd += 1
    if odd:
        fails.append(f"{odd} rows with a non-standard key set")

    # --- base fields must never be null -------------------------------
    # These are carried straight through from the input, so a null means the
    # copy is broken, not that the vendor lacks data.
    for f in ["ticker", "market", "locale", "active"]:
        n = 0
        for r in meta:
            if r.get(f) is None:
                n += 1
        if n:
            fails.append(f"{n} rows with null {f}")

    # --- errors should track the active/delisted split ----------------
    act = []
    dead = []
    for r in meta:
        if r.get("active"):
            act.append(r)
        else:
            dead.append(r)

    err_act = 0
    for r in act:
        if r.get("overview_error") is not None:
            err_act += 1
    err_del = 0
    for r in dead:
        if r.get("overview_error") is not None:
            err_del += 1

    print(f"active    {len(act):6d}   {err_act} with overview_error "
          f"({pct(err_act, len(act)):.1f}%)")
    print(f"delisted  {len(dead):6d}   {err_del} with overview_error "
          f"({pct(err_del, len(dead)):.1f}%)")
    print()

    codes = {}
    for r in meta:
        e = r.get("overview_error")
        if e is None:
            continue
        if e not in codes:
            codes[e] = 0
        codes[e] += 1
    if codes:
        print("error codes")
        pairs = []
        for c in codes:
            pairs.append((codes[c], c))
        pairs.sort(reverse=True)
        for n, c in pairs:
            print(f"  {c:20s} {n:6d}")
        print()
        # Anything that is not a 404 is a transient failure worth retrying,
        # since 404 means the vendor genuinely has no record.
        transient = 0
        for c in codes:
            if c != "404":
                transient += codes[c]
        if transient:
            fails.append(f"{transient} non-404 errors -- delete those lines "
                         f"and re-run to retry")

    # --- field coverage, split by phase -------------------------------
    coverage(act, OVERVIEW_FIELDS, "active")
    if dead:
        coverage(dead, OVERVIEW_FIELDS, "delisted")

    # --- sanity on the values themselves ------------------------------
    bad_cap = 0
    bad_emp = 0
    for r in meta:
        c = r.get("market_cap")
        if c is not None and c <= 0:
            bad_cap += 1
        e = r.get("total_employees")
        if e is not None and e < 0:
            bad_emp += 1
    if bad_cap:
        fails.append(f"{bad_cap} rows with non-positive market_cap")
    if bad_emp:
        fails.append(f"{bad_emp} rows with negative total_employees")

    # sic_description granularity: too many distinct values to bucket on
    # directly is expected, and is a note for whoever builds the sector
    # groupings rather than a defect.
    sics = set()
    for r in meta:
        s = r.get("sic_description")
        if s is not None:
            sics.add(s)
    print(f"distinct sic_description values: {len(sics)}")
    print()

    # --- verdict ------------------------------------------------------
    if fails:
        print("FAILED")
        for m in fails:
            print(f"  - {m}")
        sys.exit(1)

    print("OK -- structure and completeness check out")


if __name__ == "__main__":
    main()