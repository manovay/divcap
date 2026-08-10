# Dividends

Five years of US cash dividend events, filtered to our ticker universe.
**165,888 events, 2021-08 through 2026-08.**

## Where the data lives

| | Path |
|---|---|
| **HDFS (use this)** | `$TEAM/reference/dividends_5y.jsonl` — 61.4 MB, 165,888 rows |
| Local (after a pull) | `dividends/divs_5y/dividends_5y.jsonl` |
| **Committed sample** | `dividends/sample_dividends.jsonl` — 16 records, in git |

One JSON object per line, one line per dividend event.

The full file is gitignored (multi-MB vendor data; the working agreement keeps
data in HDFS rather than git). To see the record shape without credentials or a
cluster session, read the committed sample — it's picked to cover the
`frequency` and `distribution_type` values you'll actually branch on, not a
random slice.

Reading it in Spark:

```python
divs = spark.read.json(f"{TEAM}/reference/dividends_5y.jsonl")
```

Alongside it, from the ticker stage:
`$TEAM/reference/tickers_all_5y_metadata.jsonl` (20,729 rows).

---

## Read this before using the data

**We are NOT on `/v3/reference/dividends`.** That endpoint is deprecated and is
what `ingest/pull_divs.py` used. The current one is `/stocks/v1/dividends` and
the field names differ:

| old | new |
|---|---|
| `dividend_type`: `CD` / `SC` | `distribution_type`: `recurring` / `special` / `supplemental` / `irregular` / `unknown` |
| — | `split_adjusted_cash_amount` |
| — | `historical_adjustment_factor` |

Anything filtering on `dividend_type == "CD"` needs updating to
`distribution_type == "recurring"`.

**Use raw `cash_amount`, not `split_adjusted_cash_amount`.** The flat-file
prices we divide it by are unadjusted, so the dividend has to be in the same
share basis. Mixing an adjusted dividend with an unadjusted price is exactly
the corruption we're trying to avoid.

**`ex_dividend_date` is the one that matters.** Each event carries four dates:

| date | meaning |
|---|---|
| `declaration_date` | company announces it |
| `ex_dividend_date` | first day trading *without* the dividend — the drop happens at this open |
| `record_date` | ownership snapshot |
| `pay_date` | cash arrives, weeks later |

The strategy buys at the close on ex−1 and sells at the open on ex. Under T+1
(since May 2024) `record_date` equals `ex_dividend_date`; in older data they
differ by a day. That's the settlement-cycle change, not a data error.

---

## What's in it

Pulled the whole market, then filtered against
`ticker/tickers_all_5y_metadata.jsonl` (20,669 distinct tickers) as rows
arrived. **Keep rate runs 58–78% per month** — the rest are mutual funds and
OTC names with no exchange tape, which can never join to a price.

**165,888 events over 61 months**, averaging ~2,700/month. Two earlier estimates
were both wrong: the proposal's ~750K (extrapolated from a one-week probe that
included funds) was ~4.5x too high, and the 50–70K figure in
`ticker/README_TICKER.md` was ~2.5x too low. This number is measured.

Monthly counts swing ~4x — December 2021 had 4,195 events, January 2022 had
1,074. That's the quarterly dividend calendar: ex-dates cluster in the last
month of each quarter, and December adds year-end distributions. March, June,
September, December should consistently spike. If they don't, something is
wrong with the date filtering.

`frequency` is richer than expected: 0 (non-recurring), 1, 2, 3, 4, 12, 24, 52,
104, 365. Weekly-distributing ETFs (`frequency: 52`) exist and show up.

---

## Reproducing

Needs `MASSIVE_API_KEY` in your shell (REST key — **not** the S3 credential):

```bash
nano ~/.zshrc_local           # export MASSIVE_API_KEY=...
echo '[ -f ~/.zshrc_local ] && source ~/.zshrc_local' >> ~/.zshrc
source ~/.zshrc && chmod 600 ~/.zshrc_local
```

Requires `ticker/tickers_all_5y_metadata.jsonl` to exist first — see
`ticker/README_TICKER.md`.

```bash
cd dividends
python3 gen_all_dividends.py 5          # ~3 min
python3 util/make_sample.py             # refresh committed sample
```

Chunked by month into `divs_5y/divs_YYYY-MM.jsonl`, then concatenated into
`divs_5y/dividends_5y.jsonl`. Resume works by skipping months whose file
already exists, so an interrupted run picks up where it stopped. Each month
writes to `.part` and renames on success, so a half-written month is never
mistaken for a finished one.

**If a previous run used different filter settings, `rm -rf divs_5y` first.**
Existing month files are skipped, not re-filtered.

Push to HDFS. Only the combined file — the per-month parts are resume state.
At 61 MB the browser upload is slow and flaky, so gzip first (JSON Lines
compresses ~6x, down to ~11 MB):

```bash
gzip -k divs_5y/dividends_5y.jsonl     # -k keeps the original
# upload dividends_5y.jsonl.gz via the browser SSH gear icon

# on the cluster
gunzip dividends_5y.jsonl.gz
wc -l dividends_5y.jsonl                       # expect 165888
hdfs dfs -put dividends_5y.jsonl $TEAM/reference/
hdfs dfs -text $TEAM/reference/dividends_5y.jsonl | wc -l
```

Both counts must match — that's the check that the browser transfer didn't
truncate. `-put` fails rather than overwrites if the file exists; use `-put -f`
to replace.

---

## Joining downstream

```python
divs = spark.read.json(f"{TEAM}/reference/dividends_5y.jsonl")
meta = spark.read.json(f"{TEAM}/reference/tickers_all_5y_metadata.jsonl")
```

**Ticker alone is not a unique join key.** 60 symbols appear twice in the
metadata — once delisted under the company that used to hold them, once active
under whoever holds them now. Join on `ticker` plus a date bound
(`list_date` / `delisted_utc`), or filter to one record per ticker deliberately.

**Check for duplicate `(ticker, ex_dividend_date)` pairs** before joining. A
regular and a special distribution can share an ex-date, and a duplicate fans
out in the join and double-counts the event:

```python
divs.groupBy("ticker", "ex_dividend_date").count().filter("count > 1").show()
```

---

## Open

- **Splits are not pulled yet.** Flat-file prices are unadjusted, so a split
  inside an event window destroys the drop ratio. `/stocks/v1/splits` returns
  execution dates and ratio factors. Needed for M2.
- **Sector coverage is ~45% of active tickers** and 0% of delisted — SIC codes
  only classify operating companies. See `ticker/README_TICKER.md`.
- **Prices only cover 2026-07 and 2026-08.** Five years of dividends needs five
  years of day aggregates to join against: ~250 trading days/year × ~320 KB ≈
  400 MB. Manageable, not yet pulled. Until then a join matches only the ~5,000
  events with ex-dates in those two months — enough for a proof of concept.
- **JSON Lines is a poor storage format here.** 61 MB for 165,888 records is
  ~385 bytes/event, much of it repeated key names on every line. As Parquet it
  would be a few MB and read far faster. Natural first step inside the event
  table job rather than something to fix in the raw layer.
