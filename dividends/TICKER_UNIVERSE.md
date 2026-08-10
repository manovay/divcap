# Ticker Universe

How we define the set of securities a dividend-capture trade could actually be
executed on, and why it comes from the price files rather than the reference API.

---

## 1. What's in the SIP flat files

Massive's stock flat files all live under `us_stocks_sip/`. SIP = Securities
Information Processor: under Reg NMS, trades and quotes from every venue are
consolidated into one official tape by two processors — CTA/CQS (NYSE-listed,
Tape A; NYSE American / Arca / regional, Tape B) and UTP (Nasdaq-listed, Tape C).

So `us_stocks_sip` = **NMS-listed securities only**. That includes common stock,
preferreds, ETFs, ADRs, closed-end funds, warrants, and units. It excludes:

- **OTC** — dealer-quoted securities that aren't NMS. Massive sells OTC data
  separately; there is no OTC flat-file dataset.
- **Mutual funds** — they don't trade intraday. NAV is struck once after the
  close, so there is no tape to consolidate.

Day aggregates give one row per ticker per trading day, ~10,500 tickers/day.
That set *is* the tradeable universe for that date.

## 2. Why not drive the universe off the reference/dividends API

The REST endpoints cover Massive's full catalog — exchanges, dark pools, FINRA
facilities, **and OTC markets**. That's much wider than the SIP tape, so a
universe built from the API is mostly securities we have no prices for.

Measured on the Phase 1 probe (`/v3/reference/dividends`, Feb 7–13 2024):

| | count | note |
|---|---|---|
| dividend events returned | 2,888 | ~150K/yr annualized |
| `frequency: 12` (monthly) | 2,499 | overwhelmingly funds |
| `frequency: 4` (quarterly) | 234 | ≈ what operating companies pay |
| `dividend_type: CD` / `SC` | 2,884 / 4 | specials are negligible |
| joined to price data | 263 | ~9% |

Roughly 70% of the returned tickers are five-or-six-letter symbols ending in `X`
— mutual funds. The remainder of the misses are OTC foreign ordinaries (`F`
suffix), OTC common, and preferreds (Polygon writes these with a `PR` infix).

**This is not a join bug.** The dividends endpoint and the flat files describe
different universes; the intersection is small because most US distributions are
paid by instruments with no exchange tape. Excluding them is also methodologically
correct — a capture trade requires buying before the close and selling at the
ex-date open, which is undefined on an instrument that has no intraday price.

Consequence: the proposal's ~750K event estimate is ~10x too high. The real
tradeable event count is closer to 50–70K over five years. The big-data
constraint lives on the price side (23 GB of minute bars), not the event table.

## 3. The mechanism

Collapse the day aggregates to one row per ticker. Price-driven membership is
self-validating, correctly dated, and free of survivorship bias — unlike a
current-snapshot symbol directory, which can't tell you what was listed in 2024.

`dividends/gen_ticker_universe.py` — PySpark, `groupBy("ticker")`.

| | Path |
|---|---|
| Input | `$TEAM/probe/day_*.csv.gz` (defaults to `day_2024-02-08.csv.gz`) |
| Output (Parquet) | `$TEAM/curated/ticker_universe` |
| Output (CSV) | `$TEAM/curated/ticker_universe_csv` |
| Committed copy | `results/ticker_universe_2024-02-08.csv` |

Columns: `ticker`, `n_days`, `total_volume`, `total_trades`, `last_close`.

`n_days` is only meaningful over a glob — it tells you how many of the loaded
days each ticker traded on. `last_close` has no explicit ordering; treat it as a
price scale for bucketing, not as a real closing price.

Current output (Feb 8 2024): **10,484 tickers, 80 with zero volume.** Cross-checked
against `cut -d, -f1` on the raw file — same count.

Note the universe is not clean common stock: `AACIW`, `AAGRW`, and `AACT.WS` in
the first 20 rows are warrants. Security type still has to come from the
reference API, and it matters for M3 — preferreds have coupon-anchored,
near-mechanical ex-date drops, and ETFs distribute pass-through income. Pooling
them with common stock blurs the effect being measured.

## 4. Regenerating

```bash
# single day (default)
spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 4g --executor-cores 2 \
    dividends/gen_ticker_universe.py

# wider window
spark-submit ... dividends/gen_ticker_universe.py "$TEAM/probe/day_*.csv.gz"
```

`--deploy-mode client` is required — in cluster mode the driver runs on an
arbitrary node with no inherited environment and `$TEAM` silently falls through
to the hardcoded default.

Pull the CSV down and commit it:

```bash
hdfs dfs -getmerge $TEAM/curated/ticker_universe_csv/*.csv ~/ticker_universe.csv
wc -l ~/ticker_universe.csv   # expect 10485 (10484 + header)
```

Then download via the browser SSH gear icon → Download file →
`/home/<netid>/ticker_universe.csv`, and commit to `results/` **with the date in
the filename**. The HDFS output path doesn't change between runs, so an undated
committed copy silently goes stale the moment someone runs a wider glob.

The committed CSV is the canonical list — anything referencing "the universe"
should point at it or at the Parquet, never at a `part-*` file. Part filenames
carry a per-run UUID and change every time.

Reading downstream — always the directory, never a part file:

```python
tick = spark.read.parquet(f"{TEAM}/curated/ticker_universe")
```

### Gotcha: shuffle partitions

`spark.sql.shuffle.partitions` is **1000** on this cluster. Any global `orderBy`
before a write produces 1,000 part files regardless of row count — the first run
wrote 1,000 Parquet fragments of ~1.8 KB each, totalling more than the raw CSV,
because per-file footers and schema swamped ten rows of data. Thousands of tiny
files also bloat NameNode memory on a cluster shared with the whole class.

Keep the sort on the CSV branch only, where `coalesce(1)` already collapses it.
`coalesce(1)` is fine at 10K rows and will not scale — at full history, drop the
sort and let partition count follow data size, targeting ~128 MB per file.

## 5. Open

- Pull `type` / `primary_exchange` from `/v3/reference/tickers` (bulk-paginate
  `market=stocks`, both `active=true` and `active=false`), land on HDFS, join.
  Needed before M3. Requires a REST key.
- Universe membership is currently a single day. At full history it has to become
  per-date — listings and delistings make any global list wrong over five years.
