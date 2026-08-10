# Dividend Event Tables

The measurement layer. Takes every dividend event, pins a window of trading
days around its ex-date, joins prices, and computes what happened.

Two outputs. The **panel** is one row per (event × trading-day offset) — the
raw evidence, and what a time-decay curve reads. The **grain** table collapses
that to one row per event with the metrics computed. Most analysis wants the
grain table; go to the panel when you need to see the shape of the window.

Covers **2021-08-09 through 2026-08-07** — 1,255 trading days, 163,358 events.

---

## Where the data lives

| | Path |
|---|---|
| **HDFS — grain (start here)** | `$TEAM/curated/div_event_grain` — 163,358 events |
| HDFS — grain as CSV | `$TEAM/curated/div_event_grain_csv` |
| **HDFS — panel** | `$TEAM/curated/div_event_panel` — 1,470,222 rows, partitioned by `ex_ym` |
| Committed sample | `m1_dividend_events/sample_div_event_grain.jsonl` |

```python
grain = spark.read.parquet(f"{TEAM}/curated/div_event_grain")
panel = spark.read.parquet(f"{TEAM}/curated/div_event_panel")
```

Point at the **directory**, never a `part-*` file — part filenames carry a
per-run UUID that changes every run. The panel's `ex_ym` partition column is
reconstructed from the directory names, so filtering on it skips whole
directories rather than scanning everything.

The grain CSV is ~50 MB and stays on HDFS. Do not commit it; commit a sample.

---

## What one row means

Grain table, one row per `(ticker, ex_date)`.

The strategy under test: **buy at the close on ex−1, sell at the open on the
ex-date, collect the dividend.** Theory says the price should fall by exactly
the dividend, making this a wash. Decades of literature say it falls by less.
The question is whether the gap is large enough to trade.

### Core prices

| column | meaning |
|---|---|
| `prev_close` | close on the last trading day before the ex-date (cum-dividend) |
| `ex_open` | open on the ex-date |
| `ex_close` | close on the ex-date |
| `cash_amount` | dividend per share, **raw** — see Unadjusted prices below |

### Measurements

| column | formula | reads as |
|---|---|---|
| `drop_ratio` | `(prev_close − ex_open) / cash_amount` | 1.0 = full drop; below 1 = the anomaly |
| `drop_pct` | `(prev_close − ex_open) / prev_close` | the drop as a return |
| `div_yield` | `cash_amount / prev_close` | how big the dividend is relative to price |
| `capture_ret` | `(ex_open − prev_close + cash_amount) / prev_close` | **gross return on the trade** |
| `capture_ret_abn` | `capture_ret − mkt_overnight_ret` | same, minus SPY's overnight move |
| `hold_ret` | sell at +3 instead of the open | does waiting help? |

**Prefer `capture_ret_abn` as the primary metric; use `drop_ratio` for
framing.** `drop_ratio` divides by the dividend, so a small dividend turns
ordinary volatility into an enormous number — MU paid $0.15, moved $31.44
overnight, and posted a `drop_ratio` of −209.6. That is measuring volatility,
not dividend behaviour. Its `capture_ret` is a sane +3.2%. Across the full
table this shows up as a mean `drop_ratio` of 0.449 against a median of 0.870:
a long left tail from small denominators. **Use the median if you use it at
all, and never model on it.**

### Window statistics

| column | meaning |
|---|---|
| `pre_avg_ret`, `pre_avg_abn_ret` | mean daily return over ex−4 … ex−1 (drift going in) |
| `pre_vol` | stdev of those returns (volatility bucket) |
| `pre_avg_volume`, `pre_avg_dollar_volume` | liquidity/size, correctly dated |
| `post_avg_ret`, `post_avg_abn_ret` | mean daily return over ex+1 … ex+3 (does it revert?) |

`pre_avg_ret` spans ex−4 to ex−1, not ex−5: five bars give four daily changes,
because the return at offset −5 would need a bar outside the window.

**Use `pre_avg_dollar_volume` as the size proxy, not `market_cap`.** Market cap
from the ticker metadata is a *current* snapshot and is wrong for any event in
the past. Dollar volume is computed from the price data and is correctly dated
by construction.

### Flags — nothing is filtered

The table records facts and lets the analysis choose. Every flag is a column,
not a deletion.

| flag | meaning | count |
|---|---|---|
| `has_core` | both `prev_close` and `ex_open` present | 161,316 (98.7%) |
| `window_complete` | all 5 pre and 3 post bars present | 158,543 (97.1%) |
| `window_contiguous` | window spans ≤ 30 calendar days | 163,358 (**0 failures**) |
| `drop_ratio_extreme` | `abs(drop_ratio) > 20` | 1,847 (1.1%) |
| `low_yield` | `div_yield < 0.005` | 75,601 (46.3%) |

`low_yield` at 46% is the noise problem, not a data defect: a 0.4% yield on a
stock that routinely moves 1–2% a day means the denominator is smaller than the
daily noise. It is also, as it turns out, most of the answer — see Results.

---

## Coverage

| | value |
|---|---|
| Trading days in calendar | 1,255 (2021-08-09 … 2026-08-07) |
| Dividend events, total | 164,728 |
| Events with an ex-date inside the calendar | **163,358 (99.2%)** |
| Panel rows | 1,470,222 (exactly 9.00 × events) |
| **Panel rows with a price bar** | **1,454,655 (98.9%)** |

Events per year — 2021 and 2026 are partial:

| year | events | has_core | complete |
|---|---|---|---|
| 2021 | 11,804 | 11,487 | 10,790 |
| 2022 | 27,567 | 27,135 | 26,710 |
| 2023 | 29,470 | 29,147 | 28,757 |
| 2024 | 31,838 | 31,529 | 31,180 |
| 2025 | 37,640 | 37,343 | 37,002 |
| 2026 | 25,039 | 24,675 | 24,104 |

Structural checks clean: 0 duplicate `(ticker, ex_date)` keys, 0 rows where
`has_core` is true but a core price is null, 0 non-positive prices, panel/grain
ratio exactly 9.00.

### Why 1,370 events are out of scope

All of them have ex-dates before **2021-08-09**. That is the vendor entitlement
boundary, not a pipeline gap: the flat-file plan grants a *rolling* five years,
and older objects return 403 on GetObject even though LIST succeeds. Verified
by bisection — 2021-08-06 forbidden, 2021-08-09 accessible.

**The boundary moves.** Re-running the price sync next month yields a window
starting ~2021-09-10. The landed HDFS data is the canonical artifact; a re-pull
does not reproduce it, it shortens it. Do not casually delete `probe/`.

---

## Results

n = 161,296 (core prices, contiguous window, SPY excluded).

| | value |
|---|---|
| `drop_ratio` median | **0.870** |
| mean `capture_ret_abn` | +9.97 bps |
| median `capture_ret_abn` | +5.90 bps |
| win rate (`capture_ret_abn` > 0) | 54.6% |

**The anomaly reproduces.** The price falls by ~87% of the dividend, not 100%.
That is the Elton–Gruber result.

### It scales with yield — this is the finding

| yield bucket | n | median `drop_ratio` | mean abn | **median abn** |
|---|---|---|---|---|
| < 0.25% | 25,460 | 0.756 | 0.7 bps | **−0.4 bps** |
| 0.25–0.5% | 49,916 | 0.882 | 5.1 bps | 2.8 bps |
| 0.5–1% | 47,559 | 0.853 | 9.3 bps | 7.0 bps |
| 1–2% | 26,501 | 0.866 | 17.5 bps | 13.5 bps |
| ≥ 2% | 11,860 | 0.917 | 36.3 bps | **22.6 bps** |

Monotonic in both mean and median across 161K events. The median tracking the
mean matters — it means this is a broad effect, not a handful of outliers.

The mechanism is arithmetic and worth stating plainly:

```
capture_ret  ≈  div_yield × (1 − drop_ratio)
```

`drop_ratio` is roughly **flat at ~0.87 across every bucket**, so you keep ~13%
of the dividend regardless of its size. The return therefore scales with the
dividend. Check: the ≥2% bucket averages ~3% yield, and 0.13 × 3% = 39 bps
against 36 bps observed.

**So the thesis is sharper than "the anomaly exists."** At a plausible 5–20 bps
round trip, everything below 1% yield is dead. The ≥2% bucket — 11,860 events,
~2,400/year — is the only place a strategy could plausibly live. That is where
M2 should aim.

Stable year over year, so it is not one year carrying the result:

| year | n | median `drop_ratio` | median abn |
|---|---|---|---|
| 2021 | 11,485 | 0.826 | −0.9 bps |
| 2022 | 27,131 | 0.901 | +8.1 bps |
| 2023 | 29,143 | 0.856 | +8.0 bps |
| 2024 | 31,525 | 0.830 | +4.0 bps |
| 2025 | 37,339 | 0.889 | +9.9 bps |
| 2026 | 24,673 | 0.899 | +3.1 bps |

2021 is negative but is a partial year of ~11K events at the entitlement edge.

---

## Two things to know before using the table

### Exclude SPY

It is the market proxy, so for its own 20 events the `capture_ret_abn`
subtraction cancels the price move exactly and leaves the dividend yield — a
guaranteed "profit" that is pure construction. Verified: for every SPY row,
`capture_ret_abn` equals `div_yield` to the last decimal.

```python
core = grain.filter(F.col("ticker") != "SPY")
```

Negligible in aggregate, but it sits at the top of any liquidity-sorted view.

### The time-decay curve has an unexplained spike

Mean abnormal return by offset:

| offset | −4 | −3 | −2 | −1 | **0** | +1 | +2 | +3 |
|---|---|---|---|---|---|---|---|---|
| mean abn | +1.4 bps | **+20.4** | +5.8 | −0.5 | **−87.6** | −0.2 | +3.7 | +0.2 |

Offset 0 is the anomaly's footprint and looks right. The +20.4 bps at offset −3
is large and oddly localised; cumulative pre-drift is +27 bps, most of the
ex-day drop.

Most likely explanation is **event clustering, not economics**: ex-dates
concentrate on particular calendar dates, so "offset −3" is the same handful of
dates for thousands of events at once, and an equal-weighted cross-section
market-adjusted against SPY does not cancel on a big market day. Test it:

```python
panel.filter("offset = -3").groupBy("bar_date").agg(
    F.count("*").alias("n"), F.mean("abn_ret_cc").alias("abn")
).orderBy(F.desc("n")).show(20)
```

If a few dates carry the weight, that is the answer — and it also means
**daily observations are not independent**, which matters for any significance
testing and is worth stating in the report either way. The alternative reading
is genuine pre-ex run-up, which is documented in the literature. Resolve before
the curve goes in a writeup.

---

## How it works

### Trading-day offsets come from a global market calendar

Offsets are **trading** days. The obvious implementation —
`lag(close).over(partitionBy("ticker").orderBy("date"))` — takes the previous
row *present for that ticker*, so an illiquid name that did not trade for a
week silently yields a "previous close" from a week earlier, and nothing
errors. Phase 1's probe script had exactly this shape.

Instead: build one market-wide calendar from the distinct dates in the day
aggregates, index it densely, and join prices on `(ticker, calendar_date)` with
a **left** join. A missing bar then surfaces as a null we can count, not a
wrong number we cannot see. That is what `n_bars_pre` / `n_bars_post` measure,
and why 98.9% panel coverage is a meaningful number rather than an assumption.

### The contiguity flag

The calendar is built from whatever months are loaded. If those months are not
adjacent, consecutive calendar indices straddle the gap and offset −1 could
resolve to a date years earlier. That was a live problem when `day_2024-02`
sat alone under `probe/` next to 2026 data.

The full sync closed the gap, so **the flag now reports 0 failures and running
with no month arguments is safe.** The flag stays because the failure mode
recurs the moment anyone loads a partial window. Every panel row carries
`span_days`, the true calendar distance from the ex-date.

### Duplicate ex-dates are summed, not dropped

Multiple distributions can share a `(ticker, ex_date)` — a regular plus a
special, or a corrected record. 165,888 raw rows collapse to 164,728 events. A
duplicate left in place fans out in the join and double-counts. The price drop
on a given date reflects *all* distributions that day, so `cash_amount` is
summed and `n_distributions` preserves the fact.

### Metadata joins are date-aware

60 ticker symbols appear twice in the reference data — once delisted under the
company that used to hold them, once active under whoever holds them now.
**Ticker alone is not a unique join key.** The join keeps candidate rows valid
as of the ex-date, then prefers the delisted record when the event predates the
delisting, since that is the company that actually paid.

### Unadjusted prices

Flat-file prices are **not** adjusted for splits. Use the raw `cash_amount`,
never `split_adjusted_cash_amount` — the dividend must be in the same share
basis as the price you divide it by. A split inside the window still corrupts
the ratio. Splits are not pulled yet; `drop_ratio_extreme` (1,847 events) is a
heuristic stand-in until they are. **This is the first thing M2 should fix.**

---

## Running it

Requires `$TEAM` set, the reference files on HDFS, and the day aggregates
synced — see `ticker/README_TICKER.md`, `dividends/README_DIVIDEND.md`, and
`ingest/gen_pull_day_agg_data.py`.

```bash
# all months present under probe/ -- the normal case
spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 4g --executor-cores 2 \
    gen_dividend_events_table.py

# explicit months, for a faster iteration loop
spark-submit ... gen_dividend_events_table.py 2026-07 2026-08
```

Runs in ~2.5 minutes on the full window. `--deploy-mode client` is
**required**: in cluster mode the driver runs on an arbitrary node with no
inherited environment and `$TEAM` silently falls through to the hardcoded
default.

Verify afterwards — structural checks, coverage by year, SPY circularity,
headline cuts, yield buckets, year-by-year, and the time-decay curve:

```bash
spark-submit ... util/check_event_tables.py
```

Pull the grain table down (~50 MB):

```bash
hdfs dfs -getmerge $TEAM/curated/div_event_grain_csv/*.csv ~/div_event_grain.csv
wc -l ~/div_event_grain.csv    # 163359 = 163358 events + header
```

### Tuning knobs

At the top of the script:

| constant | current | note |
|---|---|---|
| `PRE` / `POST` | 5 / 3 | trading days before/after |
| `MAX_SPAN_DAYS` | 30 | contiguity threshold |
| `MARKET` | `SPY` | market proxy for abnormal returns |
| `spark.sql.shuffle.partitions` | 64 | cluster default is 1000, which fragments small outputs into 1000 tiny files |

---

## Starting Milestone 2

M2 is "measurement to strategy." The grain table is the input.

**1. Pull splits.** `/stocks/v1/splits` returns execution dates and ratio
factors. Any event whose window contains a split has a meaningless
`drop_ratio` right now — 1,847 events are flagged extreme and splits are the
likeliest cause. This is a correctness fix, and everything downstream inherits
the error until it is done.

**2. Focus on the high-yield subset.** The yield-bucket result says everything
below 1% yield is noise at realistic costs. The ≥2% bucket is 11,860 events,
~2,400/year — enough to backtest, and the only place the effect clears a
plausible spread. Build the strategy analysis there rather than on the full
161K.

**3. Decide the universe filter.** Join `sec_type` from
`$TEAM/reference/tickers_all_5y_metadata.jsonl`. ETFs slightly outnumber common
stock in the ticker universe (5,332 vs 5,303) and distribute far more often, so
they dominate the event count. A fund distribution is not the Elton–Gruber
phenomenon — no earnings-retention decision, just pass-through of underlying
income. Proposed: primary on `CS`, `ETF` as a named comparison group,
`CS + ETF + ADRC` as the outer bound. **Team decision, not settled.**

**4. Cross-sectional buckets.** Everything needed is on the grain table:
`div_yield`, `pre_vol`, `pre_avg_dollar_volume`, plus `sic_description` from
the metadata join. Sector coverage is ~45% of active tickers and 0% of
delisted — SIC classifies operating companies only, so ETFs have none. 376
distinct `sic_description` values is too granular; group to SIC major group
first.

**5. Transaction costs.** This is now the deciding question rather than a
formality: the ≥2% bucket returns 22.6 bps median, and whether that survives a
round trip determines the whole result. NBBO quotes give a measured spread
instead of an assumption — `quotes_v1` flat files, or the REST Quotes endpoint
for spot checks. Note high-yield names skew smaller and less liquid, so their
spreads are likely wider than the market average.

**6. Confounders.** Earnings announcements inside the ±3 window mean the price
move has nothing to do with the dividend; Benzinga earnings dates let you flag
and exclude. Short interest is the strongest unexploited variable available —
short sellers must pay the dividend to the lender, so ex-dates interact
directly with borrow demand, and high short interest also proxies
hard-to-borrow status, which constrains whether the trade is executable at all.

### Things that will bite

- **`ex_dividend_date` is the anchor.** `record_date`, `pay_date`, and
  `declaration_date` are carried through but none is where the price moves.
  Under T+1 (since May 2024) record date equals ex-date; in older data they
  differ by a day. Settlement-cycle change, not an error.
- **Preferred tickers use a lowercase `p` separator** (`AGMpE`), not `PR`.
  Any regex written against the old assumption will miss them.
- **Observations are probably not independent** — see the time-decay spike.
  Naive t-tests across 161K events will overstate significance.
- **`drop_ratio` mean ≠ median** by a wide margin. Anything that averages it is
  reporting the tail.
