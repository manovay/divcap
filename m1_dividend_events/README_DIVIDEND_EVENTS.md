# Dividend Event Tables

The measurement layer. Takes every dividend event, pins a window of trading
days around its ex-date, joins prices, and computes what happened.

Two outputs. The **panel** is one row per (event × trading-day offset) — the
raw evidence, and what a time-decay curve reads. The **grain** table collapses
that to one row per event with the metrics computed. Most analysis wants the
grain table; go to the panel when you need to see the shape of the window.

---

## Where the data lives

| | Path |
|---|---|
| **HDFS — grain (start here)** | `$TEAM/curated/div_event_grain` — 4,060 events |
| HDFS — grain as CSV | `$TEAM/curated/div_event_grain_csv` |
| **HDFS — panel** | `$TEAM/curated/div_event_panel` — 36,540 rows, partitioned by `ex_ym` |
| Committed copy | `m1_dividend_events/results/div_event_grain_2026-07_2026-08.csv` |

```python
grain = spark.read.parquet(f"{TEAM}/curated/div_event_grain")
panel = spark.read.parquet(f"{TEAM}/curated/div_event_panel")
```

Point at the **directory**, never a `part-*` file — part filenames carry a
per-run UUID that changes every run. The panel's `ex_ym` partition column is
reconstructed from the directory names, so filtering on it skips whole
directories rather than scanning everything.

**Current run covers 2026-07 and 2026-08 only** (27 trading days). That is a
proof of concept, not the deliverable: 4,060 of 164,728 events have an ex-date
inside the loaded calendar. The rest need more price data — see Open.

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

**Prefer `capture_ret` over `drop_ratio` as the primary metric.** `drop_ratio`
divides by the dividend, so a small dividend turns ordinary volatility into an
enormous number. From the current run: MU paid $0.15, moved $31.44 overnight,
and posted a `drop_ratio` of −209.6 — that is measuring volatility, not
dividend behaviour. Its `capture_ret` is a sane +3.2%. `drop_ratio` is the
right academic framing and belongs in the report; it is a poor thing to
average or model on.

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

| flag | meaning | current count |
|---|---|---|
| `has_core` | both `prev_close` and `ex_open` present | 3,305 of 4,060 |
| `window_complete` | all 5 pre and 3 post bars present | 2,620 |
| `window_contiguous` | window spans ≤ 30 calendar days | 4,060 (0 failures) |
| `drop_ratio_extreme` | `abs(drop_ratio) > 20` | 30 |
| `low_yield` | `div_yield < 0.005` | 1,471 |

The 755 events without `has_core` are almost entirely edge effects: an ex-date
on the first loaded day has no ex−1 bar. They shrink to near zero once the
price history is contiguous on both sides.

`low_yield` at 36% is the noise problem, not a data defect. A 0.5% yield on a
stock that routinely moves 1–2% a day means the denominator is smaller than the
daily noise. Expect a yield floor to do more for signal quality than the
extreme-ratio filter, which catches only 30 rows.

---

## Results from the current run

Events with both core prices and a contiguous window (n = 3,305):

| | value |
|---|---|
| `drop_ratio` median | **0.801** |
| `drop_ratio` p25 / p75 | 0.317 / 1.227 |
| `drop_ratio` mean | 0.664 |
| mean `capture_ret` | **+13.8 bps** |
| mean `capture_ret_abn` | **−6.4 bps** |

Two things to take from this.

**The anomaly reproduces.** The price falls by ~80% of the dividend, not 100%.
That is the Elton–Gruber result, and it is far tighter than the Phase 1 probe
(median 1.011, mean 1.299) — the market adjustment and a properly defined
universe did that.

**And it still is not tradeable.** Gross capture return is positive, but the
entire margin is market drift: SPY rose overnight on average across these 27
days. Market-adjusted, the strategy is **negative before any transaction
costs**. This is the finding the report should lead with, and it is the
justification for M2's cost modelling rather than an argument against it.

Caveat: 27 trading days in a single summer is a small, non-representative
window. Do not treat these numbers as the answer — treat them as evidence the
pipeline computes the right things.

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
wrong number we cannot see. That is what `n_bars_pre` / `n_bars_post` measure.

### The contiguity flag exists for a real reason

The calendar is built from whatever months are loaded. If those months are not
adjacent — `day_2024-02` and `day_2026-07` currently both sit under `probe/` —
consecutive calendar indices straddle the gap, and offset −1 from 2026-07-01
would resolve to 2024-02-13. Every panel row carries `span_days`, the true
calendar distance from the ex-date, and `window_contiguous` flags any event
whose window exceeds 30 calendar days.

**Running with no month arguments will load the 2024-02 island** alongside
2026. The flag catches it; the flag is not a substitute for passing the months
you mean.

### Duplicate ex-dates are summed, not dropped

Multiple distributions can share a `(ticker, ex_date)` — a regular plus a
special, or a corrected record. 165,888 raw rows collapse to 164,728 events.
A duplicate left in place fans out in the join and double-counts the event.
The price drop on a given date reflects *all* distributions that day, so
`cash_amount` is summed and `n_distributions` preserves the fact.

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
the ratio. Splits are not pulled yet; `drop_ratio_extreme` is a heuristic
stand-in until they are. **This is the first thing M2 should fix.**

---

## Running it

Requires `$TEAM` set and the reference files already on HDFS — see
`ticker/README_TICKER.md` and `dividends/README_DIVIDEND.md`.

```bash
# explicit months (recommended)
spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 4g --executor-cores 2 \
    gen_dividend_events_table.py 2026-07 2026-08

# every day_<YYYY-MM> directory under probe/ -- see the contiguity warning
spark-submit ... gen_dividend_events_table.py
```

`--deploy-mode client` is **required**: in cluster mode the driver runs on an
arbitrary node with no inherited environment and `$TEAM` silently falls through
to the hardcoded default.

Pull the grain table down:

```bash
hdfs dfs -getmerge $TEAM/curated/div_event_grain_csv/*.csv ~/div_event_grain.csv
wc -l ~/div_event_grain.csv    # 4061 = 4060 events + header
mv ~/div_event_grain.csv ~/div_event_grain_2026-07_2026-08.csv
```

Rename with the window before committing. The HDFS output path does not change
between runs, so an undated copy goes stale the moment someone runs different
months.

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

M2 is "measurement to strategy." The grain table is the input. In rough
priority order:

**1. Pull splits and fix the unadjusted-price problem.** `/stocks/v1/splits`
returns execution dates and ratio factors. Any event whose window contains a
split has a meaningless `drop_ratio` right now. This is a correctness fix, not
an enhancement, and everything downstream inherits the error until it is done.

**2. Get five years of day aggregates.** ~1,250 files at ~320 KB ≈ 400 MB,
`aws s3 sync` per year-month prefix, same pattern as `README_TICKER.md` stage 0.
This takes the event count from 4,060 to something near 100,000 and is what
makes any cross-sectional result credible. Note the schema drift: 2026 files
carry **fractional** volume, so `volume` must be `double` — a `long` schema
silently nulls every row under Spark's default PERMISSIVE parsing.

**3. Decide the universe filter.** Join `sec_type` from
`$TEAM/reference/tickers_all_5y_metadata.jsonl`. ETFs slightly outnumber common
stock in the ticker universe (5,332 vs 5,303) and distribute far more often, so
they will dominate the event count. A fund distribution is not the
Elton–Gruber phenomenon — no earnings-retention decision, just pass-through of
underlying income. Proposed: primary on `CS`, `ETF` as a named comparison
group, `CS + ETF + ADRC` as the outer bound. This is a team decision, not
settled.

**4. Cross-sectional buckets.** Everything needed is already on the grain
table: `div_yield`, `pre_vol`, `pre_avg_dollar_volume`, plus `sic_description`
from the metadata join. Sector coverage is ~45% of active tickers and 0% of
delisted — SIC classifies operating companies only, so ETFs have none. 376
distinct `sic_description` values is too granular to bucket on directly; group
to SIC major group first.

**5. Transaction costs.** The current result is already negative before costs,
so this determines *how* negative rather than whether. NBBO quotes give a
measured spread instead of an assumption — there is a `quotes_v1` flat-file
dataset, and the REST Quotes endpoint for spot checks.

**6. Confounders worth controlling.** Earnings announcements inside the ±3
window mean the price move has nothing to do with the dividend; Benzinga
earnings dates would let you flag and exclude those. Short interest is the
strongest unexploited variable available — short sellers must pay the dividend
to the lender, so ex-dates interact directly with borrow demand, and high short
interest also proxies hard-to-borrow status, which constrains whether the trade
is executable at all.

### Things that will bite

- **`ex_dividend_date` is the anchor.** `record_date`, `pay_date`, and
  `declaration_date` are carried through but none of them is where the price
  moves. Under T+1 (since May 2024) record date equals ex-date; in older data
  they differ by a day. That is the settlement-cycle change, not an error.
- **Preferred tickers use a lowercase `p` separator** (`AGMpE`), not `PR`.
  Any regex written against the old assumption will miss them.
- **`window_complete` is currently false for ~35% of events**, almost entirely
  edge effects from a 27-day calendar. Do not build a filter around that number
  — it changes completely with more price history.
- **27 days in one summer is not a sample.** Any cross-sectional result from
  the current run is illustrative only.
