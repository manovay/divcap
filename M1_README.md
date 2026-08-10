# Milestone 1 — Data Layer

Everything M2 needs is built and on HDFS. This is the index: what exists, where
it lives, what the gotchas are, and where to go next.

Detail lives in the per-stage READMEs:

| | |
|---|---|
| `ticker/README_TICKER.md` | universe definition, reference pulls, coverage checks |
| `dividends/README_DIVIDEND.md` | the dividend pull, endpoint migration |
| `m1_dividend_events/README_DIVIDEND_EVENTS.md` | event tables, metric definitions, M2 starting notes |

---

## The three datasets

```
$TEAM = /user/ms16965_nyu_edu/divcap
```

### 1. Ticker metadata

| | |
|---|---|
| **HDFS** | `$TEAM/reference/tickers_all_5y_metadata.jsonl` — 11.1 MB, 20,729 rows |
| **Sample in git** | `ticker/sample_tickers_metadata.jsonl` — 15 records |
| Membership list | `$TEAM/curated/ticker_sip_list` — 13,192 tickers |

One JSON object per line, one line per (ticker, listing status). Covers
**every NMS-listed security active today, plus everything delisted within the
last 5 years**: 13,080 active + 7,649 delisted.

The delisted half is not padding. 158 tickers that traded in July–August 2026
were *already delisted* by the time we pulled reference data — 68 of them
common stock. Over five years that gap is real survivorship bias.

**Attributes**

| field | source | coverage |
|---|---|---|
| `ticker`, `market`, `locale`, `active`, `last_updated_utc` | bulk `/v3/reference/tickers` | 100% |
| `type` (`CS`/`ETF`/`PFD`/`WARRANT`/`ADRC`/…) | bulk | 100% active, ~71% delisted |
| `primary_exchange` | bulk | 100% active |
| `delisted_utc` | bulk | delisted only |
| `list_date` | Ticker Overview | 98.3% active, **0% delisted** |
| `description` | Overview | 54.8% active, 3.7% delisted |
| `address` (nested) | Overview | 45.3% active, **0% delisted** |
| `sic_description` | Overview | 44.6% active, **0% delisted** |
| `market_cap` | Overview | 45.1% active, **0% delisted** |
| `total_employees` | Overview | 44.7% active, **0% delisted** |
| `weighted_shares_outstanding` | Overview | 45.4% active, **0% delisted** |

**Nuances you will hit:**

- **Delisted tickers have no Overview data at all.** All 7,363 lookup failures
  were 404s and every one was a delisted name; zero active tickers failed.
  Ticker Overview covers currently-listed companies only. Any event on a
  since-delisted company has null sector, market cap, address, employees.
- **The ~45% on active is not missing data.** It is almost exactly CS (5,303)
  plus ADRC (373). SIC codes classify *operating companies*; ETFs, funds,
  warrants, and structured products have no business to classify. So sector
  and fundamentals structurally cover ~40% of the universe.
- **Ticker alone is not a unique key.** 60 symbols appear twice — once
  delisted under the company that used to hold them, once active under
  whoever holds them now. Join on `ticker` **plus a date bound**
  (`list_date` / `delisted_utc`). The event-table job already does this.
- **Use a LEFT join with an explicit unknown bucket.** ~29% of delisted
  records have no `type` field at all; an inner join drops them silently.
- **`market_cap` is a current snapshot**, wrong for any historical event. Use
  `pre_avg_dollar_volume` from the event table as the size proxy instead.
- **ETFs slightly outnumber common stock** (5,332 vs 5,303 in the traded
  universe). They also distribute far more often, so they will dominate the
  event count.
- **Preferred tickers use a lowercase `p` separator** (`AGMpE`), not `PR`.

### 2. Dividend events

| | |
|---|---|
| **HDFS** | `$TEAM/reference/dividends_5y.jsonl` — 61.4 MB, 165,888 rows |
| **Sample in git** | `dividends/sample_dividends.jsonl` — 16 records |

One JSON object per line, one line per dividend record, **2021-08 through
2026-08**. Pulled market-wide, then filtered against the ticker metadata as
rows arrived — keep rate ran 58–78% per month, the rest being mutual funds and
OTC names with no exchange tape that could never join to a price.

**Attributes:** `ticker`, `ex_dividend_date`, `record_date`, `pay_date`,
`declaration_date`, `cash_amount`, `split_adjusted_cash_amount`,
`historical_adjustment_factor`, `frequency`, `distribution_type`, `currency`,
`id`.

**Nuances:**

- **We are on `/stocks/v1/dividends`, not `/v3/reference/dividends`.** The
  latter is deprecated. Field names differ: `distribution_type`
  (`recurring`/`special`/`supplemental`/`irregular`/`unknown`) replaced
  `dividend_type` (`CD`/`SC`). Any code filtering on `"CD"` is stale.
- **Use raw `cash_amount`, never `split_adjusted_cash_amount`.** Flat-file
  prices are unadjusted, so the dividend must be in the same share basis.
- **`ex_dividend_date` is the only date that matters** for the strategy. Under
  T+1 (since May 2024) `record_date` equals it; in older data they differ by a
  day. That is the settlement-cycle change, not an error.
- **1,160 duplicate `(ticker, ex_date)` pairs exist** — a regular plus a
  special, or a corrected record. Left in place they fan out in a join and
  double-count. The event job sums `cash_amount` and keeps `n_distributions`.
- **`frequency` is richer than expected:** 0 (non-recurring), 1, 2, 3, 4, 12,
  24, 52, 104, 365. Weekly-distributing ETFs exist.
- Monthly counts swing ~4x with the quarterly calendar — Mar/Jun/Sep/Dec spike.

### 3. Dividend-event grain table

| | |
|---|---|
| **HDFS — grain** | `$TEAM/curated/div_event_grain` — 4,060 events |
| HDFS — grain CSV | `$TEAM/curated/div_event_grain_csv` |
| **HDFS — panel** | `$TEAM/curated/div_event_panel` — 36,540 rows, partitioned by `ex_ym` |
| Committed copy | `m1_dividend_events/results/div_event_grain_2026-07_2026-08.csv` |

**This currently covers 2026-07 and 2026-08 only — 27 trading days.** 4,060 of
164,728 events have an ex-date inside the loaded calendar. It is a proof of
concept, not the deliverable.

**Getting to the full 5 years is job #1.** It needs day aggregates for the
whole window: ~1,250 files at ~320 KB ≈ 400 MB, `aws s3 sync` per year-month
prefix (see `README_TICKER.md` stage 0). That takes the event count from 4,060
to roughly 100,000 and is what makes any cross-sectional result credible. The
event job already handles multiple months — pass them as arguments.

Two grains: the **panel** is one row per (event × trading-day offset −5…+3),
the raw evidence and what a time-decay curve reads. The **grain** table
collapses it to one row per event with metrics computed.

Read the directory, never a `part-*` file:

```python
grain = spark.read.parquet(f"{TEAM}/curated/div_event_grain")
panel = spark.read.parquet(f"{TEAM}/curated/div_event_panel")
```

---

## What we measure

The strategy: **buy at the close on ex−1, sell at the open on the ex-date,
collect the dividend.** Theory says the price falls by exactly the dividend,
making it a wash. The literature says it falls by less. The question is whether
the gap is tradeable.

| column | formula | reads as |
|---|---|---|
| `drop_ratio` | `(prev_close − ex_open) / cash_amount` | 1.0 = full drop; below 1 = the anomaly |
| `drop_pct` | `(prev_close − ex_open) / prev_close` | the drop as a return |
| `div_yield` | `cash_amount / prev_close` | dividend size relative to price |
| `capture_ret` | `(ex_open − prev_close + cash_amount) / prev_close` | **gross return on the trade** |
| `capture_ret_abn` | `capture_ret − mkt_overnight_ret` | same, minus SPY's overnight move |
| `hold_ret` | sell at +3 instead of the open | does waiting help |
| `pre_avg_ret`, `pre_avg_abn_ret` | mean daily return, ex−4…ex−1 | drift going in |
| `pre_vol` | stdev of those | volatility bucket |
| `pre_avg_dollar_volume` | mean close×volume, ex−5…ex−1 | liquidity/size, correctly dated |
| `post_avg_ret`, `post_avg_abn_ret` | mean daily return, ex+1…ex+3 | does it revert |

Flags, not filters: `has_core`, `window_complete`, `window_contiguous`,
`drop_ratio_extreme`, `low_yield`. The table records facts; the analysis
chooses what to exclude.

**Prefer `capture_ret` over `drop_ratio` as the primary metric.** `drop_ratio`
divides by the dividend, so a small dividend turns ordinary volatility into an
enormous number — MU paid $0.15, moved $31.44 overnight, and posted a
`drop_ratio` of −209.6. That is measuring volatility, not dividend behaviour.
Its `capture_ret` is a sane +3.2%. Keep `drop_ratio` for the academic framing;
do not average or model on it.

### Results so far (n = 3,305 with both core prices)

| | value |
|---|---|
| `drop_ratio` median | **0.801** |
| p25 / p75 | 0.317 / 1.227 |
| mean | 0.664 |
| mean `capture_ret` | **+13.8 bps** |
| mean `capture_ret_abn` | **−6.4 bps** |

**The anomaly reproduces.** Price falls ~80% of the dividend, not 100% — the
Elton–Gruber result, and far tighter than the Phase 1 probe (median 1.011, mean
1.299).

**And it still is not tradeable.** Gross capture is positive, but the entire
margin is market drift: SPY rose overnight on average across these 27 days.
Market-adjusted, the strategy is **negative before any transaction costs.**

Caveat that must not get dropped when these numbers move into slides: 27
trading days in one summer is not a sample. The direction matches the
literature; the magnitude could be a July artifact. Treat this as evidence the
pipeline computes the right things, not as the answer.

---

## Milestone 2

Two things gate everything: **five years of day aggregates**, and **splits**.
Every event whose window contains a split has a meaningless drop ratio right
now — that is a correctness bug, not an enhancement. `/stocks/v1/splits`
returns execution dates and ratio factors.

### 1. Cross-sectional groupBy — the core deliverable

What the proposal promised and what a grader will look for. Yield buckets,
sector, size, liquidity, volatility, and the time-decay curve. Every input is
already on the grain table. Worth two people.

At 4,060 events you *cannot* do this — slice by yield × sector × size and cells
hit single digits. At ~100K you can. Hence the sequencing.

**The time-decay curve is the most interesting piece and the least obvious.**
For each offset −5…+3, plot mean abnormal return. If the drop is a real
anomaly you would see flat pre-drift, a sharp negative jump at offset 0, and
partial reversion after. If instead there is drift *before* the ex-date, the
market is pricing it in early and the "anomaly" is partly a measurement
artifact. Either outcome is a result. The panel table exists to support exactly
this.

Also settle the universe filter here: primary on `CS`, `ETF` as a named
comparison group, `CS + ETF + ADRC` as the outer bound. A fund distribution is
not the Elton–Gruber phenomenon — no earnings-retention decision, just
pass-through of underlying income. **This is a team decision, not settled.**

### 2. ML on `description` to fill the sector gap

The reframe that makes this worth doing: **`sic_description` is missing for 55%
of active tickers**, so sector analysis structurally covers ~40% of the
universe. That is a documented hole, not a nice-to-have.

Embed the `description` text, cluster it, and you have a *pseudo-sector* for
names with no SIC — including ETFs, where the description states what the fund
holds. Spark MLlib has the tooling: `Tokenizer` → `HashingTF`/`Word2Vec` →
`KMeans`.

Validate it by checking that clusters agree with `sic_description` where both
exist. If they do, the clusters are trustworthy where SIC is absent. If they
do not, you have learned the descriptions are not sector-informative and can
say so.

Then the real question: **does the pseudo-sector predict anything?** Feed it
into the cross-sectional groupBy alongside SIC and see whether it separates
drop ratios. If it does not, that is reportable too.

Note 376 distinct `sic_description` values is too granular to bucket on
directly — group to SIC major group first, and size the cluster count similarly.

### 3. The predictive model

Cross-sectional analysis says which *kinds* of events have favourable ratios on
average. The model turns that into a rule: given an upcoming ex-date and what
is knowable the day before, take this trade or skip it?

**The framing that matters: you are standing at the close on ex−1 with a
decision to make. Everything the model sees must be knowable at that moment.**

**Label.** `capture_ret_abn` is the outcome. Build both formulations:

- *Regression* — predict `capture_ret_abn` directly. More informative, and it
  lets you size positions rather than just accept/reject.
  `RandomForestRegressor` or `GBTRegressor`.
- *Classification* — predict `capture_ret_abn > threshold`. Cleaner as a
  decision rule. The threshold is the interesting choice: `> 0` is naive
  because it ignores costs. Better is `> estimated_round_trip_cost`, which
  ties the model to the cost work. A stated placeholder of 10–20 bps is fine
  until spreads are measured.

Base rate of "profitable" will be well under 50% — mean `capture_ret_abn` is
already −6.4 bps. That is the point. The model's job is finding the subset
where the sign flips.

**Feature leakage will kill this if you let it.** It is the most common way a
project like this produces a fake result.

*Legitimate* (known at the ex−1 close): `div_yield`, `pre_avg_ret`,
`pre_avg_abn_ret`, `pre_vol`, `pre_avg_dollar_volume`, `frequency`,
`n_distributions`, `distribution_types`, `sec_type`, `primary_exchange`,
`sic_description` (or the text cluster), days from `declaration_date` to
`ex_date`, calendar features (month, quarter-end flag).

*Leaking*: `ex_open`, `ex_close`, `post_close`, `drop_ratio`, `drop_pct`,
`capture_ret`, `post_avg_ret` — all derived from or after the outcome. And
subtly: **`market_cap` from Overview**, because it is a *current* snapshot, so
for a 2021 event it encodes what the company later became. Use
`pre_avg_dollar_volume` instead.

Make the feature list an **explicit whitelist in code**, not a blacklist.
Blacklists fail silently when someone adds a column.

**Train/test split must be temporal.** `randomSplit` is wrong — it puts 2025
events in training and 2022 in test, so the model learns from the future. Train
on 2021–2024, test on 2025–2026. This also breaks naive `CrossValidator` use,
since k-fold shuffles across time; if you want tuning, do walk-forward (train
year 1 → validate year 2; train 1–2 → validate 3; …).

**Accuracy is the wrong metric.** If 40% of events are profitable, a model
predicting "never trade" is 60% accurate and earns nothing. Report:

| metric | question |
|---|---|
| mean `capture_ret_abn` of predicted-positive events | does selection beat no selection |
| same across *all* test events | the "always trade" baseline |
| count selected | tradeable, or three events a year |
| hit rate among selected | how often you are right |
| AUC | ranking quality, secondary |

The comparison that matters is **selected vs. always-trade**. 800 events
averaging +15 bps against a full-set −6 bps is a genuine result. 12 events is
overfitting.

**Mechanics.** `StringIndexer(handleInvalid="keep")` for categoricals —
important, because the test set will contain sector values absent from training
and the default throws. Then `OneHotEncoder` → `VectorAssembler` → model, all
in a `Pipeline`. Start with `LogisticRegression` as the baseline: interpretable,
readable coefficients, and if a gradient-boosted tree cannot beat it, that is
itself informative.

**The deliverable is feature importance, not the model.** Nobody deploys this.
What makes it a result is the ranking: which characteristics actually predict a
profitable capture. If `div_yield` dominates and the rest is noise, the anomaly
is a yield phenomenon. If `pre_vol` matters more, the story is about noise
traders.

And if nothing beats the baseline, **that is a real and reportable result** —
consistent with the market being efficient after costs, which the current −6.4
bps already hints at. A null result is a finding. Manufacturing a positive one
by leaking features or shuffling the split is the only actual failure available.

### 4. Everything else

- **Transaction costs.** The result is already negative before costs, so this
  determines *how* negative. NBBO quotes give a measured spread instead of an
  assumption — `quotes_v1` flat files, or the REST Quotes endpoint for spot
  checks.
- **Confounders.** Earnings announcements inside the ±3 window mean the price
  move has nothing to do with the dividend; Benzinga earnings dates let you
  flag and exclude. **Short interest is the strongest unexploited variable
  available** — short sellers must pay the dividend to the lender, so ex-dates
  interact directly with borrow demand, and high short interest also proxies
  hard-to-borrow status, which constrains whether the trade is executable.
- **MongoDB** belongs in the streaming stretch goal as the serving layer for
  per-event analytics and live signals, which is a real use with a real
  justification. Geographic clustering on `address` is not: it is the company's
  *current* HQ, populated for 45% of active and 0% of delisted names, with no
  mechanism linking HQ location to ex-dividend price behaviour. Any cluster
  found will be "financial firms are in New York."
