# Ticker Universe & Metadata

How we define the set of securities a dividend-capture trade could actually be
executed on, and how we attach the attributes the cross-sectional analysis
needs.

**If you just want the data: use `ticker/tickers_all_5y_metadata.jsonl`.** It is
one JSON object per line, one line per (ticker, listing status), carrying both
the bulk reference fields and the company detail. Everything else in this
document explains how it was built and what its limits are.

That file is gitignored — it is multi-MB vendor data, and the working agreement
keeps data in HDFS rather than git. To see the record shape without running the
pullers or holding credentials, read the committed sample:
**`ticker/sample_tickers_metadata.jsonl`** (15 records, section 7).

---

## The pipeline

| # | Stage | Script | Output |
|---|---|---|---|
| 1 | SIP tickers from price files | `ticker/gen_ticker_sip_list.py` (Spark) | `ticker_sip_list_<months>.csv` |
| 2 | Bulk reference pull | `ticker/gen_all_tickers.py` | `tickers_all_5y.jsonl` |
| 3 | Sanity check | `ticker/util/check_universe_coverage.py` | stdout |
| 4 | Company detail | `ticker/gen_ticker_metadata.py` | `tickers_all_5y_metadata.jsonl` |
| 5 | Validation | `ticker/util/check_ticker_metadata.py` | stdout, non-zero exit on failure |
| 6 | Committed sample | `ticker/util/make_sample.py` | `sample_tickers_metadata.jsonl` |

Stage 1 defines **membership** (who is in the universe). Stages 2 and 4 supply
**attributes** (what they are). Stage 3 confirms the two agree. Stage 6 exists
purely so the record shape is visible in the repo.

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

Day aggregates give one row per ticker per trading day. That set *is* the
tradeable universe for that date.

### Why not drive the universe off the dividends API

The REST endpoints cover Massive's full catalog — exchanges, dark pools, FINRA
facilities, **and OTC markets**. Much wider than the SIP tape, so a universe
built from the API is mostly securities we have no prices for.

Measured on the Phase 1 probe (`/v3/reference/dividends`, Feb 7–13 2024):

| | count | note |
|---|---|---|
| dividend events returned | 2,888 | ~150K/yr annualized |
| `frequency: 12` (monthly) | 2,499 | overwhelmingly funds |
| `frequency: 4` (quarterly) | 234 | ≈ what operating companies pay |
| `dividend_type: CD` / `SC` | 2,884 / 4 | specials are negligible |
| joined to price data | 263 | ~9% |

Roughly 70% of the returned tickers are symbols ending in `X` — mutual funds.
The remaining misses are OTC foreign ordinaries (`F` suffix), OTC common, and
preferreds (written with a `PR` infix).

**This is not a join bug.** The dividends endpoint and the flat files describe
different universes. Excluding the misses is also methodologically correct: a
capture trade requires buying before the close and selling at the ex-date open,
which is undefined on an instrument with no intraday price.

Consequence: the proposal's ~750K event estimate is ~10x too high. The real
tradeable event count is closer to 50–70K over five years. The big-data
constraint lives on the price side, not the event table.

---

## 2. Stage 1 — SIP ticker list

`ticker/gen_ticker_sip_list.py`, PySpark. Reads `day_<YYYY-MM>/*.csv.gz` for one
or more months and collapses to the distinct set of tickers. Output is one
ticker per line, no header, no other columns.

Price-driven membership is self-validating, correctly dated, and free of
survivorship bias — unlike a current-snapshot symbol directory, which cannot
tell you what was listed two years ago.

**Current output (2026-07 + 2026-08, 27 trading days): 13,192 tickers.**
Cross-checked against `cut -d, -f1` on the raw files — same count.

### Flat-file gotchas

- `window_start` is **nanoseconds** since epoch in UTC, despite the vendor docs
  saying seconds. Day bars are stamped at 00:00 ET, not at the open.
- `volume` is **fractional** in the 2026 files (fractional-share trading). A
  `long` schema silently nulls every row under Spark's default PERMISSIVE
  parsing. Declare it `double`. The 2024 files are integer-valued and parse
  fine either way.
- Spark maps an explicit schema **positionally** and ignores header names, so
  column order matters. Verified as
  `ticker,volume,open,close,high,low,window_start,transactions`.
- `.csv.gz` is not splittable: one file is one task regardless of size.

---

## 3. Stage 2 — bulk reference pull

`ticker/gen_all_tickers.py`. Paginates `/v3/reference/tickers?market=stocks`.
The `ticker` parameter is optional and defaults to empty, so this is ~40
requests, not one per symbol.

Runs **two phases in one invocation**. `active=true` is the current listing;
`active=false` is a *separate* query returning only delisted names, not a
superset. Both go to one file; each record carries its own `active` flag.

The delisted phase is filtered to a lookback window (default 5 years) on
`delisted_utc`. The endpoint has no date filter, so the filter runs client-side
— every page is still fetched, only the kept rows shrink. Records with no
`delisted_utc` are kept regardless, since they cannot be dated and dropping them
would silently narrow the universe.

**Current output: 20,729 rows — 13,080 active, 7,649 delisted (from 23,284
before the 5-year window).**

The delisted phase is not optional. See stage 3.

---

## 4. Stage 3 — coverage sanity check

`ticker/util/check_universe_coverage.py`. Confirms the reference pull actually
covers the price-side universe.

**Current result:**

| | count |
|---|---|
| SIP list | 13,192 |
| in both | 13,034 (98.8%) |
| SIP, not in active | 158 |
| …of which in the delisted pull | 158 |
| …with no reference record at all | 0 |
| active, not in SIP | 46 |

The 158 are securities that traded in July–August 2026 and were **already
delisted** by the time the reference table was pulled — 68 CS and 41 ETF among
them. If 158 vanish in two months, the survivorship hole over five years would
be large. This is the concrete justification for the `active=false` phase.

The 46 in the other direction are listed but did not trade in either month.

### Type mix of the matched set

| type | count | share |
|---|---|---|
| ETF | 5,332 | 40.9% |
| CS | 5,303 | 40.7% |
| WARRANT | 443 | 3.4% |
| PFD | 429 | 3.3% |
| ADRC | 373 | 2.9% |
| FUND | 333 | 2.6% |
| UNIT | 292 | 2.2% |
| SP, RIGHT, ETS, ETV, ETN | 529 | 4.1% |

**ETFs slightly outnumber common stock.** They also distribute far more
frequently (often monthly vs quarterly), so their share of *events* will be much
higher than their 41% share of tickers. A fund distribution is not the
Elton–Gruber phenomenon — no earnings-retention decision, just pass-through of
underlying income. Proposed handling: primary analysis on `CS`, `ETF` as a named
comparison group, `CS + ETF + ADRC` as the outer bound. Warrants, rights, units,
and structured products (~9%) don't pay dividends and drop out anyway.

Exchange mix is XNAS 42.5%, XNYS 22.4%, ARCX 20.7%, BATS 12.0%, XASE 2.4% —
five NMS venues and nothing else, confirming the SIP-based definition did what
it was meant to.

---

## 5. Stage 4 — company detail

`ticker/gen_ticker_metadata.py`. Joins `/v3/reference/tickers/{ticker}` (Ticker
Overview) onto every row of the bulk pull. This is **one call per symbol**, so
~20,700 requests. At 20-way concurrency it runs at ~65/s — about five minutes.

Carried through from the bulk row: `ticker`, `market`, `locale`, `type`,
`active`, `last_updated_utc`.

Added from Overview: `address`, `description`, `list_date`, `sic_description`,
`market_cap`, `total_employees`, `weighted_shares_outstanding`, plus an
`overview_error` field.

### Resumability

Resume is by **ticker set**, not row count. Requests are fired in parallel so
completion order does not match input order, and a count-based resume would
silently skip and duplicate rows. On restart the output is read back and
already-done tickers are skipped.

Failed lookups are **written**, with null Overview fields and an
`overview_error` string, so a restart does not retry them forever. To retry,
delete those lines and re-run. Non-404 errors are transient and worth retrying;
404 means the vendor genuinely has no record.

### Coverage — read this before using the file

**All 7,363 errors are 404s, and all of them are on delisted tickers. Zero
active tickers failed.** Ticker Overview covers currently-listed companies only.

| field | active (13,080) | delisted (7,649) |
|---|---|---|
| `list_date` | 98.3% | 0% |
| `weighted_shares_outstanding` | 45.4% | 0% |
| `address` | 45.3% | 0% |
| `market_cap` | 45.1% | 0% |
| `total_employees` | 44.7% | 0% |
| `sic_description` | 44.6% | 0% |
| `description` | 54.8% | 3.7% |

The ~45% on active is **not missing data**. It is almost exactly CS (5,303) plus
ADRC (373). SIC codes classify operating companies; ETFs, funds, warrants, and
structured products have no business to classify. So sector, market cap, and
employees are really "operating companies only," which is the right population
for the sector analysis anyway — but it means that analysis structurally covers
~40% of the universe.

376 distinct `sic_description` values. Too granular to bucket on directly; M3
will need a coarser mapping (SIC major group, or hand-rolled).

### Ticker is not a unique join key

60 tickers appear **twice** — once delisted under the company that used to hold
the symbol, once active under whoever holds it now. Symbol reuse is real and
grows with the window length.

Any join of dividend events to reference data on ticker alone will attach the
wrong company for these. The join needs `ticker` **plus a date bound**
(`list_date` / `delisted_utc`), or a deliberate one-record-per-ticker filter.

---

## 6. Stage 5 — validation

`ticker/util/check_ticker_metadata.py`. Exits non-zero on structural failure, so
it drops into a Makefile or CI.

**Hard failures:** input ticker absent from output, true duplicates (same ticker,
same active status), extras, unparseable lines, null base fields, non-uniform key
sets, non-positive `market_cap`, negative `total_employees`, any non-404 error.

**Reported, not failures:** per-field coverage split by phase, symbol reuse,
distinct SIC count. The near-total nulls on the delisted side are expected.

---

## 7. Stage 6 — committed sample

`ticker/util/make_sample.py` writes `ticker/sample_tickers_metadata.jsonl`: 15
records committed to the repo so the schema is inspectable without credentials.

Selection is **deliberate, not random**. A random slice would be mostly ETFs and
delisted 404s, which would misrepresent what a populated record looks like. The
sample covers every shape a consumer will hit:

| n | bucket | what it shows |
|---|---|---|
| 6 | active `CS` with `sic_description` | the fully-populated case |
| 2 | active `ETF` | null sector, market cap, employees |
| 2 | active `PFD` / `WARRANT` | other instrument types |
| 5 | delisted | the 404 case — every Overview field null |

Deterministic: rows are sorted by ticker and the first N of each bucket are
taken, so re-running against the same input reproduces the same sample.

### The gitignore exception

`.gitignore` excludes `*.jsonl`, so the sample needs an explicit negation:

```
*.jsonl
...
!sample_*.jsonl
```

**Ordering is load-bearing** — gitignore applies rules in order and the last
match wins, so the negation must come after the pattern it overrides. (A
negation also cannot rescue a file inside an ignored *directory*; only file
patterns are ignored here, so that case does not arise.)

Verify before committing — this should print nothing:

```bash
git check-ignore -v ticker/sample_tickers_metadata.jsonl
```

---

## 8. Reproducing from scratch

### Prerequisites

**Cluster** (`nyu-dataproc-m`) — for stage 1 only:

```bash
# HDFS path shortcut
nano ~/.bashrc_local          # export TEAM=/user/ms16965_nyu_edu/divcap
echo '[ -f ~/.bashrc_local ] && source ~/.bashrc_local' >> ~/.bashrc
source ~/.bashrc && echo $TEAM
```

**S3 credentials** — only if you need to pull new flat files:

```bash
pip3 install --user awscli
echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc && source ~/.bashrc

mkdir -p ~/.aws && nano ~/.aws/credentials
#   [massive]
#   aws_access_key_id = ...
#   aws_secret_access_key = ...
chmod 600 ~/.aws/credentials

# also add to ~/.bashrc_local:
#   export EP=https://files.massive.com
aws s3 ls s3://flatfiles/us_stocks_sip/day_aggs_v1/2026/08/ \
    --endpoint-url $EP --profile massive
```

**Local** — for stages 2–5:

```bash
nano ~/.zshrc_local           # export MASSIVE_API_KEY=...
echo '[ -f ~/.zshrc_local ] && source ~/.zshrc_local' >> ~/.zshrc
source ~/.zshrc && chmod 600 ~/.zshrc_local
```

Three separate credentials. The S3 keys and the REST key are **not**
interchangeable.

### Stage 0 — land flat files (skip if already on HDFS)

```bash
cd ~ && mkdir -p pull/2026-08
aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1/2026/08/ pull/2026-08/ \
    --endpoint-url $EP --profile massive

# S3 objects are bare dates; rename to match the day_ convention
cd ~/pull/2026-08 && for f in *.csv.gz; do mv "$f" "day_$f"; done

hdfs dfs -mkdir -p $TEAM/probe/day_2026-08
hdfs dfs -put ~/pull/2026-08/*.csv.gz $TEAM/probe/day_2026-08/
```

### Stage 1 — SIP ticker list (on the cluster)

```bash
spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 4g --executor-cores 2 \
    gen_ticker_sip_list.py 2026-07 2026-08

hdfs dfs -getmerge $TEAM/curated/ticker_sip_list/*.csv ~/ticker_sip_list.csv
wc -l ~/ticker_sip_list.csv    # expect 13192
mv ~/ticker_sip_list.csv ~/ticker_sip_list_2026-07_2026-08.csv
```

`--deploy-mode client` is **required** — in cluster mode the driver runs on an
arbitrary node with no inherited environment and `$TEAM` silently falls through
to the hardcoded default.

Download via the browser SSH gear icon → Download file →
`/home/<netid>/ticker_sip_list_2026-07_2026-08.csv`, and commit to `ticker/`
**with the window in the filename**. The HDFS output path does not change
between runs, so an undated copy goes stale the moment someone runs different
months.

### Stages 2–5 (local)

```bash
cd ticker
python3 gen_all_tickers.py 5                              # ~1 min
python3 util/check_universe_coverage.py
python3 gen_ticker_metadata.py tickers_all_5y.jsonl       # ~5 min
python3 util/check_ticker_metadata.py
python3 util/make_sample.py                               # refresh committed sample
```

`gen_ticker_metadata.py` takes an optional second argument capping how many
*new* tickers to fetch, useful for a smoke test:

```bash
python3 gen_ticker_metadata.py tickers_all_5y.jsonl 100
```

Push the result to HDFS for the Spark jobs:

```bash
hdfs dfs -mkdir -p $TEAM/reference
hdfs dfs -put tickers_all_5y_metadata.jsonl $TEAM/reference/
```

### Reading it in Spark

Always point at the **directory**, never a `part-*` file — part filenames carry
a per-run UUID and change every time.

```python
meta = spark.read.json(f"{TEAM}/reference/tickers_all_5y_metadata.jsonl")
sip  = spark.read.csv(f"{TEAM}/curated/ticker_sip_list")
```

Use a **left** join with an explicit unknown bucket, not an inner join — many
older delisted records have no `type` at all, and an inner join drops them
silently.

### Gotcha: shuffle partitions

`spark.sql.shuffle.partitions` is **1000** on this cluster. Any global `orderBy`
before a write produces 1,000 part files regardless of row count — an early run
wrote 1,000 Parquet fragments of ~1.8 KB each, together larger than the raw CSV,
because per-file footers swamped ten rows of data. Thousands of tiny files also
bloat NameNode memory on a cluster shared with the whole class.

Use `coalesce(1).sortWithinPartitions(...)` instead: one partition first, then
sort inside it. No shuffle, one file. `coalesce(1)` is fine at 13K rows and will
**not** scale — at full history, drop the sort and let partition count follow
data size, targeting ~128 MB per file.

---

## 9. Open

- **Sector granularity.** 376 distinct `sic_description` strings need mapping to
  a coarser grouping before M3. Check whether Overview also returns a numeric
  `sic_code` — prefix-grouping a code beats string-matching descriptions.
- **Point-in-time attributes.** `market_cap` and `weighted_shares_outstanding`
  are *current* snapshots, wrong for events years in the past. Dollar volume
  computed from our own price data is correctly dated and is the better size
  proxy.
- **Sector for delisted names.** Unavailable from Overview. Either an unknown
  bucket (~1% of the current universe, growing with window length) or CRSP via
  WRDS, which carries point-in-time SIC codes.
- **Per-date membership.** The SIP list is currently a single flat set over the
  loaded months. At full history it has to become per-date; listings and
  delistings make any global list wrong over five years.
- **Deprecated endpoints.** `ingest/pull_divs.py` uses `/v3/reference/dividends`,
  which the docs list as deprecated. The current Dividends endpoint also returns
  adjustment factors for normalizing historical prices — exactly what M2 needs
  for the unadjusted-price problem. Switch before the five-year pull.
