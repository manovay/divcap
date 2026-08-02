# divcap — Dividend Capture at Market Scale

CS-GY 6513 Big Data final project. Testing whether the ex-dividend price drop anomaly is tradeable, and under what conditions.

**Team:** Samprith Kalakata (srk9068), Nihaal Chadha (nc3297), Elvin Seudieu (es7424), Man Sharma (ms16965)

---

## Where things live

| | Path |
|---|---|
| Cluster | `https://dataproc.hpc.nyu.edu/ssh` → `nyu-dataproc-m` |
| Shared data (HDFS) | `/user/ms16965_nyu_edu/divcap` |
| Repo | `github.com/manovay/divcap` |
| Data vendor | Massive (formerly Polygon.io — Polygon docs still apply), Stocks Starter $29/mo |

Do **not** use the course JupyterHub for real work. It's `local[*]`, 4 GB, 0 GB free disk, and cannot see Dataproc HDFS. Use it only for plotting small exported CSVs.

---

## 1. What's done (Phase 1)

1. **Environment** — `boto3` + `awscli` installed to `~/.local`, credentials in `~/.aws/credentials` (S3) and `~/.bashrc_local` (REST key).
2. **Data pulled** — 5 days of day aggregates, 1 day of minute aggregates, 2,888 dividend events (Feb 7–13, 2024).
3. **Landed on HDFS** — `$TEAM/probe/`, with ACLs granting all four members `rwx` plus `default:` inheritance.
4. **Spark verified** — Spark 3.5.3 on YARN. Must launch with `--deploy-mode client`.
5. **EDA done** — schema, timezone handling, coverage, and data quality all checked (see §2).
6. **First measurement** — drop ratio computed on 263 events, written to `$TEAM/curated/probe_events` (Parquet) and `results/probe_events.csv`.
7. **Repo live** — puller script and results committed.

---

## 2. What we know about the data

| Fact | Value | Implication |
|---|---|---|
| Day agg file | ~200 KB/day | 5 yrs ≈ 250 MB. Trivial. |
| Minute agg file | 17.6 MB/day | 5 yrs ≈ **23 GB**. Fits on HDFS without filtering. |
| Tickers/day | ~10,500 | ~70 zero-volume |
| Minute bar coverage | mean 147, max 944, min 1 | **Sparse.** Use `last(..., ignorenulls=True)`, never positional offsets. |
| Dividend events/week | 2,888 | ~750K over 5 years — far more than the 100K we assumed |
| HDFS replication | 1 | No storage doubling |

**Four gotchas:**
1. `window_start` is **nanoseconds since epoch, UTC**. Always convert: `F.from_utc_timestamp(F.timestamp_seconds(col/1e9), "America/New_York")`. Verified: data spans 04:00–19:59 ET.
2. Flat file prices are **unadjusted** for splits/dividends. Correct for measuring raw drops, but splits will corrupt any ticker that split mid-window.
3. Massive has **two separate credentials** — S3 access/secret for flat files, REST API key for the API. Not interchangeable.
4. `.csv.gz` is **not splittable** → one Spark task per file regardless of size. Convert to Parquet early.

---

## 3. First result — and why not to trust it yet

Drop ratio = (close day before ex-date − open on ex-date) / dividend amount.

| n | mean | median |
|---|---|---|
| 263 | 1.299 | 1.011 |

Median ≈ 1.0 looks like the textbook no-arbitrage result, but **the sample is dominated by noise.** AAPL paid $0.24 and the stock *rose* $0.33 that day → ratio −1.375. That's ordinary volatility divided by a tiny denominator. Other extremes: WRK −5.65, COR +8.18, NYCB +4.60 (the Feb 2024 bank selloff).

**This validates the proposal's design.** Yield bucketing isn't optional — it's the only way to get signal. We also need to market-adjust returns before dividing, use medians over means, and filter split contamination (|ratio| > 20).

**Open question:** only 263 of 2,888 events matched to price data (~9%). Some is expected (Feb 7 has no prior day in a 5-day window), but most isn't — likely funds and ETFs absent from `us_stocks_sip`. **This determines our real universe size and is the first Phase 2 task.**

---

## 4. Teammate setup

You need: an NYU Dataproc account, a GitHub personal access token, and (if you're doing ingest) your own Massive keys.

```bash
# 1. tooling
pip3 install --user boto3 awscli
echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc && source ~/.bashrc

# 2. credentials — edit BY HAND. Do not paste heredocs into the browser console; it mangles them.
nano ~/.aws/credentials
#   [massive]
#   aws_access_key_id = ...
#   aws_secret_access_key = ...
chmod 600 ~/.aws/credentials

nano ~/.bashrc_local
#   export MASSIVE_API_KEY=...              <- REST key. No spaces around '='.
#   export EP=https://files.massive.com
#   export TEAM=/user/ms16965_nyu_edu/divcap
echo '[ -f ~/.bashrc_local ] && source ~/.bashrc_local' >> ~/.bashrc && source ~/.bashrc

# 3. verify
hdfs dfs -ls $TEAM/probe
aws s3 ls s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/02/ --endpoint-url $EP --profile massive
curl -s "https://api.massive.com/v3/reference/dividends?ticker=AAPL&limit=1&apiKey=$MASSIVE_API_KEY" | python3 -m json.tool

# 4. repo
cd ~ && git clone https://github.com/manovay/divcap.git
cd divcap && git config user.name "Your Name" && git config user.email "you@nyu.edu"
git config --global credential.helper store
```

GitHub asks for a **token**, not your password (account passwords were disabled in 2021). Settings → Developer settings → Personal access tokens → Tokens (classic) → tick `repo`.

---

## 5. Working agreements

1. **Code in git, data in HDFS.** Never commit `.csv.gz`, `.parquet`, `.jsonl`, or credentials. Small derived results (<1 MB) are fine.
2. **Logic lives in `.py` files.** Notebooks don't diff and will cause merge conflicts across four people. Notebooks are for exploration only.
3. **Don't hog the cluster.** Shared with the whole class. Cap at `--num-executors 4`. Exit your Spark shell when done — an idle shell holds YARN resources.
4. **One person owns bulk ingest.** The Massive plan is licensed for individual use; share derived tables, not raw vendor files.
5. **Branch per workstream**, PR into `main`.

---

## 6. Cheatsheet

```bash
# spark
pyspark --master yarn --deploy-mode client --num-executors 4 --executor-memory 4g --executor-cores 2

# download a flat file
aws s3 cp s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/02/2024-02-08.csv.gz . --endpoint-url $EP --profile massive

# authoritative trading-day calendar for a month (don't guess dates — holidays)
aws s3 ls s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/02/ --endpoint-url $EP --profile massive

# dividends (handles pagination)
python3 ingest/pull_divs.py 2024-02-07 2024-02-13 divs.jsonl

# hdfs
hdfs dfs -put -f localfile $TEAM/path/
hdfs dfs -getmerge $TEAM/curated/probe_events_csv/*.csv ~/results/out.csv
```

Canonical loader:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TEAM = "/user/ms16965_nyu_edu/divcap"
SCHEMA = ("ticker string, volume long, open double, close double, "
          "high double, low double, window_start long, transactions long")

def load(p):
    return (spark.read.option("header", True).schema(SCHEMA).csv(p)
            .withColumn("ts_et", F.from_utc_timestamp(
                F.timestamp_seconds(F.col("window_start")/1e9), "America/New_York"))
            .withColumn("date_et", F.to_date("ts_et")))
```

---

## 7. Milestones

### M1 — Full ingest
- [ ] **Resolve the 9% match rate.** Which dividend tickers have no price data, and why? Defines our universe. *(do this first — everything depends on it)*
- [ ] Bulk-pull all `day_aggs` history → Parquet partitioned by date
- [ ] Bulk-pull all dividends + splits via REST
- [ ] Bulk-pull `minute_aggs` (~23 GB, no filtering needed)
- [ ] Benchmark Parquet vs gzip-CSV read time; record for the report

### M2 — Event-grain table
- [ ] Split adjustment via the Splits endpoint
- [ ] Market-adjusted returns (subtract benchmark same-day move)
- [ ] Liquidity floor on the universe
- [ ] Dividend-event grain table: drop ratio, yield, volume, volatility, sector per event
- [ ] Outlier handling (|ratio| > 20 → flag as split/corporate action)

### M3 — Cross-sectional analysis
- [ ] Group by yield bucket, sector, size/liquidity, volatility
- [ ] Time-decay curve (how the drop evolves intraday and over following days)
- [ ] Minute-grain reconstruction of the ex-date open using the sparse-bar logic

### M4 — Model + backtest
- [ ] Spark MLlib classifier: which characteristics predict a profitable capture
- [ ] Cost-adjusted backtest (spread, commissions)

### M5 — Stretch: real-time engine
- [ ] Kafka replay of a historical trading day as an accelerated feed
- [ ] Spark Structured Streaming scoring against the ex-dividend calendar
- [ ] MongoDB serving + dashboard

### Housekeeping
- [ ] **Rotate Massive S3 keys** (exposed in a screenshot during setup)
- [ ] Consider CRSP via NYU WRDS (free) as a validation layer — cleaner distribution codes and PERMNO identifiers that survive ticker changes
