# Milestone 3 — Real-Time Signal Engine

M1/M2 measure the dividend-capture anomaly across 163,358 historical events.
M3 turns that measurement into a **decision system**: a model trained offline
on 4.4 years of intraday behaviour, deployed against a live-arriving stream of
minute bars, emitting BUY/SKIP signals at the moment the trade would actually
be placed.

Kafka → Spark Structured Streaming → Parquet, with Spark MLlib for the model.
**Built, run, and verified end to end.** Results in §5.

---

## 1. The decision being modelled

The trade: **buy at the close on ex−1, sell at the open on the ex-date, collect
the dividend.**

Theory says the price falls by exactly the dividend, making this a wash. M1
measured a median `drop_ratio` of 0.870 — the price falls ~87% of the dividend,
so ~13% is retained. M2 showed the retained fraction is roughly constant, so
the *return* scales with dividend size: median `capture_ret_abn` runs from
−0.4 bps below 0.25% yield to +22.6 bps above 2%.

That is a measurement, not a strategy. The strategy question is: **standing at
15:45 ET on ex−1, looking at today's trading, should I take this trade?**

Everything the model sees must be knowable at 15:45 ET on ex−1. That single
constraint drives every design decision below.

---

## 2. Model selection: why not a sequence model

The instinct with minute data is to feed the raw sequence to an LSTM or 1D-CNN
and let it learn volatility, drift and volume as latent features. We considered
this and rejected it for four independent reasons, any one disqualifying.

**Spark MLlib has no sequence models.** It offers linear/logistic regression,
decision trees, random forests, GBTs, naive Bayes, a fixed-width
`MultilayerPerceptronClassifier`, clustering, ALS, FP-growth. No LSTM, no CNN,
no attention. Every available model is *tabular*: fixed-width input, no notion
of column order or adjacency. Shuffle the columns and a GBT trains identically
— which is exactly why it cannot learn "volatility" from 960 minute columns,
since volatility is a property of sequence.

**Variable-length input must be collapsed regardless.** AAPL trades 645 minute
bars in a session; a thin REIT trades 5. Any tabular model needs fixed width,
so aggregation is not optional — only whether it is chosen deliberately or
naively. Both naive options fail: 960 columns (one per minute) gives no
adjacency prior and is mostly null; letting a deep model learn the collapse
needs a library we do not have.

**The data does not support representation learning.** Deep sequence models
need examples in rough proportion to input dimensionality. Raw minute input is
~4,800 values per event against ~70,000 training events. Compare ImageNet:
150K pixels, 14M examples. And the signal-to-noise ratio is brutal — a ~10 bps
effect against 100–200 bps of daily volatility. A flexible model in that regime
finds spurious structure with near-certainty.

**Streaming inference forces fixed-width features.** A windowed aggregation is
native to Structured Streaming: `groupBy` a time window, keep a few doubles of
state per key, let the watermark evict it. Buffering a variable-length ordered
per-ticker sequence needs `flatMapGroupsWithState`, hand-written eviction,
hand-written event-time ordering, state that grows with the buffer and is
checkpointed to HDFS every trigger, plus a Pandas UDF shipping model weights to
every executor. That is a semester project, not a milestone.

The last point is the honest headline: **the deployment target constrains the
model class** — the same reason most production trading systems score
engineered features rather than raw ticks.

### What we chose: Gradient-Boosted Trees

`GBTClassifier`, `maxIter=60`, `maxDepth=5`, on 61 features.

| reason | detail |
|---|---|
| Mixed types | no scaling across yield, volume ratios, categoricals |
| Interactions | "high yield **and** low volatility" without being told |
| Outlier-robust | matters given `drop_ratio`'s long tail |
| **Feature importances** | the actual deliverable |
| MLlib-native | `PipelineModel.transform()` works on a streaming DataFrame |

`LogisticRegression` trains alongside as a baseline. **It matched the GBT** —
see §5, where that turns out to be the most interesting result.

**We did not hand-pick five aggregates.** The session is cut into 13 bins and
the model gets all of them, so it discovers *when* in the day matters rather
than us asserting it.

---

## 3. Minute bars → learnable features

### The decision moment

**15:45 ET on ex−1** — fifteen minutes before the bell, enough to place an
order. Every feature uses bars from 09:30 to 15:45 only. Pre-market bars (the
files carry 04:00–20:00) are dropped: thin, and they would distort every
share-based feature.

### Binning

375 minutes = **twelve 30-minute bins plus a 15-minute stub = 13 bins**. Per
bin, three scale-free values:

| feature | definition | captures |
|---|---|---|
| `bin{k}_ret` | `b_close / b_open − 1` | intraday drift shape |
| `bin{k}_volshare` | bin volume ÷ session volume through 15:45 | *when* trading happens |
| `bin{k}_range` | `(b_high − b_low) / b_close` | intraday volatility shape |

39 columns, all scale-free by construction — raw volume is not comparable
between a $4T company and a $200M REIT, and a model given raw volume learns
"big company" and nothing else.

**Bin returns are bin-internal, not chained.** A chained return
(`close_k / close_{k−1}`) nulls the whole downstream chain when a thin ticker
misses one bin, and gaps are routine. Internal returns degrade to one null.

**`volshare` uses the session total through 15:45**, not a running total. At
the decision moment the session so far is fully known, so this is not
lookahead — and it makes batch and streaming definitions trivially identical.

### Session and context features (11)

`sess_ret`, `sess_realized_vol` (stdev of the 13 bin returns), `sess_range`,
`sess_volume`, `sess_trades`, `sess_avg_trade_size`, `sess_bars`,
`n_bins_present`, plus:

| feature | definition | note |
|---|---|---|
| `gap_ret` | today's open vs. yesterday's close | needs prior-day history |
| `sess_vol_ratio` | session volume ÷ trailing 20-day average | needs prior-day history |
| `mkt_sess_ret` | SPY's own 09:30→15:45 return | live-computable; SPY is on the wire |

The two history-dependent features come from a precomputed lookup in the
streaming job — a stream has no past.

### Event features (9, from M1's grain table)

`div_yield_1545`, `cash_amount`, `frequency`, `n_distributions`, `pre_vol`,
`pre_avg_ret`, `pre_avg_abn_ret`, `pre_avg_dollar_volume`, `days_decl_to_ex`.

Plus two categoricals: `sec_type`, `sic_description`.

**61 features total.**

### Two leaks we found and fixed

**`div_yield` had a 15-minute lookahead.** The grain table computes
`cash_amount / prev_close`, and `prev_close` is the ex−1 *close* — which has
not happened at 15:45. Replaced with `div_yield_1545 = cash_amount /
sess_close`, using the 15:45 price. Small, but real. `div_yield` is on the
banned list so the check enforces it.

**`primary_exchange` is not in the metadata file.** `gen_ticker_metadata.py`'s
`KEEP` list dropped both `primary_exchange` and `delisted_utc`; they survive
only in the raw `tickers_all_5y.jsonl`. The second categorical is
`sic_description` instead, and the metadata join prefers the active record
rather than resolving by delisting date. (`README_TICKER.md` overstates what
that file contains — worth correcting there.)

### Leakage discipline

An explicit **whitelist** in code, never a blacklist — blacklists fail silently
the moment someone adds a column. The job exits non-zero if a banned column
reaches the feature vector.

Banned: `ex_open`, `ex_close`, `post_close`, `prev_close`, `drop_ratio`,
`drop_pct`, `capture_ret`, `capture_ret_abn`, `hold_ret`, `post_avg_ret`,
`post_avg_abn_ret`, `mkt_overnight_ret`, `div_yield`, **`market_cap`**.

`market_cap` is the subtle one: it is a *current* snapshot, so for a 2022 event
it encodes what the company later became. `pre_avg_dollar_volume` is the
correctly-dated size proxy.

Labels may use the future; features may not.

### The skew guard

`gen_intraday_features.py` exports `build_features(bars)`. **Both the batch
trainer and the streaming scorer import the same module.** If they computed a
feature differently the model would receive a vector from a distribution it was
never fit on, predictions would degrade, and *nothing would error*.

The streaming path reimplements level two in Python (see §6) because a
streaming query cannot chain two stateful aggregations. Those two
implementations are the highest-risk surface in the project and were verified
by hand against a worked example.

---

## 4. Train / test design

| | window | events |
|---|---|---|
| **Train** | 2021-08 … 2025-12 | **70,282** |
| **Test / replay** | 2026-01 … 2026-08 | **12,935** |

83,217 events survive the filters and have a decision-day feature row. Split is
84/16 — the natural consequence of 4.4 years against 8 months.

**Temporal split, not `randomSplit`.** A random split puts 2025 events in
training and 2022 in test — the model learns from the future. The 2026 replay
is therefore a genuine out-of-sample test, not theatre.

(Contrast `sector_ml`, where `randomSplit` *is* correct: sector is a static
company attribute with no time dimension.)

**Label:** `capture_ret_abn > 0.0010` — 10 bps, a stated round-trip cost
placeholder. Realized balance: **34,287 negative / 35,995 positive**, i.e.
48.8% / 51.2%. That threshold lands almost exactly on the median of the
population, so the classifier faces a genuinely hard problem and accuracy is
interpretable for once: 50% is the no-information baseline.

**Filters:** `has_core`, `window_contiguous`, `ticker != 'SPY'` (circular — SPY
is the market proxy, so its own `capture_ret_abn` reduces to its dividend
yield), `div_yield >= 0.005`.

**Evaluation is P&L, not accuracy.** If half the events are profitable, a model
predicting "never trade" is 50% accurate and earns nothing. The number that
matters is the mean realized return of the selected events against the
"always trade" baseline.

---

## 5. Results

### Out-of-sample, 12,935 events in 2026

| cut | n | mean bps | med bps | win% | AUC |
|---|---|---|---|---|---|
| **GBT selected** | 6,361 | **12.35** | 7.37 | 54.3 | 0.548 |
| LogReg selected | 5,974 | 13.32 | 7.37 | 54.0 | 0.544 |
| ALWAYS TRADE (baseline) | 12,935 | 7.05 | 2.99 | 51.8 | — |

**The model beats the baseline.** Selecting 49% of events nearly doubles the
mean (12.35 vs 7.05) and more than doubles the median (7.37 vs 2.99). AUC 0.548
is modest but not noise at this sample size, on a proper temporal split.

**It does not clear costs.** The label threshold was 10 bps. Selected mean is
12.35 bps *gross* — net ~2.35 bps. The selected *median* is 7.37 bps, **below**
the cost assumption, so the median selected trade loses money and the positive
mean rests on a right tail. Honest conclusion: the effect is real and the model
adds real discrimination, but not enough to trade after realistic costs.

**LogReg matches — and slightly beats — the GBT.** This is the most interesting
result. The 39 intraday bin features add nothing a linear model cannot extract,
which is consistent with M2: the effect is close to `div_yield × 0.13`, a
nearly mechanical relationship. **The anomaly has no exploitable intraday
microstructure beyond what a linear combination already captures.** That is a
stronger finding than a marginal AUC bump would have been.

### Feature importance (GBT, top 10 of 61)

```
0.1635  mkt_sess_ret          <- market-level, constant within a day
0.0674  days_decl_to_ex
0.0492  pre_avg_abn_ret
0.0420  pre_vol
0.0337  pre_avg_ret
0.0279  sess_range
0.0268  frequency
0.0260  div_yield_1545
0.0230  gap_ret
0.0197  cash_amount
```

**Caveat that must not be dropped:** `mkt_sess_ret` dominates at 2.4× the next
feature, and it is **constant within a trading day**. So the model is doing
substantially more *day-timing* ("when the market traded like this today,
capture works tomorrow morning") than *stock-selection*. That may be a real
effect — overnight returns do correlate with the prior session — but it changes
what the result claims. Disentangling the two is future work; the diagnostic is
to check whether BUY rate per date is bimodal (a day-picker) or centred near
0.49 (a stock-picker).

Note also that `div_yield_1545` ranks only 8th, so the model is *not* simply
rediscovering the yield effect.

### Live demo, 2026-06-12 → ex-date 2026-06-15

**The demo is a proof of the engineering, not a performance measurement.** Its
purpose is to show that a model trained offline can be loaded into a streaming
query and produce signals against a live-arriving feed at the correct decision
moment. Per-day P&L is deliberately not reported: a single ex-date cluster is
one draw from a distribution whose daily noise is an order of magnitude larger
than the effect, so it carries no information about the model in either
direction. **The 12,935-event out-of-sample table above is the result.**

- 944,515 session bars replayed through Kafka at 600× (~1.5 min wall)
- ~9,100 tickers on the wire → **651 candidates** after the stream-static join
- 5 micro-batches, event time climbing 10:30 → 12:00 → 13:30 → 15:00 → 15:30 ET
- **651 scored: 169 BUY, 482 SKIP**

Top selections by probability:

```
BUY  STRF     p=0.837  yield=2.62%
BUY  AHLpE    p=0.807  yield=1.80%
BUY  MSBIP    p=0.775  yield=1.91%
BUY  VNOpM    p=0.771  yield=1.86%
BUY  PNFPpA   p=0.764  yield=1.82%
```

Conviction evolving for one ticker as the session fills:

```
bins= 6   p=0.459  SKIP  (provisional)
bins= 9   p=0.617  BUY   (provisional)
bins=12   p=0.650  BUY   (provisional)
bins=13   p=0.844  BUY   (OFFICIAL)
```

That trace is the streaming property worth pointing at: the same model, given
progressively more of the session, revises its verdict from SKIP to BUY and
firms up as the evidence accumulates. Only the 13-bin score at 15:45 is an
official signal.

Two observations on the selections. They are overwhelmingly **preferred
shares** (`AHLpE`, `MSBIP`, `VNOpM`, `PNFPpA`, `TFINp`, `VLYPO`), which follows
from the yield relationship since preferreds are high-yield by construction —
but it means any real strategy would concentrate in a thin, illiquid corner of
the market where the cost assumption is least defensible. And the full model is
visibly more selective than an earlier throwaway model trained on 1,549 rows,
which selected 232 BUYs with yields as low as 0.11% — events M2 showed are dead
at any realistic cost.

---

## 6. Architecture

```
   HDFS: min_2026-06/min_2026-06-12.csv.gz     HDFS: div_event_grain
            │                                        intraday_features
            ▼                                        tickers_all_5y_metadata
  ┌──────────────────────┐                                  │
  │ replay_producer.py   │                                  ▼
  │ sorted to event time │                    ┌──────────────────────────┐
  │ 60-600x accelerated  │                    │ gen_train_capture_model  │
  │ key = ticker         │                    │ GBT + LogReg, temporal   │
  └──────────┬───────────┘                    └────────────┬─────────────┘
             │ publish                                     │ save
             ▼                                             ▼
  ┌──────────────────────┐   ┌──────────────┐   HDFS: models/capture_gbt
  │  KAFKA BROKER :19092 │◀─▶│ ZOOKEEPER    │              │
  │  topic: minute-bars  │   │ :12181       │              │
  │  3 partitions        │   └──────────────┘              │
  └──────────┬───────────┘                                 │
             │ subscribe                                   │
             ▼                                             │
  ┌─────────────────────────────────────────────────┐      │
  │ stream_scorer.py  (Structured Streaming)        │◀─────┘
  │  1. parse JSON, session filter                  │
  │  2. stream-static join -> candidates (9k -> 651)│
  │  3. withWatermark(ts_et, 10 min)                │
  │  4. groupBy(ticker, 30-min window)  <- STATE    │
  │  5. foreachBatch:                               │
  │       accumulate bins -> assemble 61 features   │
  │       PipelineModel.transform() -> BUY/SKIP     │
  │  6. Parquet + JSONL sink                        │
  └──────────┬──────────────────────────────────────┘
             ▼
   HDFS: streaming/signals   +   ~/signals_tail.jsonl
```

### Why `foreachBatch` and not a pure streaming query

**Structured Streaming permits one stateful aggregation per query.** Our
features are inherently two-level: bars → 30-minute bins, then bins → a session
row with a 39-column pivot. That chain is illegal in a streaming query.

`foreachBatch` hands you a **static** DataFrame per micro-batch, where
arbitrary batch logic is legal again. So the streaming engine owns level one —
the windowed bin aggregation, with real watermarks and real state — and the
handler owns level two. This is the documented pattern for exactly this
situation, not a workaround.

### Why update mode

Append emits a window only once the watermark passes its end. We filter out
everything at or after 15:45, so the final bin's window would never close and
the signal would never fire. Update emits current state every batch; because
the aggregates are cumulative, the handler upserts and converges.

### Why the candidate join comes first

~9,100 tickers arrive; only the ~650 with an ex-date on the next trading day
matter. Joining before the aggregation keeps streaming state at a few hundred
keys rather than nine thousand. This is the canonical stream-static join, and
here it does real work.

### 30-minute windows align with the batch bins

`window(ts_et, "30 minutes")` produces 09:30–10:00, 10:00–10:30, … exactly the
batch bin boundaries. Filtering `min_of_day < 945` before aggregating means the
15:30 window holds only 15 minutes, matching the batch stub bin precisely.

### Scoring policy

**One official signal per candidate, at 15:45** — that is the feature
distribution the model was trained on. Provisional scores are emitted every
batch once a ticker has ≥4 bins, flagged `provisional: true`. They feed the
model a partial session with the remaining bins zero-filled, which is *not* the
training distribution, so they are honest as "what the model would say if
forced to decide now" and not as calibrated probabilities.

### Why Kafka and not the file source

Structured Streaming can read a file source, and that was our fallback. Kafka
earned its place concretely during development: the scorer crashed on a missing
topic *after* the producer had already published all 944,515 bars, and we
replayed the entire day from offset 0 without re-running the producer. That is
**replay**, plus **decoupling** (a slow consumer buffers rather than drops),
**ordering within a partition** (keyed by ticker, so each ticker's bars stay
sequential), and genuine per-message flow rather than whole-file granularity.

### MongoDB

Not available: no `mongod`, nothing on 27017, no install path without root.
`pymongo` is installed and the sink is one function, so swapping to a Mongo
Atlas instance is a five-minute change. The shipped sink writes **Parquet to
HDFS plus a local JSONL tail**. Architecturally equivalent; one fewer checkbox.

---

## 7. Infrastructure setup — full reproduction

### 7.1 What is on the cluster

| | |
|---|---|
| Kafka | **3.1.0, Scala 2.12**, at `/usr/lib/kafka` — installed, not running |
| Spark | **3.5.3, Scala 2.12.18** |
| Java | OpenJDK 11.0.20.1 |
| ZooKeeper | this Kafka predates KRaft here — `config/` has **no `kraft/`**, so ZooKeeper is required |
| MongoDB | absent |

A ZooKeeper already listens on **2181** — Dataproc's own, serving Hive/HBase.
**Do not use it.** Its ACLs are invisible to us, it belongs to services we do
not own, and a second student registering `broker.id=0` there would collide.
Run your own on a private port.

### 7.2 The landmines

| # | trap | why it bites | fix |
|---|---|---|---|
| 1 | `/usr/lib/kafka/config/` is root-owned | cannot edit in place | copy to `~/kafka-conf/` |
| 2 | default `log.dirs=/tmp/kafka-logs` | shared across all students; two brokers on one log dir corrupt each other; `/tmp` gets cleaned | `~/kafka-data/broker` |
| 3 | `kafka-server-start.sh` writes log4j to `/usr/lib/kafka/logs` | root-owned → broker fails with a confusing stack trace | `export LOG_DIR=~/kafka-data/logs` |
| 4 | `advertised.listeners` defaulting to localhost | **silently half-works** — the driver connects (it is on the master), executors on workers cannot; the query hangs with no error | must be the FQDN |
| 5 | `offsets.topic.replication.factor` defaults to 3 | a single broker cannot satisfy it; consumer group coordination fails cryptically | set to 1 |
| 6 | **topics are not auto-created for consumers** | Spark fails with `UnknownTopicOrPartitionException` before a producer has ever run | create the topic explicitly first |
| 7 | **client-mode drivers die silently** | twice a long `spark-submit` vanished with no stack trace, leaving an orphaned YARN app (RUNNING, 0 containers). Foreground jobs piped through `tee`/`grep` are exposed to SIGHUP from a browser SSH blip | **always `nohup ... &`** |

Landmine 4 is the nastiest because it looks like success. Landmine 7 cost us
two ~50-minute runs.

### 7.3 Setup

```bash
ss -tln | grep -E ':(12181|19092)\s' || echo "both free"
mkdir -p ~/kafka-conf ~/kafka-data/{zookeeper,broker,logs}

grep -n LOG_DIR ~/.bashrc_local || \
  echo "export LOG_DIR=$HOME/kafka-data/logs" >> ~/.bashrc_local
grep -n KAFKA_BROKER ~/.bashrc_local || \
  echo "export KAFKA_BROKER=$(hostname -f):19092" >> ~/.bashrc_local
source ~/.bashrc
```

`~/kafka-conf/zookeeper.properties`:

```properties
dataDir=/home/<netid>/kafka-data/zookeeper
clientPort=12181
maxClientCnxns=0
admin.enableServer=false
```

`admin.enableServer=false` is not cosmetic — ZooKeeper's admin server defaults
to port 8080, which is very likely taken on a Dataproc master.

`~/kafka-conf/server.properties`:

```properties
broker.id=0
listeners=PLAINTEXT://:19092
advertised.listeners=PLAINTEXT://<hostname -f>:19092
log.dirs=/home/<netid>/kafka-data/broker
zookeeper.connect=localhost:12181

num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.retention.hours=48
```

### 7.4 Start — ZooKeeper first

```bash
export LOG_DIR=$HOME/kafka-data/logs

nohup /usr/lib/kafka/bin/zookeeper-server-start.sh \
  ~/kafka-conf/zookeeper.properties > ~/kafka-data/zk.out 2>&1 &
sleep 10 && ss -tln | grep 12181 && echo "ZK LISTENING"

nohup /usr/lib/kafka/bin/kafka-server-start.sh \
  ~/kafka-conf/server.properties > ~/kafka-data/broker.out 2>&1 &
sleep 20 && grep -E "started|ERROR|FATAL" ~/kafka-data/broker.out | tail
# want: [KafkaServer id=0] started

grep -i "Connecting to zookeeper" ~/kafka-data/broker.out
# want: localhost:12181   (NOT 2181)

/usr/lib/kafka/bin/kafka-topics.sh --create --topic minute-bars \
  --bootstrap-server $KAFKA_BROKER --partitions 3 --replication-factor 1
```

### 7.5 Verify — two independent tests

**Test 1 — the broker works.** Use the FQDN, not localhost: this exercises the
same `advertised.listeners` path the executors will use.

```bash
/usr/lib/kafka/bin/kafka-topics.sh --create --topic spike \
  --bootstrap-server $KAFKA_BROKER --partitions 1 --replication-factor 1
echo "hello" | /usr/lib/kafka/bin/kafka-console-producer.sh \
  --topic spike --bootstrap-server $KAFKA_BROKER
/usr/lib/kafka/bin/kafka-console-consumer.sh --topic spike \
  --bootstrap-server $KAFKA_BROKER --from-beginning --max-messages 1
```

**Test 2 — Spark reaches it from executors.** This is the one that matters;
test 1 only proves Kafka works on the master.

```bash
spark-submit --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --num-executors 2 --executor-memory 2g ~/kafka_spike.py
```

with a five-line `spark.read.format("kafka")` **batch** read — deliberately not
`readStream`, because a batch read fails fast while a streaming query waiting
on nothing is indistinguishable from a hang.

**Version pinning is exact:** `_2.12` matches Spark's Scala, `3.5.3` matches
Spark. `--packages` fetches from Maven Central at submit time — a different
network path from HDFS or the vendor API, so verify it separately with
`curl -sI https://repo1.maven.org/maven2/`. After the first submit the jars
cache in `~/.ivy2`.

### 7.6 Python client

```bash
pip3 install --user kafka-python
python3 -c "import kafka; print(kafka.__version__)"     # 3.0.10
```

### 7.7 Data prerequisite

```bash
nohup python3 ingest/gen_pull_day_agg_data.py 2021-08 2026-08 minute_aggs_v1 \
  > pull_min.log 2>&1 &
```

**452 MB/month, 24.3 GB total, ~55 s/month → ~56 minutes**, 1,255 files.
Local staging peaks at one month. Check headroom first: `hdfs dfs -df -h /`.

2021-08 lands `PARTIAL` — the vendor grants a **rolling** five years and objects
older than ~2021-08-09 return 403 on GetObject even though LIST succeeds. The
boundary moves, so the landed HDFS data is the canonical artifact, not
something regenerable.

---

## 8. Pipeline

| # | script | what | timing |
|---|---|---|---|
| A | `m3_streaming/gen_intraday_features.py` | 2.5B minute bars → 50 features per (ticker, date). **Shared module.** | **14.2 min** |
| B | `m3_streaming/gen_train_capture_model.py` | join A + grain + metadata, temporal split, GBT + LogReg | ~15 min |
| C | `m3_streaming/replay_producer.py` | one day → Kafka, event-time order, accelerated | 1.5 min @600× |
| D | `m3_streaming/stream_scorer.py` | consume, window, join, score, sink | ~2 min |

### A — intraday features

```bash
nohup spark-submit --master yarn --deploy-mode client \
    --driver-memory 8g --num-executors 4 --executor-memory 6g --executor-cores 4 \
    --conf spark.ui.showConsoleProgress=false \
    m3_streaming/gen_intraday_features.py 2021-08 2026-08 > ~/feat.log 2>&1 &
```

Output: 7,587,906 (ticker, date) rows. Phase 1 (bin table) 11.2 min, phase 2
(features) 3.0 min.

**Two-phase materialization is why this finishes at all.** The feature graph
forks — the bin table feeds both the session aggregate and the per-bin pivot,
and market context forks it again for SPY. Without an explicit barrier Spark
re-scans all 2.5B rows per branch: a first attempt ran **56 minutes without
reaching the write stage**. Writing the bin table to Parquet and reading it
back cut the whole job to 14.2 minutes. `--skip-binned` reuses an existing bin
table so feature changes cost 3 minutes, not 14.

Also required: `--executor-cores 4` (16 task slots, not 8) since the 1,255
`.csv.gz` files are non-splittable and the scan is one task per file.

### B — train

```bash
nohup spark-submit --master yarn --deploy-mode client \
    --driver-memory 4g --num-executors 4 --executor-memory 6g --executor-cores 4 \
    --conf spark.ui.showConsoleProgress=false \
    m3_streaming/gen_train_capture_model.py > ~/train.log 2>&1 &
```

Saves `models/capture_gbt` plus `capture_gbt_meta.json` (feature list and
thresholds, so the scorer cannot drift from the trainer).

Dev slice for fast iteration:

```bash
... gen_train_capture_model.py \
    --features $TEAM/curated/intraday_features_dev \
    --train-end 2026-05-31 --model $TEAM/models/capture_gbt_dev
```

### D then C — scorer first, producer second

```bash
# clear prior state
hdfs dfs -rm -r -skipTrash $TEAM/streaming/ckpt_2026-06-12 2>/dev/null
rm -f ~/signals_tail.jsonl

# D — must be subscribed before bars arrive
nohup spark-submit --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --driver-memory 4g --num-executors 2 --executor-memory 4g \
  --conf spark.ui.showConsoleProgress=false \
  m3_streaming/stream_scorer.py 2026-06-12 --from-earliest > ~/stream.log 2>&1 &

watch -n 15 'grep -E "===|\[batch|OFFICIAL|    (BUY|SKIP)" ~/stream.log | tail -25'

# C — in a second window, once "start the producer now" appears
python3 m3_streaming/replay_producer.py 2026-06-12 --speed 600
```

`--from-earliest` replays whatever is already in the topic, so a re-run needs
no second producer pass. Ctrl-C the scorer once official signals print —
streaming queries never terminate on their own.

### Inspecting the signals

```bash
wc -l ~/signals_tail.jsonl            # official + provisional
hdfs dfs -ls $TEAM/streaming/signals  # official only, Parquet

# conviction trace for one ticker
python3 -c "
import json
for l in open('$HOME/signals_tail.jsonl'):
    d=json.loads(l)
    if d['ticker']=='EPM':
        print(f\"bins={d['n_bins_present']:4.0f} p={d['probability']:.3f} \"
              f\"{d['signal']:4s} provisional={d['provisional']}\")
"
```

Each signal record carries `realized_abn` — the event's true
`capture_ret_abn`, joined in for reference. It is **never** a model feature.
Note that per-day P&L computed from it is not a meaningful evaluation: one
ex-date cluster is a single draw, and the out-of-sample table in §5 is the
model's actual measured performance.

### Choosing a replay day

Must be a 2026 trading day (the test period) whose **next** trading day carries
many high-yield ex-dates. Query the grain table by `ex_date` and pick the
decision day before the winner. 2026-06-15 had 654 events / 212 above 1% yield,
so we replay **Friday 2026-06-12**.

---

## 9. What this demonstrates

| course technology | where |
|---|---|
| HDFS | all storage; 24.3 GB minute data, 1,255 files |
| Spark SQL / DataFrames | M1/M2 batch, feature engineering |
| Spark window functions | event panel, rolling 20-day context |
| **Spark MLlib** | GBT + LogReg, `Pipeline`, `StringIndexer`, `OneHotEncoder`, `VectorAssembler` |
| **Kafka** | replay transport, keyed partitioning, offsets, replay-from-zero |
| **ZooKeeper** | broker coordination |
| **Spark Structured Streaming** | watermarks, windowed state, stream-static join, update mode, `foreachBatch` |
| Parquet / partitioning | every curated output |

**Scale:** ~2.5 billion minute bars (2,020,368 per trading day × 1,255 days),
7.6M feature rows, 163,358 events, 24.3 GB.

---

## 10. Known limitations

- **`mkt_sess_ret` is a day-level feature** and dominates importance. The model
  is doing more day-timing than stock-selection. Not disentangled.
- **Gross, not net.** Selected mean 12.35 bps against a 10 bps cost assumption;
  the selected median is below cost. No measured spreads — M2's NBBO work would
  replace the placeholder.
- **Provisional scores are off-distribution** — partial sessions with the
  remaining bins zero-filled. Flagged, not calibrated.
- **The demo is one day.** 651 signals on 2026-06-12 is a single draw; the
  12,935-event test table is the result.
- **The two-implementation risk is real.** Batch level-two lives in Spark SQL,
  streaming level-two in Python. They were verified by hand, not by an
  automated equality test — that test is the highest-value thing still missing.
- **Driver-side streaming state is not fault-tolerant.** The `foreachBatch`
  handler accumulates in a plain dict; a driver restart loses it. Correct for a
  replay demo, wrong for production, where it would live in
  `flatMapGroupsWithState`.
- **`sess_vol_ratio` has extreme outliers** (one ticker at 171,201× its 20-day
  norm — a newly-listed name with a near-zero trailing average). Unclipped.
