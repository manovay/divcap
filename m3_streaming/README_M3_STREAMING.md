# Milestone 3 — Real-Time Signal Engine

Batch M1/M2 measure the dividend-capture anomaly across 163,358 historical
events. M3 turns that measurement into a **decision system**: a model trained
offline on four years of intraday behaviour, deployed against a live-arriving
stream of minute bars, emitting BUY/SKIP signals at the moment the trade would
actually be placed.

The stack is Kafka → Spark Structured Streaming → Parquet → dashboard, with
Spark MLlib for the model.

---

## 1. The decision being modelled

The dividend-capture trade: **buy at the close on ex−1, sell at the open on the
ex-date, collect the dividend.**

Theory says the price should fall by exactly the dividend, making this a wash.
M1 measured a median `drop_ratio` of 0.870 — the price falls by ~87% of the
dividend, so ~13% is retained. M2's yield buckets showed the retained fraction
is roughly constant, so the *return* scales with dividend size: median
`capture_ret_abn` runs from −0.4 bps below 0.25% yield to +22.6 bps above 2%.

That is a measurement, not a strategy. The strategy question is: **standing at
15:45 ET on ex−1, looking at today's trading, should I take this trade?**

Everything the model sees must be knowable at 15:45 ET on the day before the
ex-date. That constraint drives every design decision below.

---

## 2. Model selection: why not a sequence model

The natural instinct with minute data is to feed the raw sequence to an LSTM or
1D-CNN and let it learn volatility, drift and volume patterns as latent
features. We considered this and rejected it for four independent reasons, any
one of which is disqualifying.

**Spark MLlib has no sequence models.** The library offers linear/logistic
regression, decision trees, random forests, GBTs, naive Bayes, a fixed-width
`MultilayerPerceptronClassifier`, clustering, ALS and FP-growth. There is no
LSTM, no CNN, no attention. Every available model is *tabular*: it consumes a
fixed-width feature vector and has no notion of column order or adjacency.
Shuffle the columns and a GBT trains identically — which is precisely why it
cannot learn "volatility" from 960 minute columns, since volatility is a
property of sequence.

**Variable-length input has to be collapsed regardless.** AAPL trades 645
minute bars in a session; a thin REIT trades 5. Any tabular model needs fixed
width, so aggregation is not optional — the only question is whether it is
chosen deliberately or naively. Naive options both fail: 960 columns (one per
minute slot) gives the model no adjacency prior and is mostly null, and letting
a deep model learn the collapse requires a library we do not have.

**The data does not support representation learning.** Deep sequence models
need examples in rough proportion to input dimensionality. Raw minute input is
~4,800 values per event (960 minutes × 5 fields) against ~30,000 training
events in the high-yield subset. Compare ImageNet: 150K pixels, 14M examples.
Worse, the signal-to-noise ratio here is brutal — a ~20 bps effect against
100–200 bps of daily volatility. A flexible model in that regime finds spurious
structure with near-certainty.

**Streaming inference forces fixed-width features.** A windowed aggregation is
native to Structured Streaming: `groupBy` a time window, keep a few doubles of
state per key, let the watermark evict it. Buffering a variable-length ordered
per-ticker sequence requires `flatMapGroupsWithState`, hand-written eviction,
hand-written event-time ordering, state that grows with the buffer and is
checkpointed to HDFS every trigger, and a Pandas UDF shipping model weights to
every executor for inference. That is a semester project, not a milestone.

This last point is the honest headline: **the deployment target constrains the
model class.** The same reason most production trading systems score engineered
features rather than raw ticks.

### What we chose: Gradient-Boosted Trees

`GBTClassifier` on ~50 engineered features.

| reason | detail |
|---|---|
| Handles mixed types | no scaling needed across yield, volume ratios, categoricals |
| Captures interactions | "high yield **and** low volatility" without being told |
| Robust to outliers | matters given `drop_ratio`'s long tail |
| **Feature importances** | the actual deliverable — *which characteristics predict a profitable capture* |
| MLlib-native | `PipelineModel.transform()` works directly on a streaming DataFrame |

Baseline is `LogisticRegression` on the same features. If the GBT cannot beat
it, that is informative rather than embarrassing.

**We did not simply hand-pick five aggregates.** The session is cut into 13
bins and the model is given all of them, so it discovers *when* in the day
matters rather than us asserting it. "Bin 12 volume share dominates" is a
finding about closing-auction pressure; it is not something we encoded.

---

## 3. Transforming minute bars into learnable features

### The decision moment

**15:45 ET on ex−1.** Fifteen minutes before the bell — enough to actually
place an order. Every feature uses bars from 09:30 to 15:45 only. Pre-market
bars (the flat files carry 04:00–20:00) are dropped: they are thin and would
distort every share-based feature.

### Binning

375 minutes from 09:30 to 15:45 = **12 half-hour bins plus a 15-minute stub =
13 bins**. Per bin, three scale-free values:

| feature | definition | captures |
|---|---|---|
| `bin{k}_ret` | `b_close / b_open − 1` | intraday drift shape |
| `bin{k}_volshare` | bin volume ÷ session volume through 15:45 | *when* trading happens |
| `bin{k}_range` | `(b_high − b_low) / b_close` | intraday volatility shape |

39 columns. Everything is scale-free by construction — raw volume is not
comparable between AAPL and a $200M REIT, and a model given raw volume learns
"big company" and nothing else.

**Bin returns are bin-internal, not chained across bins.** A chained return
(`close_k / close_{k−1}`) nulls out the entire downstream chain when a thin
ticker misses one bin, and thin tickers miss bins constantly. Internal returns
degrade gracefully to a single null.

**`volshare` uses the session total through 15:45**, not a running total. At
the decision moment the whole session so far is known, so this is not lookahead
— and it makes the batch and streaming definitions trivially identical.

### Session-level features

| feature | definition |
|---|---|
| `sess_ret` | 09:30 open → 15:45 close |
| `sess_realized_vol` | stdev of the 13 bin returns |
| `sess_range` | (session high − low) ÷ close |
| `sess_volume`, `sess_trades`, `sess_bars` | activity levels |
| `n_bins_present` | data-quality flag — thin names have gaps |
| `gap_ret` | today's open vs. yesterday's close |
| `sess_vol_ratio` | session volume ÷ its own trailing 20-day average |

The last two need prior-day history. A stream has no history, so the streaming
job reads them from a small precomputed table rather than deriving them live.

### Event-level features (from M1's grain table)

`div_yield`, `pre_vol`, `pre_avg_dollar_volume`, `frequency`,
`n_distributions`, `sec_type`, `primary_exchange`, sector, and days from
`declaration_date` to `ex_date`.

**~50 features total on ~30,000 training events.** Comfortable for a GBT.

### Leakage discipline

An explicit **whitelist** in code, never a blacklist — blacklists fail silently
when someone adds a column.

Banned: `ex_open`, `ex_close`, `post_close`, `drop_ratio`, `drop_pct`,
`capture_ret`, `capture_ret_abn`, `post_avg_ret` — all derived from or after
the outcome. Also banned, subtly: **`market_cap` from the ticker metadata**,
because it is a *current* snapshot, so for a 2022 event it encodes what the
company later became. `pre_avg_dollar_volume` is the correctly-dated substitute.

### The skew guard

`streaming/intraday_features.py` exports `build_features(bars)` — minute bars
in, one row per (ticker, date) out. **Both the batch trainer and the streaming
scorer import and call this same function.** If they computed a feature
differently the model would receive a vector from a distribution it was not fit
on, predictions would degrade, and *nothing would error*.

A test asserts that batch and streaming produce identical vectors for one known
(ticker, date). This is the single highest-value test in the project.

---

## 4. Train / test design

| | window | events |
|---|---|---|
| **Train** | 2021-08 … 2025-12 | ~130K, ~25K after the yield filter |
| **Test / replay** | 2026-01 … 2026-08 | ~25K, ~5K after filter |

**Temporal split, not `randomSplit`.** A random split puts 2025 events in
training and 2022 events in test — the model learns from the future. The 2026
replay is therefore a genuine out-of-sample test, not theatre.

(Note the contrast with `sector_ml`, where `randomSplit` *is* correct: sector
is a static company attribute with no time dimension. The temporal rule applies
to return prediction.)

**Label:** `capture_ret_abn > 0.0010` — 10 bps, a stated round-trip cost
placeholder until M2's spread work lands. On the `div_yield >= 0.01` subset the
median is ~13.5 bps, so the base rate sits near 50% and this is a real
classification problem rather than a degenerate one.

**Filters:** `has_core`, `window_contiguous`, `ticker != 'SPY'` (circular — SPY
is the market proxy, so its own `capture_ret_abn` reduces to its dividend
yield), `div_yield >= 0.01`.

**Evaluation is P&L, not accuracy.** If 50% of events are profitable, a model
predicting "never trade" is 50% accurate and earns nothing. Report:

| metric | question |
|---|---|
| mean `capture_ret_abn` of predicted-BUY events | does selection beat no selection? |
| same across all test events | the "always trade" baseline |
| count selected | tradeable, or three events a year? |
| hit rate among selected | how often right |
| AUC | ranking quality, secondary |

**The comparison that matters is selected vs. always-trade.** 800 events at
+15 bps against a full-set +5 bps is a genuine result; 12 events is overfitting.

**A null result is a finding.** If nothing beats the baseline, that is
consistent with the market being efficient after costs. Manufacturing a
positive result by leaking features or shuffling the split is the only real
failure available.

---

## 5. Architecture

```
   HDFS: min_2026-MM/*.csv.gz          HDFS: div_event_grain
   (one historical trading day)        intraday_features
            │                          tickers_all_5y_metadata
            ▼                                   │
  ┌──────────────────────┐                      ▼
  │  replay_producer.py  │            ┌────────────────────────┐
  │  event-time order    │            │ train_capture_model.py │
  │  60× accelerated     │            │ GBT, temporal split    │
  │  key = ticker        │            └───────────┬────────────┘
  └──────────┬───────────┘                        │ save
             │ publish                            ▼
             ▼                          HDFS: models/capture_gbt
  ┌──────────────────────┐   ┌──────────────┐             │
  │   KAFKA BROKER       │◀─▶│  ZOOKEEPER   │             │
  │   topic: minute-bars │   │  :12181      │             │
  │   :19092             │   └──────────────┘             │
  └──────────┬───────────┘                                │
             │ subscribe                                  │
             ▼                                            │
  ┌─────────────────────────────────────────────────┐     │
  │  stream_scorer.py  (Structured Streaming)       │◀────┘
  │   1. parse JSON, withWatermark(ts_et, 10 min)   │
  │   2. stream-static join → tomorrow's ex-div     │
  │      calendar  (12,000 tickers → ~200)          │
  │   3. windowed groupBy → 13-bin features         │
  │   4. join precomputed daily context             │
  │   5. PipelineModel.transform() → BUY / SKIP     │
  │   6. foreachBatch → Parquet + JSONL tail        │
  └──────────┬──────────────────────────────────────┘
             ▼
   HDFS: streaming/signals ────▶  dashboard.py
```

**The stream-static join in step 2 is the piece worth highlighting.** It is the
canonical Structured Streaming pattern for enriching a stream with a dimension
table, and here it does real work: ~12,000 streaming tickers collapse to the
~200 with an ex-date tomorrow.

**Closing the loop:** the replay covers ex−1 *and* the ex-date open, so once
the opening bars arrive the system computes realized `capture_ret_abn` for its
own predictions and updates a running P&L. The dashboard shows predictions
being made and then scored. That is the difference between "we streamed data"
and "we built a signal engine."

### Why Kafka and not just files

Structured Streaming can read from a file source, and that was our fallback.
Kafka earns its place on four counts: **replay** (a crashed consumer resumes
from its stored offset), **decoupling** (a slow consumer causes buffering, not
data loss), **ordering** (guaranteed within a partition, so keying by ticker
keeps each ticker's bars sequential — which matters because the features are
sequential), and **genuine per-message flow** rather than whole-file
granularity.

### MongoDB

Not available on the cluster: no `mongod`, nothing listening on 27017, no
install path without root. `pymongo` is installed and the sink is a single
function, so swapping to a Mongo Atlas instance is a five-minute change. The
shipped sink writes **Parquet to HDFS plus a local JSONL tail** for the
dashboard. Architecturally equivalent; one fewer checkbox.

---

## 6. Infrastructure setup — full reproduction

### 6.1 What is already on the cluster

| | |
|---|---|
| Kafka | **3.1.0, Scala 2.12**, at `/usr/lib/kafka` — installed, not running |
| Spark | **3.5.3, Scala 2.12.18** |
| Java | OpenJDK 11.0.20.1 |
| ZooKeeper | Kafka 3.1.0 predates KRaft mode here — `config/` has **no `kraft/` directory**, so a ZooKeeper is required |

A ZooKeeper is already listening on **2181** — that is Dataproc's own, serving
Hive/HBase. **Do not use it.** We cannot see its ACLs, it belongs to services we
do not own, and a second student registering `broker.id=0` there would collide
with us. Run our own.

### 6.2 The five landmines

| # | trap | why it bites | fix |
|---|---|---|---|
| 1 | `/usr/lib/kafka/config/` is root-owned | cannot edit in place | copy configs to `~/kafka-conf/` |
| 2 | Default `log.dirs=/tmp/kafka-logs` | shared across all students; two brokers on one log dir corrupt each other; `/tmp` gets cleaned | `~/kafka-data/broker` |
| 3 | `kafka-server-start.sh` writes log4j output to `/usr/lib/kafka/logs` | root-owned → broker fails to start with a confusing stack trace | `export LOG_DIR=~/kafka-data/logs` |
| 4 | `advertised.listeners` defaulting to localhost | **silently half-works** — the driver connects (it is on the master), executors on workers cannot. Query hangs with no error | must be the FQDN |
| 5 | `offsets.topic.replication.factor` defaults to 3 | single broker cannot satisfy it; consumer group coordination fails cryptically | set to 1 |

Landmine 4 is the nastiest because it looks like success.

### 6.3 Setup

```bash
# ports: 12181 (our ZK), 19092 (our broker). Verify free first.
ss -tln | grep -E ':(12181|19092)\s' || echo "both free"

mkdir -p ~/kafka-conf ~/kafka-data/zookeeper ~/kafka-data/broker ~/kafka-data/logs

grep -n LOG_DIR ~/.bashrc_local || \
  echo 'export LOG_DIR=/home/'$USER'/kafka-data/logs' >> ~/.bashrc_local
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

### 6.4 Start (order matters — ZooKeeper first)

```bash
export LOG_DIR=/home/$USER/kafka-data/logs

nohup /usr/lib/kafka/bin/zookeeper-server-start.sh ~/kafka-conf/zookeeper.properties \
  > ~/kafka-data/zk.out 2>&1 &
sleep 10
ss -tln | grep 12181 && echo "ZK LISTENING"

nohup /usr/lib/kafka/bin/kafka-server-start.sh ~/kafka-conf/server.properties \
  > ~/kafka-data/broker.out 2>&1 &
sleep 20
grep -E "started|ERROR|FATAL" ~/kafka-data/broker.out | tail
# want: [KafkaServer id=0] started
```

Confirm it bound to *our* ZooKeeper:

```bash
grep -i "Connecting to zookeeper" ~/kafka-data/broker.out
# want: localhost:12181  (not 2181)
```

### 6.5 Verify — two independent tests

**Test 1: broker works.** Use the FQDN, not localhost — this exercises the same
`advertised.listeners` path the executors will use.

```bash
/usr/lib/kafka/bin/kafka-topics.sh --create --topic spike \
  --bootstrap-server $KAFKA_BROKER --partitions 1 --replication-factor 1
echo "hello" | /usr/lib/kafka/bin/kafka-console-producer.sh \
  --topic spike --bootstrap-server $KAFKA_BROKER
/usr/lib/kafka/bin/kafka-console-consumer.sh --topic spike \
  --bootstrap-server $KAFKA_BROKER --from-beginning --max-messages 1
```

**Test 2: Spark can reach it from executors.** This is the one that matters —
test 1 only proves Kafka works on the master.

```bash
spark-submit --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --num-executors 2 --executor-memory 2g \
  ~/kafka_spike.py
```

with a five-line `spark.read.format("kafka")` batch read. Deliberately a batch
read, not `readStream` — it fails fast, whereas a streaming query waiting on
nothing is indistinguishable from a hang.

**Version pinning is exact:** `_2.12` matches Spark's Scala, `3.5.3` matches
Spark. `--packages` fetches from Maven Central at submit time, which is a
different network path from HDFS or the vendor API and must be verified
separately (`curl -sI https://repo1.maven.org/maven2/`). After the first submit
the jars cache in `~/.ivy2` and subsequent runs start much faster.

### 6.6 Data prerequisite

Minute aggregates, 2021-08 … 2026-08:

```bash
nohup python3 ingest/gen_pull_day_agg_data.py 2021-08 2026-08 minute_aggs_v1 \
  > pull_min.log 2>&1 &
```

**~452 MB/month, ~27 GB total, ~55 s/month → ~56 minutes.** Local staging peaks
at one month. Confirm headroom first: `hdfs dfs -df -h /` and `df -h /home`.

2021-08 lands `PARTIAL` — the vendor grants a **rolling** five years and objects
older than ~2021-08-09 return 403 on GetObject even though LIST succeeds. The
boundary moves; the landed HDFS data is the canonical artifact, not something
regenerable.

---

## 7. Pipeline

| # | script | what | inputs → output |
|---|---|---|---|
| A | `streaming/intraday_features.py` | minute bars → 44 features per (ticker, date). **Shared module.** | `min_*` → `curated/intraday_features` |
| B | `streaming/train_capture_model.py` | join A + grain + metadata, temporal split, GBT | → `models/capture_gbt` |
| C | `streaming/replay_producer.py` | one day's bars → Kafka, event-time order, 60× | HDFS → topic |
| D | `streaming/stream_scorer.py` | consume, window, join, score, sink | topic → `streaming/signals` |
| E | `streaming/dashboard.py` | live signal feed + running P&L | Parquet/JSONL |
| F | `streaming/start_kafka.sh` | cold-start ZK + broker | — |

### Run order

```bash
# A — batch features (time this; it decides whether we widen the window)
spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 4g --executor-cores 2 \
    streaming/intraday_features.py 2021-08 2026-08

# B — train
spark-submit ... streaming/train_capture_model.py

# D — start the scorer FIRST, it must be listening before bars arrive
spark-submit --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  ... streaming/stream_scorer.py 2026-XX-XX

# C — then replay
python3 streaming/replay_producer.py 2026-XX-XX --speed 60

# E — watch
python3 streaming/dashboard.py
```

**Order matters.** Start the consumer before the producer, or the first bars
land before anyone is subscribed. (`startingOffsets=earliest` mitigates this,
but starting in order avoids the question entirely.)

### Choosing the replay day

Must be a 2026 trading day (the test period) with many ex-dates on the
following trading day. Quarter-end weeks cluster heavily. Verify the chosen day
is not in the training window.

---

## 8. Risks

| risk | severity | mitigation |
|---|---|---|
| **Training/serving skew** | high | shared `build_features()`; equality test on a known (ticker, date) |
| Kafka dies before the demo | medium | `start_kafka.sh`; rehearse a cold start |
| Executors cannot reach broker | medium | verified in setup test 2; symptom is a silent hang |
| Model beats nothing | medium | **report it** — the M2 yield-bucket result stands alone |
| Streaming state bugs | medium | plain windowed aggregations only; no `flatMapGroupsWithState` |
| Feature job too slow | low | narrow the training window to 2024–2025 |

### Fallbacks, in order of what to cut

1. Dashboard → console prints
2. Realized P&L → predictions only
3. Parquet sink → console only
4. **Model → hardcoded rule** (`yield ≥ 2% AND vol_ratio < 1.5`)

A working stream scoring a simple rule beats a broken stream scoring a GBT.

---

## 9. What this demonstrates

| course technology | where |
|---|---|
| HDFS | all storage, 27 GB minute data |
| Spark SQL / DataFrames | M1/M2 batch, feature engineering |
| Spark window functions | event panel, bin aggregation, rolling context |
| **Spark MLlib** | GBT classifier, `Pipeline`, `StringIndexer`/`OneHotEncoder` |
| **Kafka** | replay transport, keyed partitioning, offsets |
| **ZooKeeper** | broker coordination |
| **Spark Structured Streaming** | watermarks, windowed state, stream-static join, `foreachBatch` |
| Parquet / partitioning | every curated output |

Scale: ~2 billion minute bars, 163,358 events, 27 GB.
