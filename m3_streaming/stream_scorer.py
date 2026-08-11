#!/usr/bin/env python3
"""
Stage D -- consume replayed minute bars, build features live, score the model.

    Kafka -> parse -> session filter -> stream-static join (candidates)
          -> watermark -> 30-min windowed aggregation (the state)
          -> foreachBatch -> assemble features -> PipelineModel.transform()
          -> Parquet + JSONL sink

WHY foreachBatch AND NOT A PURE STREAMING QUERY
Structured Streaming permits ONE stateful aggregation per query. Our features
are inherently two-level: aggregate bars into 30-minute bins, then aggregate
bins into a session row with a 39-column pivot. That chain is illegal in a
streaming query.

foreachBatch hands you a STATIC DataFrame per micro-batch, where arbitrary
batch logic is legal again. So the streaming engine owns level one (the
windowed bin aggregation, with real watermarks and real state) and the
handler owns level two. This is the documented pattern for exactly this
situation, not a workaround.

WHY UPDATE MODE
Append emits a window only once the watermark passes its end. We filter out
everything at or after 15:45, so the final bin's window would never close and
the signal would never fire. Update emits current state every batch; because
the aggregates are cumulative, the handler upserts and converges to the same
answer.

WHY THE CANDIDATE JOIN COMES FIRST
~9,000 tickers arrive on the wire; only the ~200-650 with an ex-date on the
next trading day matter. Joining before the aggregation means streaming state
is a few hundred keys rather than nine thousand. This is the canonical
stream-static join, and here it does real work.

SCORING POLICY
One official signal per candidate, at the 15:45 decision moment, because that
is the feature distribution the model was trained on. Provisional scores are
emitted every batch once enough bins exist, flagged `provisional=true`, for
the dashboard only -- they are computed on partial sessions and must not be
read as decisions.

Run (start this BEFORE the producer):
    spark-submit --master yarn --deploy-mode client \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      --driver-memory 4g --num-executors 2 --executor-memory 4g \
      --conf spark.ui.showConsoleProgress=false \
      m3_streaming/stream_scorer.py 2026-06-12 \
        --model $TEAM/models/capture_gbt_dev

Outputs:
    $TEAM/streaming/signals/           Parquet, one row per scored candidate
    ~/signals_tail.jsonl               local append, for the dashboard
"""

import os
import sys
import json
import time
from pyspark.sql import SparkSession, functions as F, types as T

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gen_intraday_features import (            # noqa: E402
    N_BINS, SESSION_START_MIN, DECISION_MIN, BIN_MINUTES, MARKET,
    INTRADAY_FEATURES,
)

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "minute-bars")

TAIL = os.path.expanduser("~/signals_tail.jsonl")
MIN_BINS_PROVISIONAL = 4

BAR_SCHEMA = T.StructType([
    T.StructField("ticker", T.StringType()),
    T.StructField("volume", T.DoubleType()),
    T.StructField("open", T.DoubleType()),
    T.StructField("close", T.DoubleType()),
    T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()),
    T.StructField("window_start", T.LongType()),
    T.StructField("transactions", T.LongType()),
])


# ------------------------------------------------------------ driver state
# The handler accumulates bin rows across micro-batches. A plain dict on the
# driver is correct here: one driver, one stream, and the volume is a few
# hundred tickers x 13 bins. It is NOT fault-tolerant -- a driver restart
# loses it. For a replay demo that is the right trade; a production system
# would keep this in the state store via flatMapGroupsWithState.
STATE = {}          # ticker -> {bin_idx: agg dict}
SCORED = set()      # tickers with an official signal already emitted
MAX_MIN = {"v": -1}  # high-water mark of event-time minute-of-day
COUNTS = {"batches": 0, "rows": 0, "provisional": 0, "official": 0}


def parse_args(argv):
    a = {"day": None,
         "model": f"{TEAM}/models/capture_gbt",
         "out": f"{TEAM}/streaming/signals",
         "topic": TOPIC,
         "starting": "latest"}
    i = 0
    while i < len(argv):
        k = argv[i]
        if k == "--model":
            a["model"] = argv[i + 1]; i += 2
        elif k == "--out":
            a["out"] = argv[i + 1]; i += 2
        elif k == "--topic":
            a["topic"] = argv[i + 1]; i += 2
        elif k == "--from-earliest":
            a["starting"] = "earliest"; i += 1
        elif not k.startswith("--"):
            a["day"] = k; i += 1
        else:
            sys.exit(f"unknown arg: {k}")
    if not a["day"]:
        sys.exit("usage: stream_scorer.py YYYY-MM-DD [--model PATH] "
                 "[--from-earliest]")
    return a


# --------------------------------------------------------- static context

def load_candidates(spark, day):
    """
    Tickers with an ex-date on the trading day AFTER `day`, plus everything
    the model needs that is not derived from the stream.

    The decision day comes from the M1 event panel (offset == -1), which is
    already correct across weekends and holidays -- no need to rebuild a
    trading calendar here.
    """
    grain = spark.read.parquet(f"{TEAM}/curated/div_event_grain")
    panel = spark.read.parquet(f"{TEAM}/curated/div_event_panel")
    meta = spark.read.json(f"{TEAM}/reference/tickers_all_5y_metadata.jsonl")

    dec = (panel.filter((F.col("offset") == -1)
                        & (F.col("bar_date") == F.lit(day)))
           .select("ticker", "ex_date"))

    m = (meta.select(F.col("ticker").alias("m_ticker"),
                     F.col("type").alias("sec_type"),
                     "sic_description", "active")
         .orderBy(F.col("active").desc())
         .dropDuplicates(["m_ticker"])
         .drop("active"))

    c = (grain.join(dec, ["ticker", "ex_date"], "inner")
         .filter(F.col("ticker") != MARKET)
         .select("ticker", "ex_date", "cash_amount", "frequency",
                 "n_distributions", "pre_vol", "pre_avg_ret",
                 "pre_avg_abn_ret", "pre_avg_dollar_volume",
                 "declaration_date", "div_yield", "capture_ret_abn")
         .withColumn("days_decl_to_ex",
                     F.datediff("ex_date", F.to_date("declaration_date")))
         .join(F.broadcast(m), F.col("ticker") == F.col("m_ticker"), "left")
         .drop("m_ticker")
         .withColumn("sec_type", F.coalesce("sec_type", F.lit("UNKNOWN")))
         .withColumn("sic_description",
                     F.coalesce("sic_description", F.lit("UNKNOWN"))))
    return c


def load_daily_context(spark, day):
    """
    gap_ret and sess_vol_ratio for the decision day. A stream has no prior-day
    history, so these come from the batch feature table -- the same values the
    trainer saw, by construction.
    """
    f = spark.read.parquet(f"{TEAM}/curated/intraday_features")
    return (f.filter(F.col("date_et") == F.lit(day))
            .select("ticker", "gap_ret", "sess_vol_ratio"))


# ------------------------------------------------------ feature assembly

def bins_to_features(ticker, bins, mkt_ret, ctx):
    """
    Level two: bin rows -> one feature dict. Mirrors features_from_binned in
    gen_intraday_features.py exactly. Any divergence here is training/serving
    skew, which produces confident garbage and no error.
    """
    sess_volume = 0.0
    sess_trades = 0.0
    sess_bars = 0
    sess_high = None
    sess_low = None
    lo_bin = None
    hi_bin = None

    for k, b in bins.items():
        sess_volume += b["b_volume"]
        sess_trades += b["b_trades"]
        sess_bars += b["b_bars"]
        sess_high = b["b_high"] if sess_high is None else max(sess_high, b["b_high"])
        sess_low = b["b_low"] if sess_low is None else min(sess_low, b["b_low"])
        if lo_bin is None or k < lo_bin:
            lo_bin = k
        if hi_bin is None or k > hi_bin:
            hi_bin = k

    sess_open = bins[lo_bin]["b_open"]
    sess_close = bins[hi_bin]["b_close"]

    out = {"ticker": ticker}

    rets = []
    for k in range(N_BINS):
        b = bins.get(k)
        if b is None:
            out[f"bin{k}_ret"] = 0.0
            out[f"bin{k}_volshare"] = 0.0
            out[f"bin{k}_range"] = 0.0
            continue
        r = (b["b_close"] / b["b_open"] - 1.0) if b["b_open"] > 0 else 0.0
        out[f"bin{k}_ret"] = r
        rets.append(r)
        out[f"bin{k}_volshare"] = (b["b_volume"] / sess_volume
                                   if sess_volume > 0 else 0.0)
        out[f"bin{k}_range"] = ((b["b_high"] - b["b_low"]) / b["b_close"]
                                if b["b_close"] > 0 else 0.0)

    if len(rets) > 1:
        mu = sum(rets) / len(rets)
        var = sum((x - mu) ** 2 for x in rets) / (len(rets) - 1)
        out["sess_realized_vol"] = var ** 0.5
    else:
        out["sess_realized_vol"] = 0.0

    out["sess_ret"] = (sess_close / sess_open - 1.0) if sess_open > 0 else 0.0
    out["sess_range"] = ((sess_high - sess_low) / sess_close
                         if sess_close and sess_close > 0 else 0.0)
    out["sess_volume"] = sess_volume
    out["sess_trades"] = sess_trades
    out["sess_avg_trade_size"] = (sess_volume / sess_trades
                                  if sess_trades > 0 else 0.0)
    out["sess_bars"] = float(sess_bars)
    out["n_bins_present"] = float(len(bins))
    out["sess_close"] = sess_close

    out["mkt_sess_ret"] = mkt_ret if mkt_ret is not None else 0.0
    c = ctx.get(ticker, {})
    out["gap_ret"] = c.get("gap_ret") if c.get("gap_ret") is not None else 0.0
    out["sess_vol_ratio"] = (c.get("sess_vol_ratio")
                             if c.get("sess_vol_ratio") is not None else 1.0)
    return out


def make_handler(spark, model, cand_rows, ctx, out_path, day):
    """Returns the foreachBatch callback, closed over the static context."""

    def score(rows, provisional):
        """Assemble, transform, sink. `rows` is a list of feature dicts."""
        if not rows:
            return
        recs = []
        for r in rows:
            c = cand_rows.get(r["ticker"])
            if c is None:
                continue
            sc = r["sess_close"]
            if not sc or sc <= 0:
                continue
            rec = dict(r)
            rec.update({
                "ex_date": c["ex_date"],
                "cash_amount": float(c["cash_amount"]),
                "frequency": float(c["frequency"] or 0),
                "n_distributions": float(c["n_distributions"] or 1),
                "pre_vol": float(c["pre_vol"] or 0.0),
                "pre_avg_ret": float(c["pre_avg_ret"] or 0.0),
                "pre_avg_abn_ret": float(c["pre_avg_abn_ret"] or 0.0),
                "pre_avg_dollar_volume": float(c["pre_avg_dollar_volume"] or 0.0),
                "days_decl_to_ex": float(c["days_decl_to_ex"]
                                         if c["days_decl_to_ex"] is not None
                                         else -1),
                "sec_type": c["sec_type"],
                "sic_description": c["sic_description"],
                # recomputed against the 15:45 price, not the close: the
                # close has not happened at the decision moment
                "div_yield_1545": float(c["cash_amount"]) / sc,
                # carried for evaluation only, never a feature
                "realized_abn": (float(c["capture_ret_abn"])
                                 if c["capture_ret_abn"] is not None else None),
            })
            recs.append(rec)

        if not recs:
            return

        df = spark.createDataFrame(recs)
        pred = model.transform(df)

        want = ["ticker", "ex_date", "prediction", "probability",
                "div_yield_1545", "sess_ret", "sess_vol_ratio",
                "n_bins_present", "realized_abn"]
        got = pred.select(*[c for c in want if c in pred.columns]).collect()

        sig = []
        for g in got:
            d = g.asDict()
            p = d.get("probability")
            d["probability"] = float(p[1]) if p is not None else None
            d["ex_date"] = str(d["ex_date"])
            d["signal"] = "BUY" if d["prediction"] == 1.0 else "SKIP"
            d["provisional"] = provisional
            d["decision_day"] = day
            d["emitted_at"] = time.strftime("%H:%M:%S")
            sig.append(d)

        with open(TAIL, "a") as fh:
            for s in sig:
                fh.write(json.dumps(s) + "\n")

        if not provisional:
            (spark.createDataFrame(sig)
             .write.mode("append").parquet(out_path))
            COUNTS["official"] += len(sig)
            buys = sum(1 for s in sig if s["signal"] == "BUY")
            print(f"\n*** OFFICIAL SIGNALS: {len(sig)} scored, "
                  f"{buys} BUY, {len(sig)-buys} SKIP ***", flush=True)
            for s in sorted(sig, key=lambda x: -(x["probability"] or 0))[:10]:
                print(f"    {s['signal']:4s} {s['ticker']:8s} "
                      f"p={s['probability']:.3f} "
                      f"yield={s['div_yield_1545']*100:.2f}%", flush=True)
        else:
            COUNTS["provisional"] += len(sig)

    def handler(batch_df, batch_id):
        COUNTS["batches"] += 1
        rows = batch_df.collect()
        COUNTS["rows"] += len(rows)
        if not rows:
            return

        mkt_bins = {}
        for r in rows:
            k = int((r["win_start_min"] - SESSION_START_MIN) // BIN_MINUTES)
            k = min(k, N_BINS - 1)
            if r["win_start_min"] > MAX_MIN["v"]:
                MAX_MIN["v"] = r["win_start_min"]
            agg = {
                "b_open": r["b_open"], "b_close": r["b_close"],
                "b_high": r["b_high"], "b_low": r["b_low"],
                "b_volume": r["b_volume"], "b_trades": r["b_trades"],
                "b_bars": r["b_bars"],
            }
            if r["ticker"] == MARKET:
                mkt_bins[k] = agg
                continue
            STATE.setdefault(r["ticker"], {})[k] = agg

        # SPY's session return so far, for abnormal-return context
        mkt_ret = None
        if mkt_bins:
            lo, hi = min(mkt_bins), max(mkt_bins)
            o, c = mkt_bins[lo]["b_open"], mkt_bins[hi]["b_close"]
            if o and o > 0:
                mkt_ret = c / o - 1.0

        now = MAX_MIN["v"]
        et = f"{now//60:02d}:{now%60:02d}" if now >= 0 else "--:--"
        print(f"[batch {batch_id:3d}] {len(rows):5d} bin-updates  "
              f"event-time {et} ET  tracking {len(STATE)} candidates",
              flush=True)

        # --- the decision moment
        official = now >= DECISION_MIN - BIN_MINUTES
        pending = [t for t in STATE if t not in SCORED]

        if official and pending:
            feats = [bins_to_features(t, STATE[t], mkt_ret, ctx)
                     for t in pending]
            score(feats, provisional=False)
            SCORED.update(pending)
        else:
            prov = [t for t in STATE
                    if len(STATE[t]) >= MIN_BINS_PROVISIONAL]
            if prov:
                feats = [bins_to_features(t, STATE[t], mkt_ret, ctx)
                         for t in prov]
                score(feats, provisional=True)

    return handler


def main():
    a = parse_args(sys.argv[1:])

    spark = (SparkSession.builder.appName("divcap-stream-scorer")
             .getOrCreate())
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    print(f"\n=== decision day: {a['day']} ===")
    print(f"=== model: {a['model']} ===")
    print(f"=== broker: {BROKER}  topic: {a['topic']} ===")

    from pyspark.ml import PipelineModel
    model = PipelineModel.load(a["model"])
    print("=== model loaded ===")

    cand = load_candidates(spark, a["day"]).cache()
    n_cand = cand.count()
    print(f"=== {n_cand} candidates with an ex-date after {a['day']} ===")
    if n_cand == 0:
        sys.exit("no candidates -- is this the trading day before an ex-date?")
    cand.select("ticker", "ex_date", "cash_amount", "div_yield").show(10)

    cand_rows = {r["ticker"]: r.asDict() for r in cand.collect()}

    ctx_rows = {r["ticker"]: r.asDict()
                for r in load_daily_context(spark, a["day"]).collect()}
    print(f"=== daily context for {len(ctx_rows)} tickers ===")

    # keep SPY on the wire for market context, drop everything else
    keep = set(cand_rows) | {MARKET}
    keep_df = spark.createDataFrame([(t,) for t in keep], ["ticker"])

    # ------------------------------------------------------------ stream
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BROKER)
           .option("subscribe", a["topic"])
           .option("startingOffsets", a["starting"])
           .option("maxOffsetsPerTrigger", 200000)
           .load())

    bars = (raw.select(F.from_json(F.col("value").cast("string"),
                                   BAR_SCHEMA).alias("b"))
            .select("b.*")
            .withColumn("ts_et", F.from_utc_timestamp(
                F.timestamp_seconds(F.col("window_start") / 1e9),
                "America/New_York"))
            .withColumn("min_of_day",
                        F.hour("ts_et") * 60 + F.minute("ts_et"))
            .filter((F.col("min_of_day") >= SESSION_START_MIN)
                    & (F.col("min_of_day") < DECISION_MIN)))

    # STREAM-STATIC JOIN: ~9,000 tickers on the wire -> a few hundred here.
    # Doing this before the aggregation keeps streaming state small.
    bars = bars.join(F.broadcast(keep_df), "ticker", "inner")

    # The single permitted stateful aggregation. 30-minute windows align
    # exactly with the batch bin boundaries; the session filter above means
    # the 15:30 window holds 15 minutes, matching the batch stub bin.
    binned = (bars
              .withWatermark("ts_et", "10 minutes")
              .groupBy("ticker", F.window("ts_et", f"{BIN_MINUTES} minutes"))
              .agg(
                  F.min(F.struct("min_of_day", "open"))["open"].alias("b_open"),
                  F.max(F.struct("min_of_day", "close"))["close"].alias("b_close"),
                  F.max("high").alias("b_high"),
                  F.min("low").alias("b_low"),
                  F.sum("volume").alias("b_volume"),
                  F.sum("transactions").alias("b_trades"),
                  F.count("*").alias("b_bars"),
              )
              .withColumn("win_start_min",
                          F.hour(F.col("window.start")) * 60
                          + F.minute(F.col("window.start")))
              .drop("window"))

    handler = make_handler(spark, model, cand_rows, ctx_rows, a["out"], a["day"])

    q = (binned.writeStream
         .outputMode("update")          # append would never fire the last bin
         .foreachBatch(handler)
         .option("checkpointLocation",
                 f"{TEAM}/streaming/ckpt_{a['day']}")
         .trigger(processingTime="5 seconds")
         .start())

    print("\n=== streaming. start the producer now. Ctrl-C to stop. ===\n",
          flush=True)
    try:
        q.awaitTermination()
    except KeyboardInterrupt:
        print(f"\n=== stopping. batches={COUNTS['batches']} "
              f"rows={COUNTS['rows']} official={COUNTS['official']} "
              f"provisional={COUNTS['provisional']} ===")
        q.stop()
    spark.stop()


if __name__ == "__main__":
    main()