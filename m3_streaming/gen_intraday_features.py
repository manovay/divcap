#!/usr/bin/env python3
"""
Stage A -- intraday features from minute bars, as of the 15:45 ET decision.

THIS MODULE IS IMPORTED BY BOTH THE BATCH TRAINER AND THE STREAMING SCORER.
That is its reason for existing. If batch and streaming compute a feature
differently, the model receives a vector from a distribution it was never fit
on, predictions degrade, and NOTHING ERRORS. Every definition lives here once.

DECISION MOMENT
Buy at the ex-1 close, sell at the ex-date open. The decision is therefore made
near the ex-1 close; we fix it at 15:45 ET, fifteen minutes before the bell --
enough to actually place an order. Every feature uses bars from 09:30 to 15:45
on the decision day and nothing else. Pre-market bars (the files carry
04:00-20:00) are dropped: they are thin and would distort every share-based
feature.

BINS
375 minutes = twelve 30-minute bins plus a 15-minute stub = 13. Per bin:
return, volume share, high-low range. All scale-free -- raw volume is not
comparable between a $4T company and a $200M REIT, and a model handed raw
volume learns "big company" and nothing else.

Bin returns are BIN-INTERNAL (close/open - 1), not chained across bins. A
chained return nulls the entire downstream chain when a thin ticker misses one
bin, and thin-ticker gaps are routine. Internal returns degrade to one null.

Volume share uses the session total THROUGH 15:45, not a running total. At the
decision moment the whole session so far is known, so this is not lookahead --
and it makes the batch and streaming definitions trivially identical.

SCALE -- WHY TWO PHASES
~2.5 billion minute bars across 1,255 non-splittable .csv.gz files, so the
scan is 1,255 single-threaded tasks and is by far the dominant cost.

The feature graph forks: the bin table feeds BOTH the session aggregate and
the per-bin pivot, and add_market_context forks it again for SPY. Without an
explicit barrier Spark can re-scan all 2.5B rows once per branch. A first
attempt at the full window ran 56 minutes without reaching the write stage.

So phase 1 writes the bin table (~65M rows) to Parquet and phase 2 reads it
back. The expensive scan happens exactly once, and the two phases are
independently restartable -- rerun with --skip-binned to iterate on features
without re-scanning.

The bin open/close use a struct trick rather than a window function:

    F.min(F.struct("min_of_day", "open"))["open"]

Structs compare lexicographically, so min-by-min_of_day yields the earliest
row's open. Same answer as first()-over-a-window, no shuffle.

Run:
    nohup spark-submit --master yarn --deploy-mode client \
        --driver-memory 8g \
        --num-executors 4 --executor-memory 6g --executor-cores 4 \
        --conf spark.ui.showConsoleProgress=false \
        m3_streaming/gen_intraday_features.py 2021-08 2026-08 \
        > ~/feat.log 2>&1 &

    # dev slice to a separate path
    ... gen_intraday_features.py 2026-05 2026-06 $TEAM/curated/intraday_features_dev

    # re-derive features from an existing bin table, no re-scan
    ... gen_intraday_features.py 2021-08 2026-08 --skip-binned

Use nohup: the driver runs on the login node in client mode, and a dropped
terminal or a second concurrent submit can take it out with no stack trace.

Output: <out>, partitioned by ym. One row per (dividend-paying ticker, trading
day it traded), 50 feature columns. Roughly 4-6M rows; only the ~163K falling
on the trading day before an ex-date are used for training, but the rest must
exist because sess_vol_ratio needs 20 days of trailing history.

Imported by:
    gen_train_capture_model.py  -- build_features, INTRADAY_FEATURES
    stream_scorer.py            -- build_features, N_BINS, DECISION_MIN
"""

import os
import sys
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")

SESSION_START_MIN = 9 * 60 + 30      # 09:30 ET
DECISION_MIN = 15 * 60 + 45          # 15:45 ET -- the decision moment
BIN_MINUTES = 30
N_BINS = 13                          # 12 full + one 15-minute stub
MARKET = "SPY"

SCHEMA = ("ticker string, volume double, open double, close double, "
          "high double, low double, window_start long, transactions long")


# ------------------------------------------------------------------ load

def load_bars(spark, paths):
    """
    Minute (or day) bars with an ET timestamp and minute-of-day.

    window_start is NANOSECONDS since epoch UTC, despite the vendor docs
    saying seconds. Minute bars are stamped at the START of the minute.
    volume is double, not long: the newer files carry fractional volume from
    fractional-share trading, and a long schema silently nulls every row.
    """
    return (spark.read.option("header", True).schema(SCHEMA).csv(paths)
            .withColumn("ts_et", F.from_utc_timestamp(
                F.timestamp_seconds(F.col("window_start") / 1e9),
                "America/New_York"))
            .withColumn("date_et", F.to_date("ts_et"))
            .withColumn("min_of_day",
                        F.hour("ts_et") * 60 + F.minute("ts_et")))


# -------------------------------------------------------------- features

def session_bars(bars):
    """09:30 <= t < 15:45. Everything outside is unknowable or distorting."""
    return bars.filter(
        (F.col("min_of_day") >= SESSION_START_MIN)
        & (F.col("min_of_day") < DECISION_MIN))


def bin_aggregates(bars):
    """
    One row per (ticker, date, bin) with that bin's OHLCV. PHASE 1 -- this is
    the only function that touches the full minute table.

    least(idx, N_BINS-1) is belt-and-braces: session_bars already bounds
    min_of_day, so idx cannot exceed 12, but a future change to DECISION_MIN
    would silently create a 14th bin without it.
    """
    idx = F.floor((F.col("min_of_day") - SESSION_START_MIN) / BIN_MINUTES)

    return (bars
            .withColumn("bin", F.least(idx, F.lit(N_BINS - 1)).cast("int"))
            .groupBy("ticker", "date_et", "bin")
            .agg(
                # struct trick: earliest/latest row in the bin, no window
                F.min(F.struct("min_of_day", "open"))["open"].alias("b_open"),
                F.max(F.struct("min_of_day", "close"))["close"].alias("b_close"),
                F.max("high").alias("b_high"),
                F.min("low").alias("b_low"),
                F.sum("volume").alias("b_volume"),
                F.sum("transactions").alias("b_trades"),
                F.count("*").alias("b_bars"),
            ))


def features_from_binned(binned):
    """
    PHASE 2 -- bin table in, one row per (ticker, date_et) out.

    Reads the small materialized table, so the forks below (session aggregate
    and per-bin pivot both consume `binned`) are cheap re-reads of ~65M rows
    rather than re-scans of 2.5B.
    """
    # Session totals first: the volume-share denominator.
    sess = (binned.groupBy("ticker", "date_et")
            .agg(
                F.min(F.struct("bin", "b_open"))["b_open"].alias("sess_open"),
                F.max(F.struct("bin", "b_close"))["b_close"].alias("sess_close"),
                F.max("b_high").alias("sess_high"),
                F.min("b_low").alias("sess_low"),
                F.sum("b_volume").alias("sess_volume"),
                F.sum("b_trades").alias("sess_trades"),
                F.sum("b_bars").alias("sess_bars"),
                F.count("*").alias("n_bins_present"),
            ))

    # Per-bin scale-free values.
    f = (binned.join(sess.select("ticker", "date_et", "sess_volume"),
                     ["ticker", "date_et"])
         .withColumn("b_ret",
                     F.when(F.col("b_open") > 0,
                            F.col("b_close") / F.col("b_open") - 1.0))
         .withColumn("b_volshare",
                     F.when(F.col("sess_volume") > 0,
                            F.col("b_volume") / F.col("sess_volume"))
                     .otherwise(0.0))
         .withColumn("b_range",
                     F.when(F.col("b_close") > 0,
                            (F.col("b_high") - F.col("b_low"))
                            / F.col("b_close"))
                     .otherwise(0.0)))

    # Pivot to one row per (ticker, date). max(when(...)) is a pivot without
    # the extra shuffle pivot() would introduce.
    aggs = []
    for k in range(N_BINS):
        c = F.col("bin") == k
        aggs.append(F.max(F.when(c, F.col("b_ret"))).alias(f"bin{k}_ret"))
        aggs.append(F.max(F.when(c, F.col("b_volshare")))
                    .alias(f"bin{k}_volshare"))
        aggs.append(F.max(F.when(c, F.col("b_range")))
                    .alias(f"bin{k}_range"))
    # Realized volatility over the day's shape: stdev of the bin returns.
    aggs.append(F.stddev("b_ret").alias("sess_realized_vol"))

    wide = f.groupBy("ticker", "date_et").agg(*aggs)

    return (sess.join(wide, ["ticker", "date_et"], "inner")
            .withColumn("sess_ret",
                        F.when(F.col("sess_open") > 0,
                               F.col("sess_close") / F.col("sess_open") - 1.0))
            .withColumn("sess_range",
                        F.when(F.col("sess_close") > 0,
                               (F.col("sess_high") - F.col("sess_low"))
                               / F.col("sess_close")))
            .withColumn("sess_avg_trade_size",
                        F.when(F.col("sess_trades") > 0,
                               F.col("sess_volume") / F.col("sess_trades"))))


def build_features(bars):
    """
    THE SHARED ENTRY POINT: minute bars in, one row per (ticker, date_et) out.

    The streaming scorer calls this on its accumulated bars; the batch job
    splits the two phases across a Parquet write for scale, but the semantics
    are identical -- which is what keeps training and serving aligned.
    """
    return features_from_binned(bin_aggregates(session_bars(bars)))


def add_market_context(feats):
    """
    SPY's own 09:30-15:45 return, broadcast to every row of the same date.
    If the whole market is down 2%, a name's sess_ret means something
    different. SPY is in the stream, so this is live-computable.
    """
    mkt = (feats.filter(F.col("ticker") == MARKET)
           .select("date_et", F.col("sess_ret").alias("mkt_sess_ret")))
    return feats.join(F.broadcast(mkt), "date_et", "left")


def add_daily_context(feats, day_bars):
    """
    Features needing prior-day history. BATCH ONLY -- a stream has no history,
    so the streaming job reads these from a precomputed lookup instead.

    sess_vol_ratio is probably the most predictive intraday feature we have:
    unusual volume ahead of an ex-date suggests positioning.
    """
    w_prev = Window.partitionBy("ticker").orderBy("date_et")
    w_20 = w_prev.rowsBetween(-20, -1)

    ctx = (day_bars
           .select("ticker", "date_et", "close", "volume")
           .withColumn("prev_close", F.lag("close").over(w_prev))
           .withColumn("avg20_volume", F.avg("volume").over(w_20))
           .select("ticker", "date_et", "prev_close", "avg20_volume"))

    return (feats.join(ctx, ["ticker", "date_et"], "left")
            .withColumn("gap_ret",
                        F.when(F.col("prev_close") > 0,
                               F.col("sess_open") / F.col("prev_close") - 1.0))
            .withColumn("sess_vol_ratio",
                        F.when(F.col("avg20_volume") > 0,
                               F.col("sess_volume") / F.col("avg20_volume")))
            .drop("prev_close", "avg20_volume"))


# ----------------------------------------------------- feature name lists
# Exported so Stage B assembles its vector from the same source of truth
# rather than a hand-copied list that drifts.

BIN_FEATURES = []
for _k in range(N_BINS):
    BIN_FEATURES += [f"bin{_k}_ret", f"bin{_k}_volshare", f"bin{_k}_range"]

SESSION_FEATURES = [
    "sess_ret", "sess_realized_vol", "sess_range", "sess_volume",
    "sess_trades", "sess_avg_trade_size", "sess_bars", "n_bins_present",
]

CONTEXT_FEATURES = ["gap_ret", "sess_vol_ratio", "mkt_sess_ret"]

INTRADAY_FEATURES = BIN_FEATURES + SESSION_FEATURES + CONTEXT_FEATURES


# ------------------------------------------------------------ batch entry

def months_between(start, end):
    ys, ms = start.split("-")
    ye, me = end.split("-")
    y, m, ye, me = int(ys), int(ms), int(ye), int(me)
    out = []
    while (y, m) <= (ye, me):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def main():
    args = []
    skip_binned = False
    for a in sys.argv[1:]:
        if a == "--skip-binned":
            skip_binned = True
        else:
            args.append(a)

    if len(args) > 1:
        start, end = args[0], args[1]
    else:
        start, end = "2021-08", "2026-08"

    if len(args) > 2:
        out = args[2]
    else:
        out = f"{TEAM}/curated/intraday_features"

    binned_path = out + "_binned"

    months = months_between(start, end)
    min_paths = [f"{TEAM}/probe/min_{m}/*.csv.gz" for m in months]
    day_paths = [f"{TEAM}/probe/day_{m}/*.csv.gz" for m in months]

    spark = (SparkSession.builder.appName("divcap-intraday-features")
             .getOrCreate())
    n_parts = max(64, min(600, len(months) * 10))
    spark.conf.set("spark.sql.shuffle.partitions", str(n_parts))

    t0 = time.time()
    print(f"\n=== intraday features: {start}..{end} ({len(months)} months) ===")
    print(f"=== out: {out} ===")
    print(f"=== shuffle partitions: {n_parts} ===")

    # -------------------------------------------------- phase 1: bin table
    if skip_binned:
        print(f"=== SKIPPING phase 1, reusing {binned_path} ===")
    else:
        # Only tickers that ever pay a dividend can produce an event.
        # Filtering before the heavy aggregation is the single biggest win.
        # SPY is in this set (it pays dividends), so market context survives.
        divs = (spark.read.json(f"{TEAM}/reference/dividends_5y.jsonl")
                .select("ticker").distinct())
        print(f"=== dividend-paying tickers: {divs.count()} ===")

        bars = load_bars(spark, min_paths).join(F.broadcast(divs), "ticker")
        (bin_aggregates(session_bars(bars))
         .write.mode("overwrite").parquet(binned_path))
        print(f"=== phase 1 (bin table) done in "
              f"{(time.time()-t0)/60:.1f} min ===")

    t1 = time.time()
    binned = spark.read.parquet(binned_path)

    # -------------------------------------------------- phase 2: features
    feats = features_from_binned(binned)
    feats = add_market_context(feats)
    feats = add_daily_context(feats, load_bars(spark, day_paths))
    feats = feats.withColumn("ym", F.date_format("date_et", "yyyy-MM"))

    feats.write.mode("overwrite").partitionBy("ym").parquet(out)
    print(f"=== phase 2 (features) done in {(time.time()-t1)/60:.1f} min ===")
    print(f"=== total {(time.time()-t0)/60:.1f} min ===")

    # ------------------------------------------ verification on the output
    back = spark.read.parquet(out)
    print(f"=== {back.count()} (ticker, date) rows ===")

    print("=== bin coverage (13 = full session) ===")
    back.groupBy("n_bins_present").count().orderBy("n_bins_present").show(20)

    print("=== null rate on key features ===")
    back.select(
        F.count("*").alias("n"),
        F.sum(F.col("sess_ret").isNull().cast("int")).alias("null_sess_ret"),
        F.sum(F.col("gap_ret").isNull().cast("int")).alias("null_gap"),
        F.sum(F.col("sess_vol_ratio").isNull().cast("int")).alias("null_volratio"),
        F.sum(F.col("mkt_sess_ret").isNull().cast("int")).alias("null_mkt"),
    ).show()

    print("=== sample: liquid names on the replay day ===")
    (back.filter("date_et = '2026-06-12'")
     .orderBy(F.desc("sess_volume"))
     .select("ticker", "sess_ret", "sess_realized_vol", "sess_vol_ratio",
             "gap_ret", "mkt_sess_ret", "n_bins_present", "bin12_volshare")
     .show(10))

    spark.stop()


if __name__ == "__main__":
    main()