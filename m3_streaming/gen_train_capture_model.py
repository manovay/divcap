#!/usr/bin/env python3
"""
Stage B -- train the dividend-capture classifier and persist it for streaming.

Predicts, at 15:45 ET on the day before an ex-date, whether that capture trade
will clear costs. Trains offline, saves a PipelineModel to HDFS; the streaming
scorer loads it and calls transform(). That offline/online split is how every
production ML system works -- MLlib cannot fit a model on an unbounded stream,
and training on the replayed stream would leak the test period into training.

THE DECISION MOMENT DEFINES THE FEATURE SET
Everything the model sees must be knowable at 15:45 ET on ex-1. That single
rule is enforced by an explicit WHITELIST, never a blacklist -- a blacklist
fails silently the moment someone adds a column.

Two leaks worth naming because they are not obvious:

  div_yield from the grain table is cash_amount / prev_close, and prev_close
  is the ex-1 CLOSE -- which has not happened at 15:45. We recompute
  div_yield_1545 = cash_amount / sess_close using the 15:45 price instead.

  market_cap from the ticker metadata is a CURRENT snapshot, so for a 2022
  event it encodes what the company later became. pre_avg_dollar_volume is
  the correctly-dated size proxy.

Labels may use the future; features may not. The label is built from
capture_ret_abn, which is entirely realized after the decision.

TEMPORAL SPLIT
randomSplit is wrong here: it puts 2025 events in training and 2022 events in
test, so the model learns from the future. Train on <= 2025-12, test on 2026,
which also makes the 2026 replay a genuine out-of-sample demo.

(Contrast sector_ml, where randomSplit IS correct -- sector is a static
company attribute with no time dimension.)

EVALUATION IS P&L, NOT ACCURACY
If half the events are profitable, a model predicting "never trade" is 50%
accurate and earns nothing. The number that matters is the mean realized
return of the events it selects, against the "always trade" baseline.

Run:
    spark-submit --master yarn --deploy-mode client \
        --driver-memory 8g --num-executors 4 --executor-memory 6g \
        --executor-cores 4 --conf spark.ui.showConsoleProgress=false \
        m3_streaming/gen_train_capture_model.py

    # dev slice
    ... gen_train_capture_model.py --features $TEAM/curated/intraday_features_dev \
        --train-end 2026-05-31 --model $TEAM/models/capture_gbt_dev

Outputs:
    <model>            PipelineModel, loaded by stream_scorer.py
    <model>_meta.json  feature list + thresholds, so the scorer cannot drift
"""

import os
import sys
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gen_intraday_features import INTRADAY_FEATURES, N_BINS   # noqa: E402

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")

# --- knobs
COST_BPS = 0.0010        # round-trip cost placeholder; label = abn > this
MIN_YIELD = 0.005        # below this the dividend is smaller than daily noise
TRAIN_END = "2025-12-31"
MARKET = "SPY"

# --- features carried from the event grain (all knowable at 15:45 on ex-1)
EVENT_FEATURES = [
    "div_yield_1545",           # recomputed against the 15:45 price
    "cash_amount",
    "frequency",
    "n_distributions",
    "pre_vol",
    "pre_avg_ret",
    "pre_avg_abn_ret",
    "pre_avg_dollar_volume",
    "days_decl_to_ex",
]

CATEGORICAL = ["sec_type", "sic_description"]

# --- anything on this list in the feature vector is a bug, not a choice
BANNED = {
    "ex_open", "ex_close", "post_close", "prev_close",
    "drop_ratio", "drop_pct", "capture_ret", "capture_ret_abn",
    "hold_ret", "post_avg_ret", "post_avg_abn_ret",
    "mkt_overnight_ret", "div_yield", "market_cap", "label",
}


def validate_no_leakage(cols):
    """Returns the offending columns. Empty list means clean."""
    bad = []
    for c in cols:
        if c in BANNED:
            bad.append(c)
    return bad


def parse_args(argv):
    a = {
        "features": f"{TEAM}/curated/intraday_features",
        "model": f"{TEAM}/models/capture_gbt",
        "train_end": TRAIN_END,
        "min_yield": MIN_YIELD,
        "cost": COST_BPS,
    }
    i = 0
    while i < len(argv):
        k = argv[i]
        if k == "--features":
            a["features"] = argv[i + 1]; i += 2
        elif k == "--model":
            a["model"] = argv[i + 1]; i += 2
        elif k == "--train-end":
            a["train_end"] = argv[i + 1]; i += 2
        elif k == "--min-yield":
            a["min_yield"] = float(argv[i + 1]); i += 2
        elif k == "--cost":
            a["cost"] = float(argv[i + 1]); i += 2
        else:
            sys.exit(f"unknown arg: {k}")
    return a


def build_dataset(spark, feat_path, min_yield):
    """
    One row per event, with the intraday features of its DECISION DAY.

    The decision day is the trading day before the ex-date. Rather than
    recompute a trading calendar, we reuse the M1 event panel: offset == -1
    is by construction the previous trading day, already correct for weekends
    and holidays.
    """
    grain = spark.read.parquet(f"{TEAM}/curated/div_event_grain")
    panel = spark.read.parquet(f"{TEAM}/curated/div_event_panel")
    feats = spark.read.parquet(feat_path)
    meta = spark.read.json(f"{TEAM}/reference/tickers_all_5y_metadata.jsonl")

    # decision day per event
    dec = (panel.filter(F.col("offset") == -1)
           .select("ticker", "ex_date",
                   F.col("bar_date").alias("decision_date")))

    ev = (grain
          .filter(F.col("has_core") & F.col("window_contiguous"))
          .filter(F.col("ticker") != MARKET)      # circular: SPY is the proxy
          .filter(F.col("div_yield") >= min_yield)
          .join(dec, ["ticker", "ex_date"], "inner"))

    # intraday features of the decision day
    ev = ev.join(feats.withColumnRenamed("date_et", "decision_date"),
                 ["ticker", "decision_date"], "inner")

    # Ticker attributes.
    #
    # NOTE: tickers_all_5y_metadata.jsonl does NOT carry primary_exchange or
    # delisted_utc -- gen_ticker_metadata.py's KEEP list dropped them, and
    # they survive only in the raw tickers_all_5y.jsonl. So the date-aware
    # resolution used in M1 is not possible here; instead we prefer the
    # ACTIVE record for the ~60 recycled symbols. On 163K events that is a
    # rounding error, and it is honest about what the data supports.
    #
    # sic_description is ~45% populated (SIC classifies operating companies,
    # so ETFs and funds have none). UNKNOWN is a real category, not a gap --
    # it is close to a proxy for "is a fund".
    w_meta = Window.partitionBy("m_ticker").orderBy(F.col("active").desc())
    m = (meta
         .select(F.col("ticker").alias("m_ticker"),
                 F.col("type").alias("sec_type"),
                 "sic_description",
                 "active")
         .withColumn("_rn", F.row_number().over(w_meta))
         .filter(F.col("_rn") == 1)
         .drop("_rn", "active")
         .withColumn("sec_type", F.coalesce("sec_type", F.lit("UNKNOWN")))
         .withColumn("sic_description",
                     F.coalesce("sic_description", F.lit("UNKNOWN"))))

    ev = (ev.join(F.broadcast(m), ev["ticker"] == m["m_ticker"], "left")
          .withColumn("sec_type", F.coalesce("sec_type", F.lit("UNKNOWN")))
          .withColumn("sic_description",
                      F.coalesce("sic_description", F.lit("UNKNOWN")))
          .drop("m_ticker"))

    # --- derived features, and the label
    ev = (ev
          # 15:45 price, not the close: the close has not happened yet
          .withColumn("div_yield_1545",
                      F.when(F.col("sess_close") > 0,
                             F.col("cash_amount") / F.col("sess_close")))
          .withColumn("days_decl_to_ex",
                      F.datediff("ex_date", F.to_date("declaration_date")))
          .withColumn("label",
                      F.when(F.col("capture_ret_abn") > F.lit(COST_BPS), 1.0)
                      .otherwise(0.0))
          .filter(F.col("capture_ret_abn").isNotNull())
          .filter(F.col("div_yield_1545").isNotNull()))

    # --- imputation. Tree models in MLlib do not accept NaN in the vector.
    # A missing bin means no trade in that half hour, so 0 is the meaningful
    # fill for return, share and range alike. n_bins_present already tells
    # the model how much of the session was actually observed.
    zero_fill = {}
    for k in range(N_BINS):
        zero_fill[f"bin{k}_ret"] = 0.0
        zero_fill[f"bin{k}_volshare"] = 0.0
        zero_fill[f"bin{k}_range"] = 0.0
    zero_fill.update({
        "gap_ret": 0.0,             # neutral: no overnight move
        "sess_vol_ratio": 1.0,      # neutral: volume equals its own norm
        "mkt_sess_ret": 0.0,
        "sess_realized_vol": 0.0,
        "pre_vol": 0.0,
        "pre_avg_ret": 0.0,
        "pre_avg_abn_ret": 0.0,
        "days_decl_to_ex": -1.0,
        "n_distributions": 1.0,
        "frequency": 0.0,
    })
    ev = ev.fillna(zero_fill)

    return ev


def summarize(df, label):
    """P&L of a selected subset. The only evaluation that means anything."""
    r = df.select(
        F.count("*").alias("n"),
        F.mean("capture_ret_abn").alias("mean_abn"),
        F.expr("percentile_approx(capture_ret_abn, 0.5)").alias("med_abn"),
        F.mean(F.when(F.col("capture_ret_abn") > 0, 1.0).otherwise(0.0))
         .alias("win_rate"),
    ).collect()[0]
    return {
        "cut": label,
        "n": r["n"],
        "mean_bps": (r["mean_abn"] or 0) * 1e4,
        "med_bps": (r["med_abn"] or 0) * 1e4,
        "win_rate": r["win_rate"] or 0,
    }


def main():
    a = parse_args(sys.argv[1:])

    spark = (SparkSession.builder.appName("divcap-train-capture")
             .getOrCreate())
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    print(f"\n=== features: {a['features']} ===")
    print(f"=== model out: {a['model']} ===")
    print(f"=== train <= {a['train_end']}, test after ===")
    print(f"=== min_yield {a['min_yield']}, cost {a['cost']*1e4:.0f} bps ===")

    ev = build_dataset(spark, a["features"], a["min_yield"]).cache()
    n = ev.count()
    print(f"\n=== {n} events with a decision-day feature row ===")

    if n == 0:
        sys.exit("no events -- check that the feature window covers the "
                 "event window")

    # ---------------------------------------------------- feature vector
    numeric = EVENT_FEATURES + INTRADAY_FEATURES
    bad = validate_no_leakage(numeric + CATEGORICAL)
    if bad:
        sys.exit(f"LEAKAGE: banned columns in the feature set: {bad}")
    print(f"=== {len(numeric)} numeric + {len(CATEGORICAL)} categorical "
          f"features, leakage check passed ===")

    missing = []
    for c in numeric + CATEGORICAL:
        if c not in ev.columns:
            missing.append(c)
    if missing:
        sys.exit(f"missing columns: {missing}")

    # ---------------------------------------------------- temporal split
    train = ev.filter(F.col("ex_date") <= a["train_end"])
    test = ev.filter(F.col("ex_date") > a["train_end"])
    n_tr, n_te = train.count(), test.count()
    print(f"=== train {n_tr}, test {n_te} ===")
    if n_tr == 0 or n_te == 0:
        sys.exit("empty split -- adjust --train-end")

    print("=== label balance ===")
    train.groupBy("label").count().orderBy("label").show()

    # ---------------------------------------------------------- pipeline
    stages = []
    enc_in, enc_out = [], []
    for c in CATEGORICAL:
        # handleInvalid="keep": the test set will contain categories absent
        # from training, and the default throws rather than degrading.
        stages.append(StringIndexer(inputCol=c, outputCol=c + "_idx",
                                    handleInvalid="keep"))
        enc_in.append(c + "_idx")
        enc_out.append(c + "_oh")
    stages.append(OneHotEncoder(inputCols=enc_in, outputCols=enc_out,
                                handleInvalid="keep"))
    stages.append(VectorAssembler(inputCols=numeric + enc_out,
                                  outputCol="features",
                                  handleInvalid="skip"))

    gbt = GBTClassifier(labelCol="label", featuresCol="features",
                        maxIter=60, maxDepth=5, stepSize=0.1, seed=42)

    model = Pipeline(stages=stages + [gbt]).fit(train)
    print("=== GBT trained ===")

    # baseline: if a linear model matches the GBT, that is worth knowing
    lr = LogisticRegression(labelCol="label", featuresCol="features",
                            maxIter=50)
    lr_model = Pipeline(stages=stages + [lr]).fit(train)

    # -------------------------------------------------------- evaluation
    ev_auc = BinaryClassificationEvaluator(labelCol="label",
                                           metricName="areaUnderROC")

    rows = []
    for name, mdl in [("GBT", model), ("LogReg", lr_model)]:
        pred = mdl.transform(test).cache()
        auc = ev_auc.evaluate(pred)
        sel = pred.filter(F.col("prediction") == 1.0)
        s = summarize(sel, f"{name} selected")
        s["auc"] = auc
        rows.append(s)
        pred.unpersist()

    base = summarize(test, "ALWAYS TRADE (baseline)")
    base["auc"] = float("nan")
    rows.append(base)

    print("\n=== out-of-sample P&L ===")
    print(f"{'cut':26s} {'n':>7s} {'mean bps':>9s} {'med bps':>9s} "
          f"{'win%':>6s} {'AUC':>6s}")
    for r in rows:
        print(f"{r['cut']:26s} {r['n']:7d} {r['mean_bps']:9.2f} "
              f"{r['med_bps']:9.2f} {100*r['win_rate']:6.1f} {r['auc']:6.3f}")

    print("\nThe comparison that matters is 'GBT selected' vs 'ALWAYS TRADE'.")
    print("A higher mean on a reasonable count is a real result; a higher")
    print("mean on a handful of events is overfitting.")

    # ------------------------------------------------- feature importance
    gbt_model = model.stages[-1]
    names = numeric + enc_out
    imps = list(gbt_model.featureImportances.toArray())
    pairs = sorted(zip(imps, names), reverse=True)[:25]
    print("\n=== top 25 features ===")
    for v, nm in pairs:
        print(f"  {v:7.4f}  {nm}")

    # -------------------------------------------------------------- save
    model.write().overwrite().save(a["model"])
    print(f"\n=== saved model to {a['model']} ===")

    # The scorer reads this instead of re-declaring the contract, so the two
    # cannot drift apart.
    meta = {
        "numeric_features": numeric,
        "categorical_features": CATEGORICAL,
        "cost_bps": a["cost"],
        "min_yield": a["min_yield"],
        "train_end": a["train_end"],
        "n_train": n_tr,
        "n_test": n_te,
        "features_path": a["features"],
    }
    meta_path = a["model"] + "_meta.json"
    (spark.createDataFrame([(json.dumps(meta),)], ["meta"])
     .coalesce(1).write.mode("overwrite").text(meta_path))
    print(f"=== saved metadata to {meta_path} ===")

    spark.stop()


if __name__ == "__main__":
    main()