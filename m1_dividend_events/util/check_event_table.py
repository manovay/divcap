#!/usr/bin/env python3
"""
Verify the dividend event tables.

Structural checks (counts reconcile, no duplicate keys, flags consistent)
plus the distributional cuts that decide whether the headline number is
real or an artifact.

Run:
    spark-submit --master yarn --deploy-mode client \
        --num-executors 4 --executor-memory 4g --executor-cores 2 \
        check_event_table.py
"""

import os
from pyspark.sql import SparkSession, functions as F

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
MARKET = "SPY"


def stats(df, label):
    """Mean AND median for both metrics -- the mean alone hides tail effects."""
    return df.select(
        F.lit(label).alias("cut"),
        F.count("*").alias("n"),
        F.expr("percentile_approx(drop_ratio, 0.5)").alias("med_ratio"),
        F.mean("capture_ret_abn").alias("mean_abn"),
        F.expr("percentile_approx(capture_ret_abn, 0.5)").alias("med_abn"),
        F.mean(F.when(F.col("capture_ret_abn") > 0, 1.0)
               .otherwise(0.0)).alias("win_rate"),
    )


def main():
    spark = SparkSession.builder.appName("divcap-check-events").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "64")

    grain = spark.read.parquet(f"{TEAM}/curated/div_event_grain").cache()
    panel = spark.read.parquet(f"{TEAM}/curated/div_event_panel")

    n = grain.count()
    print(f"\n=== grain {n} rows, panel {panel.count()} rows ===")
    print(f"=== panel/grain ratio {panel.count() / n:.2f} (expect 9.00) ===\n")

    # --- structural ---------------------------------------------------
    dupes = (grain.groupBy("ticker", "ex_date").count()
             .filter("count > 1").count())
    print(f"duplicate (ticker, ex_date) keys: {dupes}   <- must be 0")

    bad_flag = grain.filter(
        F.col("has_core") & (F.col("prev_close").isNull()
                             | F.col("ex_open").isNull())).count()
    print(f"has_core true but a core price null: {bad_flag}   <- must be 0")

    neg_price = grain.filter("prev_close <= 0 OR ex_open <= 0").count()
    print(f"non-positive prices: {neg_price}   <- must be 0\n")

    # --- coverage over time -------------------------------------------
    print("=== events per year (watch for a thin first/last year) ===")
    (grain.withColumn("yr", F.year("ex_date"))
     .groupBy("yr")
     .agg(F.count("*").alias("events"),
          F.sum(F.col("has_core").cast("int")).alias("has_core"),
          F.sum(F.col("window_complete").cast("int")).alias("complete"))
     .orderBy("yr").show())

    core = grain.filter(F.col("has_core") & F.col("window_contiguous"))

    # --- the SPY circularity -------------------------------------------
    # capture_ret_abn subtracts SPY's overnight move. For SPY's OWN events
    # that subtraction cancels the price change exactly, leaving the yield --
    # so SPY posts a guaranteed "profit" that is pure construction.
    spy = core.filter(F.col("ticker") == MARKET)
    print(f"=== {MARKET} events in the table: {spy.count()} "
          f"(circular -- exclude) ===")
    spy.select("ex_date", "div_yield", "capture_ret",
               "capture_ret_abn").orderBy("ex_date").show(5)

    core = core.filter(F.col("ticker") != MARKET)

    # --- headline, sliced ----------------------------------------------
    print("=== headline cuts ===")
    cuts = stats(core, "all")
    cuts = cuts.union(stats(core.filter("NOT drop_ratio_extreme"), "no extreme"))
    cuts = cuts.union(stats(core.filter("div_yield >= 0.005"), "yield >= 0.5%"))
    cuts = cuts.union(stats(core.filter("div_yield >= 0.01"), "yield >= 1%"))
    cuts = cuts.union(stats(core.filter("window_complete"), "complete window"))
    cuts.show(truncate=False)

    # --- yield buckets --------------------------------------------------
    # If the anomaly is real it should strengthen with yield: a bigger
    # dividend means a bigger signal relative to the same daily noise.
    print("=== by yield bucket ===")
    (core.withColumn("bucket",
                     F.when(F.col("div_yield") < 0.0025, "1 <0.25%")
                     .when(F.col("div_yield") < 0.005, "2 0.25-0.5%")
                     .when(F.col("div_yield") < 0.01, "3 0.5-1%")
                     .when(F.col("div_yield") < 0.02, "4 1-2%")
                     .otherwise("5 >=2%"))
     .groupBy("bucket")
     .agg(F.count("*").alias("n"),
          F.expr("percentile_approx(drop_ratio, 0.5)").alias("med_ratio"),
          F.mean("capture_ret_abn").alias("mean_abn"),
          F.expr("percentile_approx(capture_ret_abn, 0.5)").alias("med_abn"))
     .orderBy("bucket").show(truncate=False))

    # --- year by year ---------------------------------------------------
    # A result driven by one year is not a result.
    print("=== by year ===")
    (core.withColumn("yr", F.year("ex_date"))
     .groupBy("yr")
     .agg(F.count("*").alias("n"),
          F.expr("percentile_approx(drop_ratio, 0.5)").alias("med_ratio"),
          F.mean("capture_ret_abn").alias("mean_abn"),
          F.expr("percentile_approx(capture_ret_abn, 0.5)").alias("med_abn"))
     .orderBy("yr").show())

    # --- time-decay curve ------------------------------------------------
    # The panel's reason for existing. Flat pre-drift, a jump at offset 0,
    # partial reversion after = a real anomaly. Drift BEFORE the ex-date
    # means the market prices it in early.
    print("=== mean abnormal return by offset (time-decay curve) ===")
    (panel.filter(F.col("ticker") != MARKET)
     .groupBy("offset")
     .agg(F.count("ret_cc").alias("n"),
          F.mean("ret_cc").alias("mean_ret"),
          F.mean("abn_ret_cc").alias("mean_abn_ret"))
     .orderBy("offset").show())

    spark.stop()


if __name__ == "__main__":
    main()