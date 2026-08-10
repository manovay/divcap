#!/usr/bin/env python3
"""
Sizes the pseudo-sector deliverable in EVENTS, not tickers.

census_sector_labels.py found 1,837 fillable tickers. That is the wrong
denominator: warrants, units and SPACs do not distribute, so some of that 1,837
carries zero dividend events and contributes nothing to the sector analysis.
This counts the events each bucket actually carries.

The number that decides whether to build the model: events in FILLABLE. If it
is a meaningful share of the total, the pseudo-sector widens sector coverage. If
it is a few hundred, say so in the report and move to the M2 cross-sectional
groupBy instead.

Join notes:
- Ticker is NOT a unique key -- 60 symbols appear twice, once active once
  delisted. The census found zero delisted rows in TRAIN or FILLABLE, so this
  filters to active and asserts one row per ticker rather than fanning out.
- Left join FROM dividends so events with no reference record are visible as a
  NO_METADATA bucket instead of vanishing.

Run (on nyu-dataproc-m):
    spark-submit --master yarn --deploy-mode client \
        --num-executors 2 --executor-memory 4g --executor-cores 2 \
        sector_ml/count_fillable_events.py

Inputs (HDFS): $TEAM/reference/tickers_all_5y_metadata.jsonl
               $TEAM/reference/dividends_5y.jsonl
Output: stdout only.
"""

import os
import sys
from pyspark.sql import SparkSession, functions as F

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
META = f"{TEAM}/reference/tickers_all_5y_metadata.jsonl"
DIVS = f"{TEAM}/reference/dividends_5y.jsonl"

MIN_CHARS = 80  # must match census_sector_labels.py


def main():
    spark = (SparkSession.builder.appName("divcap-fillable-events")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())

    # ---- reference, one row per ticker -----------------------------------
    meta = (spark.read.json(META)
            .withColumn("desc_len", F.length(F.trim(F.coalesce("description", F.lit("")))))
            .withColumn("desc_usable", F.col("desc_len") >= MIN_CHARS)
            .withColumn("has_sic",
                        F.length(F.trim(F.coalesce("sic_description", F.lit("")))) > 0)
            .withColumn("sec_type", F.coalesce("type", F.lit("UNKNOWN")))
            .withColumn("bucket",
                        F.when(F.col("desc_usable") & F.col("has_sic"), "TRAIN")
                         .when(F.col("desc_usable") & ~F.col("has_sic"), "FILLABLE")
                         .when(~F.col("desc_usable") & F.col("has_sic"), "SIC_ONLY")
                         .otherwise("UNREACHABLE")))

    # The census found TRAIN and FILLABLE are 100% active, so dropping the
    # delisted half cannot lose a row from either. Verify, don't assume.
    lost = meta.filter("NOT active AND bucket IN ('TRAIN','FILLABLE')").count()
    if lost:
        sys.exit(f"ABORT: {lost} delisted rows in TRAIN/FILLABLE -- the "
                 f"active-only dedupe would drop them. Revisit the join key.")

    ref = meta.filter("active").select("ticker", "sec_type", "bucket").cache()
    n_ref, n_distinct = ref.count(), ref.select("ticker").distinct().count()
    if n_ref != n_distinct:
        sys.exit(f"ABORT: {n_ref - n_distinct} duplicate tickers among active "
                 f"rows -- the join would fan out and double-count events.")
    print(f"\n=== reference: {n_ref} active tickers, one row each ===")

    # ---- dividends -------------------------------------------------------
    divs = spark.read.json(DIVS).select("ticker", "ex_dividend_date").cache()
    n_divs = divs.count()
    print(f"=== dividend events: {n_divs} (expect 165888) ===")

    joined = (divs.join(ref, on="ticker", how="left")
                  .withColumn("bucket", F.coalesce("bucket", F.lit("NO_METADATA")))
                  .cache())

    print("\n=== EVENTS by bucket -- this is the number that matters ===")
    (joined.groupBy("bucket").agg(
        F.count("*").alias("events"),
        F.countDistinct("ticker").alias("tickers"),
        F.round(100.0 * F.count("*") / n_divs, 2).alias("pct_of_events"),
     ).orderBy(F.desc("events")).show(truncate=False))

    print("=== FILLABLE events by security type -- who actually distributes ===")
    (joined.filter("bucket = 'FILLABLE'")
           .groupBy("sec_type").agg(
               F.count("*").alias("events"),
               F.countDistinct("ticker").alias("tickers"))
           .orderBy(F.desc("events")).show(20, truncate=False))

    print("=== how many FILLABLE tickers carry ZERO events (dead weight) ===")
    fill_total = ref.filter("bucket = 'FILLABLE'").count()
    fill_paying = joined.filter("bucket = 'FILLABLE'").select("ticker").distinct().count()
    print(f"fillable tickers: {fill_total}  |  with >=1 event: {fill_paying}  "
          f"|  zero events: {fill_total - fill_paying}")

    # ---- the headline -----------------------------------------------------
    rows = {r["bucket"]: r["events"] for r in
            joined.groupBy("bucket").agg(F.count("*").alias("events")).collect()}
    train_ev = rows.get("TRAIN", 0)
    fill_ev = rows.get("FILLABLE", 0)
    covered = train_ev + rows.get("SIC_ONLY", 0)
    print(f"\n=== VERDICT ===")
    print(f"events with SIC today            : {covered} "
          f"({100.0 * covered / n_divs:.1f}% of all events)")
    print(f"events the model could label     : {fill_ev} "
          f"(+{100.0 * fill_ev / n_divs:.1f}pp)")
    print(f"events reachable by neither      : "
          f"{rows.get('UNREACHABLE', 0) + rows.get('NO_METADATA', 0)}")
    print("Build the model if the middle line is a meaningful lift. If it is a "
          "few hundred events, report the coverage ceiling and spend the time "
          "on the M2 cross-sectional groupBy.")

    spark.stop()


if __name__ == "__main__":
    main()
