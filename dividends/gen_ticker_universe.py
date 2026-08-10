#!/usr/bin/env python3
"""
Build the price-side ticker universe from day aggregates.

Collapses day_aggs to one row per ticker. This is the authoritative list of
securities with SIP activity in the window -- it is what every dividend
event must join against, and what the reference-data pull gets intersected
with later.

Defaults to the single Feb 8 file. Pass a glob to widen the window; the
groupBy collapses across days either way, and n_days tells you how many of
the loaded days each ticker actually traded on.

Run:
    spark-submit --master yarn --deploy-mode client \
        --num-executors 4 --executor-memory 4g --executor-cores 2 \
        dividends/gen_ticker_universe.py

    # or over all five probe days
    spark-submit ... dividends/gen_ticker_universe.py "$TEAM/probe/day_*.csv.gz"

Inputs  (HDFS):  $TEAM/probe/day_2024-02-08.csv.gz   (override via argv[1])
Outputs (HDFS):  $TEAM/curated/ticker_universe       (parquet)
                 $TEAM/curated/ticker_universe_csv   (single csv)
"""

import os
import sys
from pyspark.sql import SparkSession, functions as F

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")

# ticker,volume,open,close,high,low,window_start,transactions
# Header order verified against the raw file. Spark maps an explicit schema
# POSITIONALLY and ignores the header names, so this order matters.
SCHEMA = ("ticker string, volume long, open double, close double, "
          "high double, low double, window_start long, transactions long")


def load_aggs(spark, path):
    """
    Load Massive flat-file aggregates.

    window_start is NANOSECONDS since epoch in UTC, despite the vendor docs
    saying seconds. Day bars are stamped at 00:00 ET, not at the open.
    """
    return (spark.read.option("header", True).schema(SCHEMA).csv(path)
            .withColumn("ts_et", F.from_utc_timestamp(
                F.timestamp_seconds(F.col("window_start") / 1e9),
                "America/New_York"))
            .withColumn("date_et", F.to_date("ts_et")))


def main():
    if len(sys.argv) > 1:
        src = sys.argv[1]
    else:
        src = f"{TEAM}/probe/day_2024-02-08.csv.gz"

    spark = SparkSession.builder.appName("divcap-ticker-universe").getOrCreate()

    day = load_aggs(spark, src).select(
        "ticker", "date_et", "volume", "close", "transactions")

    print(f"\n=== source: {src} ===")
    day.groupBy("date_et").count().orderBy("date_et").show()

    # One row per ticker. n_days is the useful column once this runs over a
    # glob: a ticker present on 5 of 5 days is a different animal from one
    # that showed up once.
    universe = (day.groupBy("ticker")
                .agg(F.countDistinct("date_et").alias("n_days"),
                     F.sum("volume").alias("total_volume"),
                     F.sum("transactions").alias("total_trades"),
                     F.last("close").alias("last_close"))
                .orderBy("ticker")
                .cache())

    n = universe.count()
    print(f"=== {n} distinct tickers ===")
    universe.show(20, truncate=False)

    # Zero-volume names are listed but did not trade. They are in the
    # universe for membership purposes but cannot support a capture trade,
    # so flag the count now rather than discovering it inside a backtest.
    n_zero = universe.filter(F.col("total_volume") == 0).count()
    print(f"=== {n_zero} tickers with zero volume across the window ===")

    out = f"{TEAM}/curated/ticker_universe"
    universe.write.mode("overwrite").parquet(out)
    (universe.coalesce(1).write.mode("overwrite")
     .option("header", True).csv(f"{out}_csv"))
    print(f"wrote {n} rows to {out}")

    spark.stop()


if __name__ == "__main__":
    main()
