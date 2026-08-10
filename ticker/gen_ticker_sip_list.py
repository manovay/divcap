#!/usr/bin/env python3
"""
Build the SIP ticker list from day aggregates.

Collapses day_aggs to the distinct set of tickers with SIP activity in the
given month(s). This is the price-side universe: every dividend event must
join against it, and the reference-data pull gets intersected with it.

Output is one ticker per line, no header, no other columns.

Run:
    # default: 2026-08
    spark-submit --master yarn --deploy-mode client \
        --num-executors 4 --executor-memory 4g --executor-cores 2 \
        gen_ticker_sip_list.py

    # one or more months, as YYYY-MM
    spark-submit ... gen_ticker_sip_list.py 2026-07 2026-08

Inputs  (HDFS):  $TEAM/probe/day_<YYYY-MM>/*.csv.gz
Outputs (HDFS):  $TEAM/curated/ticker_sip_list       (single csv, no header)
"""

import os
import sys
from pyspark.sql import SparkSession, functions as F

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")

# ticker,volume,open,close,high,low,window_start,transactions
# Header order verified against the raw files. Spark maps an explicit schema
# POSITIONALLY and ignores the header names, so this order matters.
#
# volume is double, not long: the 2026 files carry fractional volume
# (fractional-share trading), and a long schema would silently null every
# row of it under Spark's default PERMISSIVE parsing.
SCHEMA = ("ticker string, volume double, open double, close double, "
          "high double, low double, window_start long, transactions long")


def load_aggs(spark, paths):
    """
    Load Massive flat-file aggregates.

    window_start is NANOSECONDS since epoch in UTC, despite the vendor docs
    saying seconds. Day bars are stamped at 00:00 ET, not at the open, and
    from_utc_timestamp handles the EST/EDT shift across months.
    """
    return (spark.read.option("header", True).schema(SCHEMA).csv(paths)
            .withColumn("ts_et", F.from_utc_timestamp(
                F.timestamp_seconds(F.col("window_start") / 1e9),
                "America/New_York"))
            .withColumn("date_et", F.to_date("ts_et")))


def main():
    if len(sys.argv) > 1:
        months = sys.argv[1:]
    else:
        months = ["2026-08"]

    paths = []
    for m in months:
        paths.append(f"{TEAM}/probe/day_{m}/*.csv.gz")

    spark = SparkSession.builder.appName("divcap-ticker-sip-list").getOrCreate()

    day = load_aggs(spark, paths).select("ticker", "date_et")

    # Diagnostic only -- confirms which trading days actually loaded, and
    # that the timezone conversion did not bleed dates across month edges.
    print(f"\n=== months: {months} ===")
    day.groupBy("date_et").count().orderBy("date_et").show(50)

    tickers = day.select("ticker").distinct().cache()
    n = tickers.count()
    print(f"=== {n} distinct tickers ===")

    # coalesce(1) then sort within the single partition: gives one sorted
    # output file without a global sort, which would fan out to
    # spark.sql.shuffle.partitions (1000 on this cluster) part files.
    out = f"{TEAM}/curated/ticker_sip_list"
    (tickers.coalesce(1).sortWithinPartitions("ticker")
     .write.mode("overwrite").option("header", False).csv(out))
    print(f"wrote {n} tickers to {out}")

    spark.stop()


if __name__ == "__main__":
    main()