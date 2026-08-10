#!/usr/bin/env python3
"""
Build the dividend-event tables: an (event x trading-day-offset) panel, and a
collapsed one-row-per-event grain table.

    panel   one row per (ticker, ex_date, offset) for offset in [-5, +3]
    grain   one row per (ticker, ex_date), metrics computed off the panel

WHY A GLOBAL TRADING CALENDAR
Offsets are TRADING days, not calendar days. The naive approach --
lag(close) over partitionBy(ticker).orderBy(date) -- takes the previous row
PRESENT FOR THAT TICKER, so an illiquid name that did not trade for a week
silently yields a "previous close" from a week earlier and nothing errors.

Instead we build one market-wide calendar from the distinct dates in the day
aggregates, index it densely, and join prices on (ticker, calendar_date) with
a LEFT join. A missing bar then surfaces as a null we can count, not as a
wrong number we cannot see.

WHY THE CONTIGUITY FLAG
The calendar is built from whatever months are loaded. If those months are
not adjacent -- e.g. day_2024-02 and day_2026-07 sitting in probe/ together
-- consecutive calendar indices can straddle a two-year gap, and offset -1
from 2026-07-01 would resolve to 2024-02-13. Every panel row therefore
carries the true calendar distance from the ex-date, and the grain table
flags any event whose window spans more than MAX_SPAN_DAYS.

UNADJUSTED PRICES
Flat-file prices are NOT adjusted for splits, so use the raw `cash_amount`
from the dividends feed, never `split_adjusted_cash_amount` -- the dividend
must be in the same share basis as the price. A split inside the window still
corrupts the ratio; splits are not pulled yet (see README_DIVIDEND.md), so
`drop_ratio_extreme` is a heuristic flag for that until they are.

Run:
    # every month present under probe/
    spark-submit --master yarn --deploy-mode client \
        --num-executors 4 --executor-memory 4g --executor-cores 2 \
        dividends/gen_event_tables.py

    # explicit months
    spark-submit ... dividends/gen_event_tables.py 2026-07 2026-08

Inputs  (HDFS):  $TEAM/probe/day_<YYYY-MM>/*.csv.gz
                 $TEAM/reference/dividends_5y.jsonl
                 $TEAM/reference/tickers_all_5y_metadata.jsonl
Outputs (HDFS):  $TEAM/curated/div_event_panel   (parquet, partitioned by ex_ym)
                 $TEAM/curated/div_event_grain   (parquet)
                 $TEAM/curated/div_event_grain_csv
"""

import os
import sys
import subprocess
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")

PRE = 5          # trading days before the ex-date
POST = 3         # trading days after
MAX_SPAN_DAYS = 30   # calendar days across the whole window before we distrust it
MARKET = "SPY"       # market proxy for abnormal returns

# ticker,volume,open,close,high,low,window_start,transactions
# Spark maps an explicit schema POSITIONALLY and ignores header names, so
# order matters. volume is double, not long: the 2026 files carry fractional
# volume and a long schema silently nulls every row of it.
SCHEMA = ("ticker string, volume double, open double, close double, "
          "high double, low double, window_start long, transactions long")


def discover_months():
    """Every day_<YYYY-MM> directory under probe/, via the hdfs CLI."""
    p = subprocess.run(["hdfs", "dfs", "-ls", f"{TEAM}/probe"],
                       capture_output=True, text=True)
    months = []
    for line in p.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        path = parts[-1]
        name = path.rstrip("/").split("/")[-1]
        if name.startswith("day_") and len(name) == len("day_YYYY-MM"):
            months.append(name[len("day_"):])
    months.sort()
    return months


def load_aggs(spark, paths):
    """
    Load Massive flat-file day aggregates.

    window_start is NANOSECONDS since epoch in UTC, despite the vendor docs
    saying seconds. Day bars are stamped at 00:00 ET, not at the open, and
    from_utc_timestamp handles the EST/EDT shift.
    """
    return (spark.read.option("header", True).schema(SCHEMA).csv(paths)
            .withColumn("ts_et", F.from_utc_timestamp(
                F.timestamp_seconds(F.col("window_start") / 1e9),
                "America/New_York"))
            .withColumn("date_et", F.to_date("ts_et"))
            .select("ticker", "date_et", "open", "close", "high", "low",
                    "volume", "transactions"))


def build_calendar(day):
    """
    Market-wide trading calendar with a dense index. Built from the data
    itself rather than a holiday table, so it is correct by construction for
    whatever months are loaded.
    """
    w = Window.orderBy("date_et")
    return (day.select("date_et").distinct()
            .withColumn("day_idx", F.row_number().over(w) - 1))


def load_dividends(spark):
    """
    One row per (ticker, ex_date). Multiple distributions can share an
    ex-date -- a regular plus a special, or a corrected record -- and the
    price drop on that date reflects ALL of them, so cash_amount is summed
    rather than deduplicated away. n_distributions preserves the fact.
    """
    d = (spark.read.json(f"{TEAM}/reference/dividends_5y.jsonl")
         .withColumn("ex_date", F.to_date("ex_dividend_date"))
         .filter(F.col("cash_amount") > 0)
         .filter(F.col("ex_date").isNotNull()))

    return (d.groupBy("ticker", "ex_date")
            .agg(F.sum("cash_amount").alias("cash_amount"),
                 F.count("*").alias("n_distributions"),
                 F.min("frequency").alias("frequency"),
                 F.sort_array(F.collect_set("distribution_type"))
                  .alias("distribution_types"),
                 F.min("pay_date").alias("pay_date"),
                 F.min("record_date").alias("record_date"),
                 F.min("declaration_date").alias("declaration_date")))


def load_metadata(spark):
    """
    Ticker attributes. Symbol reuse means a ticker can appear twice -- once
    delisted under its former holder, once active under the current one --
    so this returns BOTH rows and the caller resolves by date. Returning one
    row per ticker here would attach the wrong company's sector.
    """
    return (spark.read.json(f"{TEAM}/reference/tickers_all_5y_metadata.jsonl")
            .select(F.col("ticker").alias("m_ticker"),
                    F.col("type").alias("sec_type"),
                    "primary_exchange",
                    F.col("active").alias("m_active"),
                    "sic_description",
                    "market_cap",
                    F.to_date("delisted_utc").alias("delisted_date"),
                    F.to_date("list_date").alias("list_date")))


def attach_metadata(ev, meta):
    """
    Date-aware join. Keep candidate rows valid as of the ex-date, then prefer
    the delisted record when the event predates the delisting, since that is
    the company that actually paid the dividend.
    """
    j = (ev.join(F.broadcast(meta),
                 ev["ticker"] == meta["m_ticker"], "left")
         .filter(F.col("m_ticker").isNull()
                 | F.col("delisted_date").isNull()
                 | (F.col("ex_date") <= F.col("delisted_date"))))

    w = (Window.partitionBy("ticker", "ex_date")
         .orderBy(F.col("delisted_date").asc_nulls_last()))
    return (j.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "m_ticker"))


def main():
    if len(sys.argv) > 1:
        months = sys.argv[1:]
    else:
        months = discover_months()

    if not months:
        sys.exit("no day_<YYYY-MM> directories found under probe/")

    paths = []
    for m in months:
        paths.append(f"{TEAM}/probe/day_{m}/*.csv.gz")

    spark = (SparkSession.builder.appName("divcap-event-tables")
             .getOrCreate())
    # Default is 1000 on this cluster, which fragments small outputs into
    # 1000 tiny files. Sized for the probe; raise it for the full history.
    spark.conf.set("spark.sql.shuffle.partitions", "64")

    print(f"\n=== months: {months} ===")

    day = load_aggs(spark, paths).cache()
    cal = build_calendar(day).cache()

    n_days = cal.count()
    bounds = cal.agg(F.min("date_et").alias("lo"),
                     F.max("date_et").alias("hi")).collect()[0]
    print(f"=== calendar: {n_days} trading days, "
          f"{bounds['lo']} .. {bounds['hi']} ===")

    # ---------------------------------------------------------- events
    divs = load_dividends(spark)
    n_divs = divs.count()

    # An event is in scope only if its ex-date is itself a trading day in the
    # loaded calendar. Everything else is out of window, not an error.
    ev = (divs.join(cal, divs["ex_date"] == cal["date_et"], "inner")
          .drop("date_et")
          .withColumnRenamed("day_idx", "ex_idx"))
    n_ev = ev.cache().count()
    print(f"=== {n_divs} dividend events total, {n_ev} with an ex-date "
          f"inside the loaded calendar ===")

    if n_ev == 0:
        print("no events in window -- nothing to build")
        spark.stop()
        return

    # ----------------------------------------------------------- panel
    offsets = []
    for i in range(-PRE, POST + 1):
        offsets.append(F.lit(i))

    panel = (ev.withColumn("offset", F.explode(F.array(*offsets)))
             .withColumn("target_idx", F.col("ex_idx") + F.col("offset")))

    cal2 = (cal.withColumnRenamed("date_et", "bar_date")
            .withColumnRenamed("day_idx", "target_idx"))

    panel = (panel.join(cal2, "target_idx", "left")
             .join(day.withColumnRenamed("date_et", "bar_date"),
                   ["ticker", "bar_date"], "left")
             .withColumn("span_days",
                         F.abs(F.datediff(F.col("bar_date"),
                                          F.col("ex_date")))))

    # Market proxy, joined by date so every event on a given day shares it.
    mkt = (day.filter(F.col("ticker") == MARKET)
           .select(F.col("date_et").alias("bar_date"),
                   F.col("open").alias("mkt_open"),
                   F.col("close").alias("mkt_close")))
    panel = panel.join(F.broadcast(mkt), "bar_date", "left")

    # Close-to-close return per bar, within the event window. The lag is over
    # the panel's own offset ordering, so it is a true trading-day step.
    wo = Window.partitionBy("ticker", "ex_date").orderBy("offset")
    panel = (panel
             .withColumn("prev_bar_close", F.lag("close").over(wo))
             .withColumn("prev_mkt_close", F.lag("mkt_close").over(wo))
             .withColumn("ret_cc",
                         F.col("close") / F.col("prev_bar_close") - 1.0)
             .withColumn("mkt_ret_cc",
                         F.col("mkt_close") / F.col("prev_mkt_close") - 1.0)
             .withColumn("abn_ret_cc",
                         F.col("ret_cc") - F.col("mkt_ret_cc"))
             .withColumn("dollar_volume", F.col("close") * F.col("volume"))
             .withColumn("ex_ym", F.date_format("ex_date", "yyyy-MM")))

    panel = panel.cache()
    n_panel = panel.count()
    print(f"=== panel: {n_panel} rows "
          f"({PRE + POST + 1} offsets x {n_ev} events) ===")

    n_bars = panel.filter(F.col("close").isNotNull()).count()
    print(f"=== {n_bars} panel rows have a price bar "
          f"({100.0 * n_bars / n_panel:.1f}%) ===")

    out_panel = f"{TEAM}/curated/div_event_panel"
    (panel.write.mode("overwrite").partitionBy("ex_ym").parquet(out_panel))
    print(f"wrote panel to {out_panel}")

    # ----------------------------------------------------------- grain
    def at(off, col):
        """Value of `col` on the bar at a specific offset."""
        return F.max(F.when(F.col("offset") == off, F.col(col)))

    def avg_between(lo, hi, col):
        return F.avg(F.when(F.col("offset").between(lo, hi), F.col(col)))

    def std_between(lo, hi, col):
        return F.stddev(F.when(F.col("offset").between(lo, hi), F.col(col)))

    grain = panel.groupBy(
        "ticker", "ex_date", "ex_ym", "cash_amount", "n_distributions",
        "frequency", "distribution_types", "pay_date", "record_date",
        "declaration_date").agg(

        # --- the two prices the strategy actually transacts at
        at(-1, "close").alias("prev_close"),
        at(0, "open").alias("ex_open"),
        at(0, "close").alias("ex_close"),

        # --- pre-window drift, volatility, liquidity
        # ret_cc at offset -PRE is null (no prior bar in the window), so the
        # pre window spans -PRE+1 .. -1: PRE bars give PRE-1 daily changes.
        avg_between(-PRE + 1, -1, "ret_cc").alias("pre_avg_ret"),
        avg_between(-PRE + 1, -1, "abn_ret_cc").alias("pre_avg_abn_ret"),
        std_between(-PRE + 1, -1, "ret_cc").alias("pre_vol"),
        avg_between(-PRE, -1, "volume").alias("pre_avg_volume"),
        avg_between(-PRE, -1, "dollar_volume").alias("pre_avg_dollar_volume"),

        # --- post-window reversion: does the drop stick?
        avg_between(1, POST, "ret_cc").alias("post_avg_ret"),
        avg_between(1, POST, "abn_ret_cc").alias("post_avg_abn_ret"),
        at(POST, "close").alias("post_close"),

        # --- market proxy around the ex-date
        at(-1, "mkt_close").alias("mkt_prev_close"),
        at(0, "mkt_open").alias("mkt_ex_open"),

        # --- coverage: how much of the window actually had bars
        F.count("close").alias("n_bars"),
        F.sum(F.when(F.col("offset").between(-PRE, -1)
                     & F.col("close").isNotNull(), 1).otherwise(0))
         .alias("n_bars_pre"),
        F.sum(F.when(F.col("offset").between(1, POST)
                     & F.col("close").isNotNull(), 1).otherwise(0))
         .alias("n_bars_post"),
        F.max("span_days").alias("max_span_days"),
    )

    grain = (grain
             # The measurement. Theory says 1.0; the literature says below 1.
             .withColumn("drop_ratio",
                         (F.col("prev_close") - F.col("ex_open"))
                         / F.col("cash_amount"))
             .withColumn("drop_pct",
                         (F.col("prev_close") - F.col("ex_open"))
                         / F.col("prev_close"))
             .withColumn("div_yield",
                         F.col("cash_amount") / F.col("prev_close"))

             # Gross return on the trade itself: buy at the ex-1 close, sell
             # at the ex open, collect the dividend. This -- not drop_ratio
             # -- is what the backtest ultimately cares about.
             .withColumn("capture_ret",
                         (F.col("ex_open") - F.col("prev_close")
                          + F.col("cash_amount")) / F.col("prev_close"))
             .withColumn("mkt_overnight_ret",
                         (F.col("mkt_ex_open") - F.col("mkt_prev_close"))
                         / F.col("mkt_prev_close"))
             .withColumn("capture_ret_abn",
                         F.col("capture_ret") - F.col("mkt_overnight_ret"))

             # Holding through the post window instead of selling at the open.
             .withColumn("hold_ret",
                         (F.col("post_close") - F.col("prev_close")
                          + F.col("cash_amount")) / F.col("prev_close"))

             # --- quality flags. Filtering is a downstream decision; the
             # grain table records the facts and lets the analysis choose.
             .withColumn("has_core",
                         F.col("prev_close").isNotNull()
                         & F.col("ex_open").isNotNull())
             .withColumn("window_complete",
                         (F.col("n_bars_pre") == PRE)
                         & (F.col("n_bars_post") == POST))
             .withColumn("window_contiguous",
                         F.col("max_span_days") <= MAX_SPAN_DAYS)
             .withColumn("drop_ratio_extreme",
                         F.abs(F.col("drop_ratio")) > 20)
             .withColumn("low_yield", F.col("div_yield") < 0.005))

    grain = grain.cache()
    n_grain = grain.count()

    print(f"\n=== event grain: {n_grain} rows ===")
    grain.agg(
        F.sum(F.col("has_core").cast("int")).alias("has_core"),
        F.sum(F.col("window_complete").cast("int")).alias("complete"),
        F.sum((~F.col("window_contiguous")).cast("int")).alias("non_contig"),
        F.sum(F.col("drop_ratio_extreme").cast("int")).alias("extreme"),
        F.sum(F.col("low_yield").cast("int")).alias("low_yield"),
    ).show()

    core = grain.filter(F.col("has_core") & F.col("window_contiguous"))
    print("=== drop_ratio, events with both core prices ===")
    core.select(
        F.count("*").alias("n"),
        F.mean("drop_ratio").alias("mean"),
        F.expr("percentile_approx(drop_ratio, 0.25)").alias("p25"),
        F.expr("percentile_approx(drop_ratio, 0.5)").alias("median"),
        F.expr("percentile_approx(drop_ratio, 0.75)").alias("p75"),
        F.mean("capture_ret").alias("mean_capture_ret"),
        F.mean("capture_ret_abn").alias("mean_capture_abn"),
    ).show()

    print("=== most liquid events ===")
    (core.orderBy(F.desc("pre_avg_dollar_volume"))
     .select("ticker", "ex_date", "cash_amount", "prev_close", "ex_open",
             "drop_ratio", "capture_ret", "div_yield")
     .show(20))

    out_grain = f"{TEAM}/curated/div_event_grain"
    grain.write.mode("overwrite").parquet(out_grain)
    (grain.drop("distribution_types").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{out_grain}_csv"))
    print(f"wrote {n_grain} rows to {out_grain}")

    spark.stop()


if __name__ == "__main__":
    main()