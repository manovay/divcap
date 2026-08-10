#!/usr/bin/env python3
"""
Step 1 of the description-ML task: can a text model actually fill the sector gap?

Answers four questions before any modelling happens:

  Q1  How many rows have BOTH a usable description and a sic_description?
      -> the labelled training set. Needs to be a few thousand.
  Q2  How many rows have a usable description but NO sic_description?
      -> the fillable gap. This is the deliverable size. If it is small, the
         result is "descriptions are absent exactly where SIC is absent",
         which is itself reportable.
  Q3  What is the class distribution of the label?
      -> 376 distinct sic_description values is too many classes. This reports
         how many survive a >= MIN_CLASS row floor, and what the
         majority-class baseline accuracy is. Any reported accuracy has to
         beat that number or it means nothing.
  Q4  How long are the descriptions, and how much of the training set is SPACs?
      -> "X is a blank check company" is a one-line boilerplate description
         with a distinctive SIC. If BLANK CHECKS is a large share of the
         training rows the model will ace them and inflate accuracy.

Note description is EMPTY STRING, not null, for ETFs and delisted names, so
isNotNull() does not filter them. length(trim(...)) does.

Run (on nyu-dataproc-m):
    spark-submit --master yarn --deploy-mode client \
        --num-executors 2 --executor-memory 4g --executor-cores 2 \
        sector_ml/census_sector_labels.py

Input (HDFS): $TEAM/reference/tickers_all_5y_metadata.jsonl
Output: stdout only. Nothing is written.
"""

import os
from pyspark.sql import SparkSession, functions as F

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
META = f"{TEAM}/reference/tickers_all_5y_metadata.jsonl"

# A class with fewer rows than this cannot be learned or evaluated.
MIN_CLASS = 50

# Below this many characters a description is a stub, not a document.
MIN_CHARS = 80


def main():
    # The cluster default is spark.sql.shuffle.partitions=1000. Every groupBy
    # below would fan out to 1000 tasks over 20K rows. Nothing is written here
    # so there is no tiny-file problem, but it wastes YARN slots on a cluster
    # shared with the whole class.
    spark = (SparkSession.builder.appName("divcap-sector-census")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())

    meta = (spark.read.json(META)
            # description is "" not null for ETFs and delisted names.
            .withColumn("desc_len", F.length(F.trim(F.coalesce("description", F.lit("")))))
            .withColumn("has_desc", F.col("desc_len") > 0)
            .withColumn("desc_usable", F.col("desc_len") >= MIN_CHARS)
            .withColumn("has_sic",
                        F.length(F.trim(F.coalesce("sic_description", F.lit("")))) > 0)
            .withColumn("sec_type", F.coalesce("type", F.lit("UNKNOWN")))
            .cache())

    total = meta.count()
    print(f"\n=== rows: {total} (expect 20729) ===")

    # ---- Q1 / Q2: the 2x2 -------------------------------------------------
    print("\n=== Q1/Q2  has_desc x has_sic, all rows ===")
    meta.groupBy("has_desc", "has_sic").count().orderBy("has_desc", "has_sic").show()

    print(f"=== same, but description >= {MIN_CHARS} chars (desc_usable) ===")
    meta.groupBy("desc_usable", "has_sic").count().orderBy("desc_usable", "has_sic").show()

    print("=== the two cells that matter, split by active ===")
    (meta.filter("desc_usable")
         .withColumn("cell", F.when(F.col("has_sic"), F.lit("TRAIN (desc+sic)"))
                              .otherwise(F.lit("FILLABLE (desc, no sic)")))
         .groupBy("cell", "active").count().orderBy("cell", "active").show(truncate=False))

    print("=== FILLABLE rows by security type -- who are we actually filling? ===")
    (meta.filter("desc_usable AND NOT has_sic")
         .groupBy("sec_type").count().orderBy(F.desc("count")).show(30, truncate=False))

    print("=== rows with NEITHER -- unreachable by this method, must stay 'unknown' ===")
    (meta.filter("NOT desc_usable AND NOT has_sic")
         .groupBy("sec_type").count().orderBy(F.desc("count")).show(30, truncate=False))

    # ---- Q3: label distribution on the training set ------------------------
    train = meta.filter("desc_usable AND has_sic").cache()
    n_train = train.count()
    print(f"\n=== Q3  labelled training rows: {n_train} ===")

    by_sic = train.groupBy("sic_description").count().cache()
    n_classes = by_sic.count()
    print(f"distinct sic_description among training rows: {n_classes}")

    keep = by_sic.filter(F.col("count") >= MIN_CLASS)
    n_keep = keep.count()
    kept_rows = keep.agg(F.sum("count")).collect()[0][0] or 0
    print(f"classes with >= {MIN_CLASS} rows: {n_keep}  "
          f"covering {kept_rows}/{n_train} rows "
          f"({100.0 * kept_rows / max(n_train, 1):.1f}%)")
    print(f"-> {n_classes - n_keep} classes are too small to learn; they need "
          f"collapsing into a coarser bucket or dropping.")

    print("\n=== top 30 classes -- note the majority-class share ===")
    top = by_sic.orderBy(F.desc("count"))
    top.withColumn("pct", F.round(100.0 * F.col("count") / n_train, 2)).show(30, truncate=False)

    biggest = top.first()
    print(f"MAJORITY-CLASS BASELINE: always predict '{biggest['sic_description']}' "
          f"-> {100.0 * biggest['count'] / n_train:.1f}% accuracy on {n_classes} classes.")
    print("Any accuracy figure reported for the model has to be compared to that.")

    # ---- Q4: description quality ------------------------------------------
    print("\n=== Q4  description length on training rows ===")
    train.select(
        F.min("desc_len").alias("min"),
        F.expr("percentile_approx(desc_len, 0.25)").alias("p25"),
        F.expr("percentile_approx(desc_len, 0.50)").alias("median"),
        F.expr("percentile_approx(desc_len, 0.75)").alias("p75"),
        F.max("desc_len").alias("max"),
    ).show()

    spac = train.filter(F.upper("sic_description").contains("BLANK CHECK")).count()
    print(f"BLANK CHECKS (SPAC) training rows: {spac} "
          f"({100.0 * spac / max(n_train, 1):.1f}%)")
    print("SPAC descriptions are near-identical boilerplate with a distinctive "
          "SIC. A large share here means headline accuracy is inflated by an "
          "easy class -- report per-class metrics, not just accuracy.")

    # Do the descriptions of no-SIC rows even talk about holdings? Eyeball it.
    print("\n=== sample FILLABLE descriptions -- are these sector-informative? ===")
    (meta.filter("desc_usable AND NOT has_sic")
         .select("ticker", "sec_type", F.substring("description", 1, 160).alias("desc"))
         .orderBy("ticker").show(15, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
