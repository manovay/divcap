#!/usr/bin/env python3
"""
Predict sector from company description text, to fill the sic_description gap.

Trains on the rows that have BOTH a usable description and a sic_description,
then labels the rows that have a description but no SIC.

    RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression

Scope decisions and why:

- CS + ADRC only. FUND/PFD/ETF have no SIC by construction, so there are zero
  training examples for them. A model trained on operating companies will still
  emit a confident label for a credit fund ("seeks a high level of current
  income"), and it will be meaningless. sec_type already buckets those.

- Deduped on description. Warrants, units and preferreds inherit the parent
  company's description verbatim (ABLV/ABLVW, ACP/ACPpA). Leaving them in puts
  identical text in both train and test, so test accuracy becomes memorisation.

- randomSplit is CORRECT here, unlike the returns model in M1_README.md
  section 3. Sector is a static attribute of a company, not a time series;
  there is no future to leak from. The temporal-split rule applies to
  capture_ret_abn, not to this.

- Label collapse. 376 distinct sic_description values, 64 two-digit major
  groups, ~10 SIC divisions. Many 4-digit distinctions are unlearnable from
  text on principle: STATE vs NATIONAL COMMERCIAL BANKS is a charter type no
  description mentions. This reports row counts at both levels before training
  so the choice is made on data.

Run:
    spark-submit --master yarn --deploy-mode client \
        --num-executors 4 --executor-memory 4g --executor-cores 2 \
        --files sector_ml/sic_code_map.json \
        sector_ml/train_sector_model.py [hybrid|major|division] [--write]

Inputs (HDFS):  $TEAM/reference/tickers_all_5y_metadata.jsonl
                sic_code_map.json (shipped via --files, read on the driver)
Outputs: stdout; with --write, $TEAM/curated/pseudo_sector
"""

import json
import os
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import (RegexTokenizer, StopWordsRemover, HashingTF,
                                IDF, StringIndexer, IndexToString)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
META = f"{TEAM}/reference/tickers_all_5y_metadata.jsonl"
MAP = "sector_ml/sic_code_map.json"

MIN_CHARS = 80      # matches census_sector_labels.py
MIN_CLASS = 40      # below this a class cannot be learned or evaluated
LEVELS = ("hybrid", "major", "division")
NUM_FEATURES = 1 << 14
SEED = 42

# SIC divisions. Ranges are inclusive on the 2-digit major group.
DIVISIONS = [
    (1, 9, "Agriculture, Forestry & Fishing"),
    (10, 14, "Mining"),
    (15, 17, "Construction"),
    (20, 39, "Manufacturing"),
    (40, 49, "Transportation & Public Utilities"),
    (50, 51, "Wholesale Trade"),
    (52, 59, "Retail Trade"),
    (60, 67, "Finance, Insurance & Real Estate"),
    (70, 89, "Services"),
    (91, 99, "Public Administration"),
]

# Boilerplate present in nearly every description. Left in, these burn hash
# buckets and add no signal.
EXTRA_STOPWORDS = [
    "company", "companies", "inc", "corp", "corporation", "ltd", "llc", "plc",
    "group", "holdings", "holding", "also", "operates", "operating", "provides",
    "offers", "engaged", "business", "businesses", "segment", "segments",
    "products", "services", "customers", "markets", "including", "well",
    "primarily", "founded", "headquartered", "subsidiaries", "sa", "nv", "ag",
]


def division_expr(col):
    """2-digit major group -> division name."""
    e = F.lit(None).cast("string")
    for lo, hi, name in reversed(DIVISIONS):
        e = F.when((col >= lo) & (col <= hi), F.lit(name)).otherwise(e)
    return e


def main():
    level = "hybrid"
    for a in sys.argv[1:]:
        if a in LEVELS:
            level = a
    write = "--write" in sys.argv

    spark = (SparkSession.builder.appName("divcap-sector-model")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())

    # ---- sic_description -> sic_code, from the committed map ---------------
    path = MAP if os.path.exists(MAP) else os.path.basename(MAP)
    with open(path) as f:
        code_map = json.load(f)
    print(f"\nsic_code map: {len(code_map)} descriptions")
    smap = spark.createDataFrame(
        [(k, str(v)) for k, v in code_map.items()], ["sic_description", "sic_code"])

    # ---- reference rows ---------------------------------------------------
    meta = (spark.read.json(META)
            .withColumn("desc_len", F.length(F.trim(F.coalesce("description", F.lit("")))))
            .withColumn("desc_usable", F.col("desc_len") >= MIN_CHARS)
            .withColumn("has_sic",
                        F.length(F.trim(F.coalesce("sic_description", F.lit("")))) > 0)
            .withColumn("sec_type", F.coalesce("type", F.lit("UNKNOWN")))
            .filter("active AND desc_usable AND sec_type IN ('CS','ADRC')")
            .join(smap, on="sic_description", how="left")
            .withColumn("major", F.substring("sic_code", 1, 2).cast("int"))
            .withColumn("division", division_expr(F.col("major")))
            .withColumn("major_lbl", F.concat(F.lit("SIC "), F.col("major").cast("string")))
            .cache())

    labelled = meta.filter("has_sic").cache()
    unlabelled = meta.filter("NOT has_sic").cache()
    print(f"CS+ADRC labelled: {labelled.count()}   unlabelled: {unlabelled.count()}")

    missing = labelled.filter("sic_code IS NULL").count()
    if missing:
        print(f"WARNING: {missing} labelled rows have no sic_code -- the map is "
              f"incomplete or a description string drifted. Re-run "
              f"build_sic_code_map.py.")

    # ---- dedupe on description -------------------------------------------
    before = labelled.count()
    labelled = (labelled.withColumn(
        "rn", F.row_number().over(
            Window.partitionBy("description").orderBy("ticker")))
        .filter("rn = 1").drop("rn").cache())
    print(f"deduped on description: {before} -> {labelled.count()} "
          f"({before - labelled.count()} inherited duplicates removed)")

    # ---- how many rows survive at each collapse level ---------------------
    for lvl, col in (("4-digit", "sic_description"),
                     ("2-digit major group", "major_lbl"),
                     ("division", "division")):
        g = labelled.groupBy(col).count()
        n_cls = g.count()
        keep = g.filter(F.col("count") >= MIN_CLASS)
        n_keep = keep.count()
        rows = keep.agg(F.sum("count")).collect()[0][0] or 0
        top = g.orderBy(F.desc("count")).first()
        print(f"{lvl:>20}: {n_cls:>3} classes | {n_keep:>3} with >={MIN_CLASS} rows "
              f"| {rows}/{labelled.count()} rows kept "
              f"| largest {100.0 * top['count'] / labelled.count():.1f}%")

    print(f"\n=== training on: {level} ===")

    # HYBRID label. A flat 2-digit label sends every small group to a shared
    # OTHER bucket -- 33% of predictions in the first major-group run, and the
    # top confusion pair in both directions. Worse, it discards information the
    # coarser model handled fine: SIC 10 (Metal Mining) fell into OTHER even
    # though division-level Mining was learnable at 0.59 recall.
    #
    # So: keep the 2-digit group where there are enough rows to learn it (this
    # is what separates SIC 60 banks from SIC 67 REITs, the distinction the
    # division collapse destroys and the one a dividend study needs), and fall
    # back to the row's own DIVISION where there are not. Every row keeps an
    # interpretable label and there is no catch-all bucket.
    div = F.coalesce(F.col("division"), F.lit("Unclassified"))
    if level == "major":
        base = F.col("major_lbl")
    elif level == "division":
        base = div
    else:
        big = [r[0] for r in labelled.groupBy("major_lbl").count()
               .filter(F.col("count") >= MIN_CLASS).collect()]
        print(f"2-digit groups kept as-is: {len(big)}; the rest fall back to "
              f"their division")
        base = F.when(F.col("major_lbl").isin(big), F.col("major_lbl")) \
                .otherwise(F.concat(div, F.lit(" (other)")))

    data = (labelled.withColumn("label_str", base)
            .filter("label_str IS NOT NULL")
            .select("ticker", "sec_type", "description", "sic_description", "label_str")
            .cache())

    # Anything still under the floor after the fallback -- a division whose
    # small groups together still do not reach MIN_CLASS -- has nowhere left to
    # go. Reported explicitly rather than silently merged.
    tiny = [r[0] for r in data.groupBy("label_str").count()
            .filter(F.col("count") < MIN_CLASS).collect()]
    if tiny:
        print(f"still under {MIN_CLASS} rows after fallback, merged to OTHER: {tiny}")
        data = data.withColumn(
            "label_str",
            F.when(F.col("label_str").isin(tiny), F.lit("OTHER"))
             .otherwise(F.col("label_str"))).cache()
    n = data.count()
    print(f"training rows: {n}, classes: {data.select('label_str').distinct().count()}")

    dist = data.groupBy("label_str").count().orderBy(F.desc("count"))
    dist.withColumn("pct", F.round(100.0 * F.col("count") / n, 2)).show(30, truncate=False)
    baseline = dist.first()["count"] / n
    print(f"MAJORITY BASELINE: {100 * baseline:.1f}%  <- the number to beat\n")

    # ---- pipeline ---------------------------------------------------------
    # RegexTokenizer, not Tokenizer: descriptions are full of commas, parens
    # and periods, and Tokenizer splits on whitespace only, so "cancer," and
    # "cancer" would become different features.
    tok = RegexTokenizer(inputCol="description", outputCol="words",
                         pattern="[^a-z]+", toLowercase=True, minTokenLength=3)
    stop = StopWordsRemover(inputCol="words", outputCol="kept")
    stop.setStopWords(StopWordsRemover.loadDefaultStopWords("english") + EXTRA_STOPWORDS)
    tf = HashingTF(inputCol="kept", outputCol="tf", numFeatures=NUM_FEATURES)
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=3)
    idx = StringIndexer(inputCol="label_str", outputCol="label", handleInvalid="keep")
    lr = LogisticRegression(maxIter=50, regParam=0.02, elasticNetParam=0.0)
    pipe = Pipeline(stages=[tok, stop, tf, idf, idx, lr])

    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    print(f"train {train.count()} / test {test.count()}")

    model = pipe.fit(train)
    pred = model.transform(test).cache()

    acc = MulticlassClassificationEvaluator(
        metricName="accuracy").evaluate(pred)
    f1 = MulticlassClassificationEvaluator(metricName="f1").evaluate(pred)
    print(f"\n=== RESULT ===")
    print(f"accuracy      {acc:.3f}")
    print(f"weighted F1   {f1:.3f}")
    print(f"baseline      {baseline:.3f}")
    print(f"lift over baseline: {acc - baseline:+.3f}")

    # Per-class, because accuracy hides a model that only learns big classes.
    labels = model.stages[-2].labels
    print("\n=== per-class on the test set ===")
    i2s = IndexToString(inputCol="prediction", outputCol="pred_str", labels=labels)
    pred2 = i2s.transform(pred)
    (pred2.groupBy("label_str").agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("pred_str") == F.col("label_str"), 1).otherwise(0)).alias("hit"))
        .withColumn("recall", F.round(F.col("hit") / F.col("n"), 3))
        .orderBy(F.desc("n")).show(30, truncate=False))

    print("=== most common confusions ===")
    (pred2.filter("pred_str != label_str")
          .groupBy("label_str", "pred_str").count()
          .orderBy(F.desc("count")).show(15, truncate=False))

    # ---- apply to the gap -------------------------------------------------
    filled = i2s.transform(model.transform(unlabelled))
    print(f"\n=== pseudo-sector assigned to {filled.count()} unlabelled CS+ADRC ===")
    filled.groupBy("pred_str").count().orderBy(F.desc("count")).show(20, truncate=False)
    filled.select("ticker", "sec_type", "pred_str",
                  F.substring("description", 1, 90).alias("desc")).show(15, truncate=False)

    if write:
        out = f"{TEAM}/curated/pseudo_sector"
        (filled.select("ticker", "sec_type",
                       F.col("pred_str").alias("pseudo_sector"),
                       F.lit(level).alias("label_level"))
               .coalesce(1).write.mode("overwrite").parquet(out))
        print(f"\nwrote {out}")

    spark.stop()


if __name__ == "__main__":
    main()
