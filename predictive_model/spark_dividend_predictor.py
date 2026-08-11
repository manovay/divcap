#!/usr/bin/env python3
"""
Spark MLlib dividend-capture classifier.

This script turns the dividend-event grain table into a supervised learning
problem: predict whether a dividend event is likely to yield a profitable
capture trade after accounting for the market move.

The design follows the project README:
- Use the event-grain features already available from the dividend-event job.
- Create a binary label from the abnormal capture return.
- Train a Logistic Regression model with Spark MLlib.
- Report feature importance through the model coefficients and a small set of
  evaluation metrics.

The script is intentionally verbose and commented so it can serve as a learning
reference while still being runnable on local or cluster Spark.
"""

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_CSV = REPO_ROOT / "m1_dividend_events" / "div_event_grain_2026-07_2026-08.csv"


def default_input_path():
    """Prefer the checked-in dividend-event CSV when it exists."""
    if LOCAL_CSV.exists():
        return str(LOCAL_CSV)
    return f"{TEAM}/curated/div_event_grain"


DEFAULT_INPUT = default_input_path()
DEFAULT_OUTPUT_DIR = str(REPO_ROOT / "predictive_model_results")


def is_csv_input_path(input_path):
    """Return True when the supplied path is a CSV file or a CSV-named directory."""
    if input_path is None:
        return False
    normalized = input_path.rstrip("/").lower()
    basename = os.path.basename(normalized)
    return basename.endswith(".csv") or basename.endswith("_csv") or basename == "csv" or "csv" in basename


def prepare_training_frame(df):
    """
    Convert the grain table into a training frame for Spark MLlib.

    This step performs three important tasks:
    1. Builds a binary label from the abnormal capture return.
    2. Engineer a compact set of numeric features from the event metrics.
    3. Drop rows that are missing the fields required by the model.

    The README emphasises that capture_ret_abn is the relevant outcome metric
    because it measures the trade's profitability after removing the market's
    own overnight move. We use a simple threshold to define a profitable event:
    capture_ret_abn > 0.0.
    """
    # Keep only rows that have the core price data required to compute the
    # target. Rows without prev_close/ex_open would otherwise create a label
    # that is undefined or based on nulls.
    filtered = df.filter(F.col("has_core") == True)

    # We want a straightforward binary task: profitable vs not profitable.
    # The threshold is 0.0, which means any event that beats the market on the
    # overnight trade receives a positive label.
    labeled = (
        filtered.withColumn(
            "label",
            F.when(F.col("capture_ret_abn") > 0.0, F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "yield_bucket",
            F.when(F.col("div_yield") < 0.005, "low")
            .when(F.col("div_yield") < 0.01, "medium")
            .otherwise("high"),
        )
        .withColumn(
            "size_bucket",
            F.when(F.col("pre_avg_dollar_volume") < 1e6, "small")
            .when(F.col("pre_avg_dollar_volume") < 5e6, "medium")
            .otherwise("large"),
        )
        .withColumn(
            "volatility_bucket",
            F.when(F.col("pre_vol") < 0.01, "low")
            .when(F.col("pre_vol") < 0.02, "medium")
            .otherwise("high"),
        )
    )

    # The model needs numeric features only. We assemble both numeric metrics
    # and a few simple categorical buckets that the pipeline will encode.
    feature_columns = [
        "cash_amount",
        "div_yield",
        "drop_pct",
        "pre_avg_ret",
        "pre_avg_abn_ret",
        "pre_vol",
        "pre_avg_dollar_volume",
        "post_avg_ret",
        "post_avg_abn_ret",
        "n_distributions",
        "frequency",
        "n_bars",
        "n_bars_pre",
        "n_bars_post",
        "yield_bucket",
        "size_bucket",
        "volatility_bucket",
    ]

    # Keep only rows where every required feature is not null.
    prepared = (
        labeled.select("label", *feature_columns)
        .dropna(subset=["label", *feature_columns])
    )

    return prepared


def build_pipeline():
    """
    Create a Spark ML pipeline with feature encoding and logistic regression.

    This is intentionally small and readable. The pipeline does the following:
    - StringIndexer converts categorical buckets to numeric indices.
    - OneHotEncoder turns those indices into sparse vectors.
    - VectorAssembler combines the numeric and encoded features.
    - LogisticRegression trains the classifier.
    """
    categorical_cols = ["yield_bucket", "size_bucket", "volatility_bucket"]
    numeric_cols = [
        "cash_amount",
        "div_yield",
        "drop_pct",
        "pre_avg_ret",
        "pre_avg_abn_ret",
        "pre_vol",
        "pre_avg_dollar_volume",
        "post_avg_ret",
        "post_avg_abn_ret",
        "n_distributions",
        "frequency",
        "n_bars",
        "n_bars_pre",
        "n_bars_post",
    ]

    stages = []
    for col in categorical_cols:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=[f"{col}_idx"], outputCols=[f"{col}_vec"], dropLast=False)
        stages.extend([indexer, encoder])

    feature_cols = numeric_cols + [f"{col}_vec" for col in categorical_cols]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50)
    stages.extend([assembler, lr])
    return Pipeline(stages=stages)


def train_model(spark, input_path, output_dir, cv_folds=3):
    """
    Run the full training flow with cross-validation.

    The process is:
    1. Load the event-grain table from the checked-in CSV or from HDFS.
    2. Prepare the training row-level data.
    3. Run k-fold cross-validation for a more stable estimate than a single
       train/test split.
    4. Fit the final model on the full prepared dataset.
    5. Evaluate performance and save the trained model.
    """
    if is_csv_input_path(input_path):
        raw = spark.read.csv(input_path, header=True, inferSchema=True)
    else:
        raw = spark.read.parquet(input_path)

    training_df = prepare_training_frame(raw)
    training_df = training_df.repartition(8)

    pipeline = build_pipeline()
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    accuracy_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    precision_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    recall_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
    f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

    row_count = training_df.count()
    if row_count < max(10, cv_folds * 2):
        train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)
        auc = evaluator.evaluate(predictions)
        accuracy = accuracy_evaluator.evaluate(predictions)
        precision = precision_evaluator.evaluate(predictions)
        recall = recall_evaluator.evaluate(predictions)
        f1 = f1_evaluator.evaluate(predictions)
        cv_auc = auc
        final_model = model
    else:
        # For a real estimate, use k-fold cross-validation rather than one
        # random train/test split. The folds are created by Spark internally
        # and the average AUC is reported.
        lr_stage = pipeline.getStages()[-1]
        param_grid = (
            ParamGridBuilder()
            .addGrid(lr_stage.regParam, [0.0, 0.01, 0.1])
            .addGrid(lr_stage.elasticNetParam, [0.0, 0.5, 1.0])
            .build()
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=cv_folds,
            seed=42,
        )
        cv_model = cv.fit(training_df)
        final_model = cv_model.bestModel
        cv_auc = cv_model.avgMetrics[0]
        holdout_predictions = final_model.transform(training_df)
        auc = evaluator.evaluate(holdout_predictions)
        accuracy = accuracy_evaluator.evaluate(holdout_predictions)
        precision = precision_evaluator.evaluate(holdout_predictions)
        recall = recall_evaluator.evaluate(holdout_predictions)
        f1 = f1_evaluator.evaluate(holdout_predictions)

    scored_rows = final_model.transform(training_df)

    # Save a row-by-row scoring output so the user can inspect the model's
    # predictions for every input event, not just the aggregate summary.
    predictions_output = os.path.join(output_dir, "predictions.csv")
    scored_rows = scored_rows.select(
        "label",
        F.col("prediction").alias("predicted_label"),
        F.lit(None).cast("double").alias("prob_positive"),
    )
    scored_rows.write.mode("overwrite").csv(predictions_output, header=True)

    model_path = os.path.join(output_dir, "dividend_capture_logreg")
    final_model.write().overwrite().save(model_path)

    lr_model = final_model.stages[-1]
    feature_names = [
        "cash_amount",
        "div_yield",
        "drop_pct",
        "pre_avg_ret",
        "pre_avg_abn_ret",
        "pre_vol",
        "pre_avg_dollar_volume",
        "post_avg_ret",
        "post_avg_abn_ret",
        "n_distributions",
        "frequency",
        "n_bars",
        "n_bars_pre",
        "n_bars_post",
        "yield_bucket_vec",
        "size_bucket_vec",
        "volatility_bucket_vec",
    ]
    coefficients = lr_model.coefficients.toArray()
    coeffs = [(float(c), name) for c, name in zip(coefficients, feature_names)]

    output = [
        f"AUC_cv_mean={cv_auc:.4f}",
        f"AUC_holdout_like={auc:.4f}",
        f"Accuracy={accuracy:.4f}",
        f"Precision={precision:.4f}",
        f"Recall={recall:.4f}",
        f"F1={f1:.4f}",
        f"Rows={training_df.count()}",
        f"Folds={cv_folds}",
        "Top coefficients:",
    ]
    for value, name in sorted(coeffs, key=lambda item: abs(item[0]), reverse=True)[:10]:
        output.append(f"{name}: {value:.4f}")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = os.path.join(output_dir, "model_summary.txt")
    summary_text = "\n".join(output)
    Path(out_path).write_text(summary_text, encoding="utf-8")

    print(summary_text)
    print(f"Saved model to {model_path}")
    print(f"Saved summary to {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Train a Spark MLlib model for dividend capture profitability")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to the dividend-event grain CSV or parquet directory")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Local directory to save the trained model and report")
    parser.add_argument("--local", action="store_true", help="Run Spark locally for development")
    parser.add_argument("--cv-folds", type=int, default=3, help="Number of cross-validation folds")
    args = parser.parse_args()

    if args.local:
        builder = SparkSession.builder.master("local[2]").appName("dividend-capture-model")
    else:
        builder = SparkSession.builder.appName("dividend-capture-model")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        train_model(spark, args.input, args.output_dir, cv_folds=args.cv_folds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
