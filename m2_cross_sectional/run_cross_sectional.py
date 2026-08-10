#!/usr/bin/env python3
"""Build the Step 2 dividend-capture cross-sectional analysis on Spark.

This job consumes the M1 event grain and event panel.  It validates the live
runtime contract before producing run-versioned Parquet outputs.  It does not
train a model, estimate transaction costs, or perform options analysis.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:  # Keep pure helpers importable for local tests without PySpark installed.
    from pyspark.sql import DataFrame, SparkSession, functions as F
except ModuleNotFoundError:  # pragma: no cover - exercised on the cluster
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]


REQUIRED_CONFIG_KEYS = (
    "team",
    "grain_path",
    "panel_path",
    "metadata_path",
    "output_root",
    "sic_description_column",
    "market_ticker",
    "min_history_days",
    "bucket_count",
    "min_cell_n",
    "event_offset_min",
    "event_offset_max",
    "primary_metric",
    "secondary_metric",
    "academic_metric",
    "metric_tolerance",
    "report_top_sic_n",
)

GRAIN_REQUIRED_COLUMNS = (
    "ticker",
    "ex_date",
    "cash_amount",
    "prev_close",
    "ex_open",
    "div_yield",
    "drop_ratio",
    "drop_pct",
    "capture_ret",
    "mkt_overnight_ret",
    "capture_ret_abn",
    "pre_avg_ret",
    "pre_avg_abn_ret",
    "pre_vol",
    "pre_avg_dollar_volume",
    "n_bars",
    "has_core",
    "window_contiguous",
)

PANEL_REQUIRED_COLUMNS = (
    "ticker",
    "ex_date",
    "bar_date",
    "offset",
    "abn_ret_cc",
)

METADATA_REQUIRED_COLUMNS = (
    "ticker",
    "active",
    "list_date",
    "sic_description",
)

MODEL_FEATURE_COLUMNS = (
    "event_id",
    "ticker",
    "ex_date",
    "div_yield",
    "pre_avg_ret",
    "pre_avg_abn_ret",
    "pre_vol",
    "pre_avg_dollar_volume",
    "frequency",
    "declaration_lead_days",
    "event_month",
)

MODEL_OUTCOME_COLUMNS = (
    "event_id",
    "ticker",
    "ex_date",
    "capture_ret",
    "capture_ret_abn",
    "drop_ratio",
    "drop_pct",
    "hold_ret",
    "has_core",
    "window_contiguous",
)

FORBIDDEN_MODEL_FEATURE_COLUMNS = frozenset(
    {
        "ex_open",
        "ex_close",
        "post_close",
        "drop_ratio",
        "drop_pct",
        "capture_ret",
        "capture_ret_abn",
        "hold_ret",
        "post_avg_ret",
        "post_avg_abn_ret",
        "div_yield_bucket",
        "pre_vol_bucket",
        "pre_avg_dollar_volume_bucket",
        "sic_description",
        "sic_code",
        "pseudo_sector",
        "label_level",
    }
)

GROUP_SUMMARY_COLUMNS = (
    "n_events",
    "n_tickers",
    "mean_capture_ret_abn",
    "median_capture_ret_abn",
    "p25_capture_ret_abn",
    "p75_capture_ret_abn",
    "positive_capture_ret_abn_rate",
    "mean_capture_ret",
    "median_capture_ret",
    "median_drop_ratio",
    "p25_drop_ratio",
    "p75_drop_ratio",
    "drop_ratio_lt_1_rate",
)

OPTIONAL_COLUMN_TYPES = {
    "frequency": "double",
    "declaration_date": "date",
    "declaration_lead_days": "integer",
    "event_month": "integer",
    "hold_ret": "double",
}

RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SIC_TEMPORAL_LIMITATION = (
    "sic_description is current-reference metadata and is not guaranteed "
    "point-in-time classification"
)
CORPORATE_ACTION_LIMITATION = (
    "Corporate-action contamination in upstream unadjusted prices is ignored "
    "for this M2 build and is not a filter or preflight blocker."
)
TIME_DECAY_DEPENDENCE_LIMITATION = (
    "Event-time observations can cluster on the same bar_date and are not "
    "independent; inspect calendar-date concentration, especially at offset -3, "
    "before interpretation or significance testing."
)


class M2ValidationError(RuntimeError):
    """A runtime prerequisite failed with an operator-actionable message."""


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the run-versioned M2 cross-sectional analysis."
    )
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--run-id", required=True, help="Unique immutable run ID")
    parser.add_argument(
        "--mode", required=True, choices=("preflight", "final")
    )
    return parser.parse_args(argv)


def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError as exc:
        raise M2ValidationError(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise M2ValidationError(f"Invalid JSON config {path}: {exc}") from exc

    missing = sorted(set(REQUIRED_CONFIG_KEYS) - set(config))
    if missing:
        raise M2ValidationError(
            f"Config is missing required keys: {', '.join(missing)}"
        )

    for key in ("team", "grain_path", "panel_path", "metadata_path", "output_root"):
        if not isinstance(config[key], str) or not config[key].strip():
            raise M2ValidationError(f"Config key {key!r} must be a non-empty string")

    if config["sic_description_column"] != "sic_description":
        raise M2ValidationError(
            "sic_description_column must be exactly 'sic_description'; alternate "
            "sector taxonomies are outside this build"
        )
    if not isinstance(config["market_ticker"], str) or not config[
        "market_ticker"
    ].strip():
        raise M2ValidationError("market_ticker must be a non-empty string")
    if int(config["min_history_days"]) < 1:
        raise M2ValidationError("min_history_days must be positive")
    if not 2 <= int(config["bucket_count"]) <= 20:
        raise M2ValidationError("bucket_count must be between 2 and 20")
    if int(config["min_cell_n"]) < 1:
        raise M2ValidationError("min_cell_n must be positive")
    if int(config["event_offset_min"]) != -4 or int(config["event_offset_max"]) != 3:
        raise M2ValidationError(
            "The proposal contract requires event_offset_min=-4 and "
            "event_offset_max=3"
        )
    if float(config["metric_tolerance"]) <= 0:
        raise M2ValidationError("metric_tolerance must be positive")
    if int(config["report_top_sic_n"]) < 1:
        raise M2ValidationError("report_top_sic_n must be positive")

    metric_expectations = {
        "primary_metric": "capture_ret_abn",
        "secondary_metric": "capture_ret",
        "academic_metric": "drop_ratio",
    }
    for key, expected in metric_expectations.items():
        if config[key] != expected:
            raise M2ValidationError(
                f"{key} must be {expected!r} for the M2 metric contract"
            )
    return config


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise M2ValidationError(
            "run_id must start with an alphanumeric character and contain only "
            "letters, digits, '.', '_' or '-'; path separators are forbidden"
        )


def resolve_runtime_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    """Apply an optional TEAM environment override to repository-default paths."""
    resolved = dict(config)
    configured_team = str(config["team"]).rstrip("/")
    runtime_team = os.environ.get("TEAM", configured_team).rstrip("/")
    resolved["team"] = runtime_team
    for key in ("grain_path", "panel_path", "metadata_path", "output_root"):
        value = str(config[key])
        value = value.replace("${TEAM}", runtime_team).replace("$TEAM", runtime_team)
        if runtime_team != configured_team and value.startswith(configured_team + "/"):
            value = runtime_team + value[len(configured_team) :]
        resolved[key] = value
    return resolved


def validate_model_feature_columns(columns: Iterable[str]) -> List[str]:
    """Return any outcome/leakage columns found in a feature table schema."""
    return sorted(set(columns) & FORBIDDEN_MODEL_FEATURE_COLUMNS)


def metric_identity_residuals(row: Mapping[str, Optional[float]]) -> Dict[str, float]:
    """Pure-Python counterpart of Spark preflight identities for unit tests."""
    required = (
        "drop_pct",
        "drop_ratio",
        "div_yield",
        "capture_ret",
        "capture_ret_abn",
        "mkt_overnight_ret",
        "stock_overnight_ret",
        "abn_overnight_ret",
    )
    if any(row.get(name) is None for name in required):
        raise ValueError("metric_identity_residuals requires non-null metrics")
    values = {name: float(row[name]) for name in required}  # type: ignore[arg-type]
    return {
        "drop_pct_equals_drop_ratio_times_div_yield": abs(
            values["drop_pct"]
            - values["drop_ratio"] * values["div_yield"]
        ),
        "capture_ret_equals_div_yield_minus_drop_pct": abs(
            values["capture_ret"] - (values["div_yield"] - values["drop_pct"])
        ),
        "capture_ret_abn_equals_capture_ret_minus_market": abs(
            values["capture_ret_abn"]
            - (values["capture_ret"] - values["mkt_overnight_ret"])
        ),
        "stock_overnight_ret_equals_negative_drop_pct": abs(
            values["stock_overnight_ret"] + values["drop_pct"]
        ),
        "abn_overnight_ret_equals_stock_minus_market": abs(
            values["abn_overnight_ret"]
            - (values["stock_overnight_ret"] - values["mkt_overnight_ret"])
        ),
    }


def deduplicate_cutpoints(
    candidates: Iterable[float], maximum: float, tolerance: float = 1e-12
) -> List[float]:
    """Return ordered usable cutpoints, preserving a minimum-valued cut.

    A cut at the observed maximum cannot create a non-empty upper bucket and is
    removed.  Duplicate quantiles are collapsed instead of fabricating bins.
    """
    cuts: List[float] = []
    for raw in sorted(float(value) for value in candidates if math.isfinite(value)):
        if raw >= maximum or math.isclose(raw, maximum, abs_tol=tolerance):
            continue
        if cuts and math.isclose(raw, cuts[-1], abs_tol=tolerance):
            continue
        cuts.append(raw)
    return cuts


def require_pyspark() -> None:
    if SparkSession is None or F is None:
        raise M2ValidationError(
            "PySpark is not installed in this interpreter. Run this job with "
            "spark-submit on the configured Spark/YARN environment."
        )


def hadoop_path_exists(spark: SparkSession, path: str) -> bool:
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(fs.exists(jvm_path))


def read_required_parquet(
    spark: SparkSession, path: str, label: str
) -> DataFrame:
    if not hadoop_path_exists(spark, path):
        raise M2ValidationError(
            f"{label} path does not exist: {path}. Inspect the matching config "
            "field and the upstream M1 HDFS output."
        )
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        raise M2ValidationError(
            f"{label} path exists but is not readable as Parquet: {path}: {exc}"
        ) from exc


def schema_rows(
    frame: DataFrame, dataset: str, required: Iterable[str]
) -> List[Tuple[str, str, str, bool]]:
    required_set = set(required)
    return [
        (dataset, field.name, field.dataType.simpleString(), field.name in required_set)
        for field in frame.schema.fields
    ]


def missing_columns(frame: DataFrame, required: Iterable[str]) -> List[str]:
    return sorted(set(required) - set(frame.columns))


def normalize_sic(column: Any) -> Any:
    trimmed = F.trim(column.cast("string"))
    return F.when(trimmed.isNull() | (trimmed == ""), F.lit(None)).otherwise(trimmed)


def enrich_sic_description(
    spark: SparkSession,
    grain: DataFrame,
    config: Mapping[str, Any],
    issues: List[str],
) -> Tuple[DataFrame, Dict[str, Any], List[Tuple[str, str, str, bool]]]:
    sic_column = str(config["sic_description_column"])
    if sic_column in grain.columns:
        enriched = grain.withColumn(sic_column, normalize_sic(F.col(sic_column)))
        enriched = enriched.withColumn(
            sic_column,
            F.coalesce(F.col(sic_column), F.lit("UNKNOWN")),
        )
        return (
            enriched,
            {
                "mode": "grain",
                "conflicting_ticker_count": 0,
                "events_blocked_by_list_date": 0,
                "metadata_non_null_sic_count": None,
            },
            [],
        )

    metadata_path = str(config["metadata_path"])
    if not hadoop_path_exists(spark, metadata_path):
        raise M2ValidationError(
            "The grain has no sic_description and metadata_path does not exist: "
            f"{metadata_path}. Inspect metadata_path and the M1 reference upload."
        )
    try:
        metadata_raw = spark.read.json(metadata_path)
    except Exception as exc:
        raise M2ValidationError(
            f"metadata_path is not readable as JSON: {metadata_path}: {exc}"
        ) from exc

    metadata_schema = schema_rows(
        metadata_raw, "metadata", METADATA_REQUIRED_COLUMNS
    )
    missing = missing_columns(metadata_raw, METADATA_REQUIRED_COLUMNS)
    if missing:
        raise M2ValidationError(
            "Metadata schema is missing required fields "
            f"{missing}; found {sorted(metadata_raw.columns)}. Inspect "
            "ticker/gen_ticker_metadata.py and metadata_path."
        )

    metadata = (
        metadata_raw.select(
            F.col("ticker").cast("string").alias("ticker"),
            F.col("active").alias("active"),
            F.to_date("list_date").alias("list_date"),
            normalize_sic(F.col("sic_description")).alias("sic_description"),
        )
        .filter(F.col("ticker").isNotNull())
        .cache()
    )

    non_null_sic_count = metadata.filter(
        F.col("sic_description").isNotNull()
    ).count()
    if non_null_sic_count == 0:
        issues.append(
            "metadata_path exposes sic_description but contains no non-null values"
        )

    description_counts = (
        metadata.filter(F.col("sic_description").isNotNull())
        .groupBy("ticker")
        .agg(
            F.countDistinct("sic_description").alias("sic_value_count"),
            F.sort_array(F.collect_set("sic_description")).alias("sic_values"),
        )
        .cache()
    )
    conflicts = description_counts.filter(F.col("sic_value_count") > 1).cache()
    conflict_count = conflicts.count()
    if conflict_count:
        examples = [
            f"{row['ticker']}: {row['sic_values']}"
            for row in conflicts.orderBy("ticker").limit(50).collect()
        ]
        print("=== conflicting metadata SIC descriptions (up to 50) ===")
        for example in examples:
            print(example)
        issues.append(
            f"{conflict_count} ticker(s) have conflicting non-null "
            "sic_description values; resolve metadata conflicts upstream"
        )

    unambiguous = description_counts.filter(F.col("sic_value_count") == 1).select(
        "ticker"
    )
    candidates = (
        metadata.filter(F.col("sic_description").isNotNull())
        .join(unambiguous, "ticker", "inner")
        .groupBy("ticker", "sic_description")
        .agg(F.min("list_date").alias("sic_list_date"))
    )

    joined = grain.join(F.broadcast(candidates), "ticker", "left")
    blocked_condition = (
        F.col("sic_description").isNotNull()
        & F.col("sic_list_date").isNotNull()
        & (F.col("ex_date") < F.col("sic_list_date"))
    )
    blocked_count = joined.filter(blocked_condition).count()
    enriched = (
        joined.withColumn(
            "sic_description",
            F.when(blocked_condition, F.lit("UNKNOWN"))
            .otherwise(F.col("sic_description"))
            .cast("string"),
        )
        .withColumn(
            "sic_description",
            F.coalesce(F.col("sic_description"), F.lit("UNKNOWN")),
        )
        .drop("sic_list_date")
    )
    return (
        enriched,
        {
            "mode": "metadata",
            "conflicting_ticker_count": conflict_count,
            "events_blocked_by_list_date": blocked_count,
            "metadata_non_null_sic_count": non_null_sic_count,
        },
        metadata_schema,
    )


def add_derived_fields(grain: DataFrame) -> DataFrame:
    declaration = (
        F.to_date("declaration_date")
        if "declaration_date" in grain.columns
        else F.lit(None).cast("date")
    )
    return (
        grain.withColumn("ex_date", F.to_date("ex_date"))
        .withColumn(
            "event_id",
            F.concat_ws("|", F.col("ticker"), F.date_format("ex_date", "yyyy-MM-dd")),
        )
        .withColumn("event_month", F.month("ex_date"))
        .withColumn(
            "declaration_lead_days", F.datediff(F.col("ex_date"), declaration)
        )
        .withColumn(
            "stock_overnight_ret", F.col("ex_open") / F.col("prev_close") - 1.0
        )
        .withColumn(
            "abn_overnight_ret",
            F.col("stock_overnight_ret") - F.col("mkt_overnight_ret"),
        )
        .withColumn(
            "ex_ym",
            F.coalesce(
                F.col("ex_ym") if "ex_ym" in grain.columns else F.lit(None),
                F.date_format("ex_date", "yyyy-MM"),
            ),
        )
    )


def metric_identity_summary(
    spark: SparkSession,
    grain: DataFrame,
    tolerance: float,
    issues: List[str],
) -> DataFrame:
    specs = (
        (
            "drop_pct_equals_drop_ratio_times_div_yield",
            F.col("drop_pct"),
            F.col("drop_ratio") * F.col("div_yield"),
        ),
        (
            "capture_ret_equals_div_yield_minus_drop_pct",
            F.col("capture_ret"),
            F.col("div_yield") - F.col("drop_pct"),
        ),
        (
            "capture_ret_abn_equals_capture_ret_minus_market",
            F.col("capture_ret_abn"),
            F.col("capture_ret") - F.col("mkt_overnight_ret"),
        ),
        (
            "stock_overnight_ret_equals_negative_drop_pct",
            F.col("stock_overnight_ret"),
            -F.col("drop_pct"),
        ),
        (
            "abn_overnight_ret_equals_stock_minus_market",
            F.col("abn_overnight_ret"),
            F.col("stock_overnight_ret") - F.col("mkt_overnight_ret"),
        ),
    )
    expressions = []
    for name, left, right in specs:
        valid = left.isNotNull() & right.isNotNull()
        difference = F.abs(left - right)
        expressions.extend(
            [
                F.sum(F.when(valid, 1).otherwise(0)).alias(f"{name}__eligible"),
                F.sum(
                    F.when(valid & (difference > F.lit(tolerance)), 1).otherwise(0)
                ).alias(f"{name}__violations"),
                F.max(F.when(valid, difference)).alias(f"{name}__max_error"),
            ]
        )
    values = grain.agg(*expressions).collect()[0].asDict()
    rows = []
    for name, _, _ in specs:
        eligible = int(values[f"{name}__eligible"] or 0)
        violations = int(values[f"{name}__violations"] or 0)
        max_error = values[f"{name}__max_error"]
        rows.append((name, eligible, violations, max_error, float(tolerance)))
        if eligible == 0:
            issues.append(f"metric identity {name} has zero eligible rows")
        if violations:
            issues.append(
                f"metric identity {name} has {violations} violation(s) above "
                f"tolerance {tolerance}"
            )
    return spark.createDataFrame(
        rows,
        "identity string, eligible_rows long, violation_rows long, "
        "max_abs_error double, tolerance double",
    )


def sample_conditions(
    grain: DataFrame, market_ticker: str
) -> List[Tuple[str, Any, str]]:
    cash_positive = F.col("cash_amount") > 0
    core = cash_positive & (F.col("has_core") == F.lit(True))
    contiguous = core & (F.col("window_contiguous") == F.lit(True))
    base = contiguous & (F.col("ticker") != F.lit(market_ticker))
    return [
        ("raw", F.lit(True), "All event-grain rows"),
        ("cash_positive", cash_positive, "cash_amount > 0"),
        ("has_core", core, "cash positive and has_core = true"),
        (
            "window_contiguous",
            contiguous,
            "cash positive, has_core, and window_contiguous = true",
        ),
        (
            "benchmark_excluded",
            base,
            f"Base conditions with benchmark ticker {market_ticker} excluded",
        ),
        ("yield_usable", base & F.col("div_yield").isNotNull(), "Base plus div_yield"),
        (
            "volatility_usable",
            base & F.col("pre_vol").isNotNull(),
            "Base plus pre_vol",
        ),
        (
            "liquidity_usable",
            base & F.col("pre_avg_dollar_volume").isNotNull(),
            "Base plus pre_avg_dollar_volume",
        ),
        (
            "sic_known",
            base & (F.col("sic_description") != "UNKNOWN"),
            "Base with known sic_description",
        ),
        (
            "sic_unknown",
            base & (F.col("sic_description") == "UNKNOWN"),
            "Base with UNKNOWN sic_description",
        ),
        (
            "full_event_time_window",
            base & (F.col("n_bars") == 9),
            "Base with n_bars = 9",
        ),
    ]


def build_sample_funnel(
    spark: SparkSession, grain: DataFrame, market_ticker: str
) -> Tuple[DataFrame, Dict[str, int]]:
    conditions = sample_conditions(grain, market_ticker)
    aggregate = grain.agg(
        *[
            F.sum(F.when(condition, 1).otherwise(0)).alias(name)
            for name, condition, _ in conditions
        ]
    ).collect()[0]
    counts = {name: int(aggregate[name] or 0) for name, _, _ in conditions}
    rows = [
        (index + 1, name, counts[name], description)
        for index, (name, _, description) in enumerate(conditions)
    ]
    return (
        spark.createDataFrame(
            rows,
            "stage_order integer, stage string, n_events long, definition string",
        ),
        counts,
    )


def base_condition(market_ticker: str) -> Any:
    return (
        (F.col("cash_amount") > 0)
        & (F.col("has_core") == F.lit(True))
        & (F.col("window_contiguous") == F.lit(True))
        & (F.col("ticker") != F.lit(market_ticker))
    )


def summary_expressions() -> List[Any]:
    return [
        F.count("*").cast("long").alias("n_events"),
        F.countDistinct("ticker").cast("long").alias("n_tickers"),
        F.avg("capture_ret_abn").alias("mean_capture_ret_abn"),
        F.expr("percentile_approx(capture_ret_abn, 0.50, 10000)").alias(
            "median_capture_ret_abn"
        ),
        F.expr("percentile_approx(capture_ret_abn, 0.25, 10000)").alias(
            "p25_capture_ret_abn"
        ),
        F.expr("percentile_approx(capture_ret_abn, 0.75, 10000)").alias(
            "p75_capture_ret_abn"
        ),
        F.avg(
            F.when(
                F.col("capture_ret_abn").isNotNull(),
                (F.col("capture_ret_abn") > 0).cast("double"),
            )
        ).alias("positive_capture_ret_abn_rate"),
        F.avg("capture_ret").alias("mean_capture_ret"),
        F.expr("percentile_approx(capture_ret, 0.50, 10000)").alias(
            "median_capture_ret"
        ),
        F.expr("percentile_approx(drop_ratio, 0.50, 10000)").alias(
            "median_drop_ratio"
        ),
        F.expr("percentile_approx(drop_ratio, 0.25, 10000)").alias(
            "p25_drop_ratio"
        ),
        F.expr("percentile_approx(drop_ratio, 0.75, 10000)").alias(
            "p75_drop_ratio"
        ),
        F.avg(
            F.when(
                F.col("drop_ratio").isNotNull(),
                (F.col("drop_ratio") < 1).cast("double"),
            )
        ).alias("drop_ratio_lt_1_rate"),
    ]


def grouped_summary(frame: DataFrame, group_column: Optional[str] = None) -> DataFrame:
    if group_column is None:
        return (
            frame.agg(*summary_expressions())
            .withColumn("analysis_group", F.lit("ALL"))
            .select("analysis_group", *GROUP_SUMMARY_COLUMNS)
        )
    return frame.groupBy(group_column).agg(*summary_expressions())


def add_quantile_bucket(
    spark: SparkSession,
    sample: DataFrame,
    dimension: str,
    value_column: str,
    bucket_column: str,
    requested_buckets: int,
) -> Tuple[DataFrame, List[Tuple[Any, ...]]]:
    bounds = sample.agg(
        F.min(value_column).alias("minimum"), F.max(value_column).alias("maximum")
    ).collect()[0]
    minimum = bounds["minimum"]
    maximum = bounds["maximum"]
    if minimum is None or maximum is None:
        raise M2ValidationError(f"{dimension} sample has no usable {value_column}")

    probabilities = [index / requested_buckets for index in range(1, requested_buckets)]
    candidates = sample.approxQuantile(value_column, probabilities, 0.0)
    cuts = deduplicate_cutpoints(candidates, float(maximum))

    bucket_number = F.lit(len(cuts) + 1)
    for index in range(len(cuts) - 1, -1, -1):
        bucket_number = F.when(
            F.col(value_column) <= F.lit(cuts[index]), F.lit(index + 1)
        ).otherwise(bucket_number)
    bucketed = sample.withColumn("_bucket_number", bucket_number.cast("integer"))
    bucketed = bucketed.withColumn(
        bucket_column,
        F.concat(F.lit("Q"), F.lpad(F.col("_bucket_number").cast("string"), 2, "0")),
    ).drop("_bucket_number")

    bucket_counts = {
        row[bucket_column]: int(row["n_events"])
        for row in bucketed.groupBy(bucket_column)
        .agg(F.count("*").alias("n_events"))
        .collect()
    }
    sample_count = sample.count()
    assigned_count = sum(bucket_counts.values())
    if assigned_count != sample_count or any(label is None for label in bucket_counts):
        raise M2ValidationError(
            f"{dimension} bucket assignment failed: sample_rows={sample_count}, "
            f"assigned_rows={assigned_count}, "
            f"labels={sorted(str(label) for label in bucket_counts)}"
        )
    actual_bucket_count = len(bucket_counts)
    rows: List[Tuple[Any, ...]] = []
    for index in range(actual_bucket_count):
        label = f"Q{index + 1:02d}"
        lower = None if index == 0 else float(cuts[index - 1])
        upper = float(cuts[index]) if index < len(cuts) else None
        rows.append(
            (
                dimension,
                value_column,
                bucket_column,
                label,
                index + 1,
                lower,
                upper,
                float(minimum),
                float(maximum),
                int(requested_buckets),
                int(actual_bucket_count),
                int(bucket_counts.get(label, 0)),
            )
        )
    return bucketed, rows


def event_time_daily_summary(
    stable_events: DataFrame,
    panel: DataFrame,
    minimum_offset: int,
    maximum_offset: int,
) -> DataFrame:
    keys = stable_events.select("ticker", "ex_date").distinct()
    usable_panel = (
        panel.select(
            F.col("ticker").cast("string").alias("ticker"),
            F.to_date("ex_date").alias("ex_date"),
            F.col("offset").cast("integer").alias("offset"),
            F.col("abn_ret_cc").cast("double").alias("abn_ret_cc"),
        )
        .filter(F.col("offset").between(minimum_offset, maximum_offset))
        .filter(F.col("abn_ret_cc").isNotNull())
    )
    return (
        usable_panel.join(keys, ["ticker", "ex_date"], "inner")
        .groupBy("offset")
        .agg(
            F.count("*").cast("long").alias("n_events"),
            F.countDistinct("ticker").cast("long").alias("n_tickers"),
            F.avg("abn_ret_cc").alias("mean_abn_ret_cc"),
            F.expr("percentile_approx(abn_ret_cc, 0.50, 10000)").alias(
                "median_abn_ret_cc"
            ),
            F.expr("percentile_approx(abn_ret_cc, 0.25, 10000)").alias(
                "p25_abn_ret_cc"
            ),
            F.expr("percentile_approx(abn_ret_cc, 0.75, 10000)").alias(
                "p75_abn_ret_cc"
            ),
        )
        .orderBy("offset")
    )


def event_time_overnight_summary(base: DataFrame) -> DataFrame:
    return (
        base.agg(
            F.count("*").cast("long").alias("n_events"),
            F.countDistinct("ticker").cast("long").alias("n_tickers"),
            F.avg("stock_overnight_ret").alias("mean_stock_overnight_ret"),
            F.expr("percentile_approx(stock_overnight_ret, 0.50, 10000)").alias(
                "median_stock_overnight_ret"
            ),
            F.expr("percentile_approx(stock_overnight_ret, 0.25, 10000)").alias(
                "p25_stock_overnight_ret"
            ),
            F.expr("percentile_approx(stock_overnight_ret, 0.75, 10000)").alias(
                "p75_stock_overnight_ret"
            ),
            F.avg("abn_overnight_ret").alias("mean_abn_overnight_ret"),
            F.expr("percentile_approx(abn_overnight_ret, 0.50, 10000)").alias(
                "median_abn_overnight_ret"
            ),
            F.expr("percentile_approx(abn_overnight_ret, 0.25, 10000)").alias(
                "p25_abn_overnight_ret"
            ),
            F.expr("percentile_approx(abn_overnight_ret, 0.75, 10000)").alias(
                "p75_abn_overnight_ret"
            ),
        )
        .withColumn(
            "stock_overnight_definition", F.lit("ex_open / prev_close - 1")
        )
        .withColumn(
            "abn_overnight_definition",
            F.lit("stock_overnight_ret - mkt_overnight_ret"),
        )
    )


def select_handoff_columns(
    frame: DataFrame, columns: Sequence[str]
) -> DataFrame:
    selected = []
    for name in columns:
        if name in frame.columns:
            selected.append(F.col(name))
        elif name in OPTIONAL_COLUMN_TYPES:
            selected.append(F.lit(None).cast(OPTIONAL_COLUMN_TYPES[name]).alias(name))
        else:
            raise M2ValidationError(
                f"Cannot create handoff: required column {name!r} is unavailable"
            )
    return frame.select(*selected)


def git_commit() -> str:
    repo_root = Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or "unavailable"
    except Exception:
        return "unavailable"


def write_parquet(frame: DataFrame, path: str) -> None:
    frame.write.mode("errorifexists").parquet(path)


def create_manifest(
    spark: SparkSession,
    config: Mapping[str, Any],
    run_id: str,
    run_root: str,
    coverage: Mapping[str, Any],
    sic_info: Mapping[str, Any],
    output_paths: Mapping[str, str],
) -> DataFrame:
    application_id = getattr(spark.sparkContext, "applicationId", "unavailable")
    row = {
        "run_id": run_id,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "grain_path": str(config["grain_path"]),
        "panel_path": str(config["panel_path"]),
        "metadata_path": str(config["metadata_path"]),
        "run_root": run_root,
        "min_ex_date": str(coverage["min_ex_date"]),
        "max_ex_date": str(coverage["max_ex_date"]),
        "history_span_days": int(coverage["history_span_days"]),
        "grain_row_count": int(coverage["grain_row_count"]),
        "panel_row_count": int(coverage["panel_row_count"]),
        "sic_enrichment_mode": str(sic_info["mode"]),
        "market_ticker_excluded": str(config["market_ticker"]),
        "market_ticker_event_count": int(coverage["market_ticker_event_count"]),
        "sic_temporal_limitation": SIC_TEMPORAL_LIMITATION,
        "corporate_action_limitation": CORPORATE_ACTION_LIMITATION,
        "time_decay_dependence_limitation": TIME_DECAY_DEPENDENCE_LIMITATION,
        "requested_bucket_count": int(config["bucket_count"]),
        "min_cell_n": int(config["min_cell_n"]),
        "event_offset_min": int(config["event_offset_min"]),
        "event_offset_max": int(config["event_offset_max"]),
        "primary_metric": str(config["primary_metric"]),
        "secondary_metric": str(config["secondary_metric"]),
        "academic_metric": str(config["academic_metric"]),
        "metric_tolerance": float(config["metric_tolerance"]),
        "metric_definitions_json": json.dumps(
            {
                "capture_ret_abn": "capture_ret - mkt_overnight_ret",
                "capture_ret": "div_yield - drop_pct",
                "drop_ratio": "(prev_close - ex_open) / cash_amount",
                "stock_overnight_ret": "ex_open / prev_close - 1",
                "abn_overnight_ret": "stock_overnight_ret - mkt_overnight_ret",
            },
            sort_keys=True,
        ),
        "spark_version": spark.version,
        "spark_application_id": application_id,
        "git_commit": git_commit(),
        "output_paths_json": json.dumps(dict(output_paths), sort_keys=True),
    }
    return spark.createDataFrame([row])


def run_job(
    spark: SparkSession,
    config: Mapping[str, Any],
    run_id: str,
    mode: str,
) -> None:
    issues: List[str] = []
    run_root = f"{str(config['output_root']).rstrip('/')}/{run_id}"
    market_ticker = str(config["market_ticker"]).strip()

    print("=== M2 cross-sectional preflight ===")
    print(f"mode={mode}")
    print(f"run_id={run_id}")
    print(f"grain_path={config['grain_path']}")
    print(f"panel_path={config['panel_path']}")
    print(f"metadata_path={config['metadata_path']}")
    print(f"market_ticker_excluded={market_ticker}")
    print(f"planned_run_root={run_root}")

    if hadoop_path_exists(spark, run_root):
        issues.append(
            f"run root already exists: {run_root}; choose a new run_id rather "
            "than overwriting an existing or partial run"
        )

    grain = read_required_parquet(spark, str(config["grain_path"]), "grain")
    panel = read_required_parquet(spark, str(config["panel_path"]), "panel")

    schema_data = schema_rows(grain, "grain", GRAIN_REQUIRED_COLUMNS)
    schema_data.extend(schema_rows(panel, "panel", PANEL_REQUIRED_COLUMNS))
    grain_missing = missing_columns(grain, GRAIN_REQUIRED_COLUMNS)
    panel_missing = missing_columns(panel, PANEL_REQUIRED_COLUMNS)
    if grain_missing:
        issues.append(
            f"grain schema missing {grain_missing}; found {sorted(grain.columns)}"
        )
    if panel_missing:
        issues.append(
            f"panel schema missing {panel_missing}; found {sorted(panel.columns)}"
        )
    if grain_missing or panel_missing:
        print("=== PREFLIGHT FAILED ===")
        for index, issue in enumerate(issues, 1):
            print(f"{index}. {issue}")
        raise M2ValidationError(
            "Required input schema is incomplete. Rebuild/inspect the M1 grain "
            "and panel before retrying."
        )
    print(
        "schema_validation=PASS "
        f"grain_columns={len(grain.columns)} panel_columns={len(panel.columns)}"
    )

    grain = grain.withColumn("ex_date", F.to_date("ex_date")).cache()
    panel = (
        panel.withColumn("ex_date", F.to_date("ex_date"))
        .withColumn("offset", F.col("offset").cast("integer"))
        .cache()
    )

    grain_count = grain.count()
    panel_count = panel.count()
    grain_key_count = grain.select("ticker", "ex_date").distinct().count()
    panel_key_count = panel.select("ticker", "ex_date", "offset").distinct().count()
    grain_null_keys = grain.filter(
        F.col("ticker").isNull() | F.col("ex_date").isNull()
    ).count()
    panel_null_keys = panel.filter(
        F.col("ticker").isNull()
        | F.col("ex_date").isNull()
        | F.col("offset").isNull()
    ).count()
    if grain_count != grain_key_count or grain_null_keys:
        issues.append(
            "grain key validation failed: "
            f"rows={grain_count}, distinct(ticker, ex_date)={grain_key_count}, "
            f"null_key_rows={grain_null_keys}"
        )
    if panel_count != panel_key_count or panel_null_keys:
        issues.append(
            "panel key validation failed: "
            f"rows={panel_count}, distinct(ticker, ex_date, offset)={panel_key_count}, "
            f"null_key_rows={panel_null_keys}"
        )

    coverage_row = grain.agg(
        F.min("ex_date").alias("min_ex_date"),
        F.max("ex_date").alias("max_ex_date"),
        F.countDistinct(F.date_format("ex_date", "yyyy-MM")).alias("distinct_ex_ym"),
    ).collect()[0]
    min_ex_date = coverage_row["min_ex_date"]
    max_ex_date = coverage_row["max_ex_date"]
    if min_ex_date is None or max_ex_date is None:
        issues.append("grain contains no usable ex_date values")
        history_span_days = 0
    else:
        history_span_days = (max_ex_date - min_ex_date).days
    if history_span_days < int(config["min_history_days"]):
        issues.append(
            f"history span is {history_span_days} days, below configured "
            f"min_history_days={config['min_history_days']}; the proof-window/"
            "full-history prerequisite is unresolved"
        )

    offset_row = panel.agg(
        F.min("offset").alias("minimum"), F.max("offset").alias("maximum")
    ).collect()[0]
    invalid_offsets = panel.filter(~F.col("offset").between(-5, 3)).count()
    if invalid_offsets:
        issues.append(
            f"panel contains {invalid_offsets} row(s) outside expected offsets -5..+3"
        )
    if (
        offset_row["minimum"] is None
        or offset_row["maximum"] is None
        or int(offset_row["minimum"]) > int(config["event_offset_min"])
        or int(offset_row["maximum"]) < int(config["event_offset_max"])
    ):
        issues.append(
            "panel does not cover the configured event-time offsets "
            f"{config['event_offset_min']}..{config['event_offset_max']}"
        )

    enriched, sic_info, metadata_schema = enrich_sic_description(
        spark, grain, config, issues
    )
    schema_data.extend(metadata_schema)
    enriched = add_derived_fields(enriched).cache()
    market_ticker_event_count = enriched.filter(
        F.col("ticker") == F.lit(market_ticker)
    ).count()
    metric_summary = metric_identity_summary(
        spark, enriched, float(config["metric_tolerance"]), issues
    ).cache()
    sample_funnel, sample_counts = build_sample_funnel(
        spark, enriched, market_ticker
    )
    sample_funnel = sample_funnel.cache()

    for stage in (
        "benchmark_excluded",
        "yield_usable",
        "volatility_usable",
        "liquidity_usable",
        "full_event_time_window",
    ):
        if sample_counts[stage] == 0:
            issues.append(f"analysis sample {stage!r} contains zero events")

    base = enriched.filter(base_condition(market_ticker)).cache()
    known_count = sample_counts["sic_known"]
    unknown_count = sample_counts["sic_unknown"]
    sic_total = known_count + unknown_count
    unknown_share = float(unknown_count / sic_total) if sic_total else None
    sic_coverage = spark.createDataFrame(
        [
            (
                str(sic_info["mode"]),
                int(known_count),
                int(unknown_count),
                unknown_share,
                int(sic_info["conflicting_ticker_count"]),
                int(sic_info["events_blocked_by_list_date"]),
                SIC_TEMPORAL_LIMITATION,
            )
        ],
        "sic_source string, known_event_count long, unknown_event_count long, "
        "unknown_event_share double, conflicting_ticker_count long, "
        "events_blocked_by_list_date long, temporal_limitation string",
    )

    coverage: Dict[str, Any] = {
        "min_ex_date": min_ex_date,
        "max_ex_date": max_ex_date,
        "history_span_days": history_span_days,
        "distinct_ex_ym": int(coverage_row["distinct_ex_ym"] or 0),
        "grain_row_count": grain_count,
        "panel_row_count": panel_count,
        "grain_distinct_key_count": grain_key_count,
        "panel_distinct_key_count": panel_key_count,
        "panel_min_offset": offset_row["minimum"],
        "panel_max_offset": offset_row["maximum"],
        "market_ticker": market_ticker,
        "market_ticker_event_count": market_ticker_event_count,
    }

    print("=== input coverage ===")
    for key, value in coverage.items():
        print(f"{key}={value}")
    print("=== SIC coverage ===")
    sic_coverage.show(truncate=False)
    print("=== metric identities ===")
    metric_summary.show(truncate=False)
    print("=== sample funnel ===")
    sample_funnel.orderBy("stage_order").show(truncate=False)

    if issues:
        print("=== PREFLIGHT FAILED ===")
        for index, issue in enumerate(issues, 1):
            print(f"{index}. {issue}")
        raise M2ValidationError(
            "Resolve the numbered preflight issue(s), then rerun the same "
            "preflight command. Final mode did not write outputs."
        )

    print("=== PREFLIGHT PASSED ===")
    if mode == "preflight":
        print("No output was written. Run --mode final with the same config and run_id.")
        return

    yield_sample = base.filter(F.col("div_yield").isNotNull()).cache()
    volatility_sample = base.filter(F.col("pre_vol").isNotNull()).cache()
    liquidity_sample = base.filter(
        F.col("pre_avg_dollar_volume").isNotNull()
    ).cache()
    stable_events = base.filter(F.col("n_bars") == 9).cache()

    boundary_rows: List[Tuple[Any, ...]] = []
    yield_bucketed, rows = add_quantile_bucket(
        spark,
        yield_sample,
        "yield",
        "div_yield",
        "div_yield_bucket",
        int(config["bucket_count"]),
    )
    boundary_rows.extend(rows)
    volatility_bucketed, rows = add_quantile_bucket(
        spark,
        volatility_sample,
        "volatility",
        "pre_vol",
        "pre_vol_bucket",
        int(config["bucket_count"]),
    )
    boundary_rows.extend(rows)
    liquidity_bucketed, rows = add_quantile_bucket(
        spark,
        liquidity_sample,
        "liquidity",
        "pre_avg_dollar_volume",
        "pre_avg_dollar_volume_bucket",
        int(config["bucket_count"]),
    )
    boundary_rows.extend(rows)
    bucket_boundaries = spark.createDataFrame(
        boundary_rows,
        "dimension string, source_column string, bucket_column string, "
        "bucket_label string, bucket_number integer, lower_bound_exclusive double, "
        "upper_bound_inclusive double, sample_min double, sample_max double, "
        "requested_bucket_count integer, actual_bucket_count integer, n_events long",
    )

    overall = grouped_summary(base)
    yield_summary = grouped_summary(yield_bucketed, "div_yield_bucket").orderBy(
        "div_yield_bucket"
    )
    volatility_summary = grouped_summary(
        volatility_bucketed, "pre_vol_bucket"
    ).orderBy("pre_vol_bucket")
    liquidity_summary = grouped_summary(
        liquidity_bucketed, "pre_avg_dollar_volume_bucket"
    ).orderBy("pre_avg_dollar_volume_bucket")
    sic_summary = (
        grouped_summary(base, "sic_description")
        .withColumn("low_n_flag", F.col("n_events") < int(config["min_cell_n"]))
        .orderBy(F.desc("n_events"), F.asc("sic_description"))
    )
    daily_summary = event_time_daily_summary(
        stable_events,
        panel,
        int(config["event_offset_min"]),
        int(config["event_offset_max"]),
    )
    observed_offsets = {
        int(row["offset"]) for row in daily_summary.select("offset").collect()
    }
    required_offsets = set(
        range(int(config["event_offset_min"]), int(config["event_offset_max"]) + 1)
    )
    if observed_offsets != required_offsets:
        raise M2ValidationError(
            "Event-time output is missing offsets after filtering non-null "
            f"abn_ret_cc: expected {sorted(required_offsets)}, observed "
            f"{sorted(observed_offsets)}"
        )
    overnight_summary = event_time_overnight_summary(base)

    model_features = select_handoff_columns(base, MODEL_FEATURE_COLUMNS)
    forbidden = validate_model_feature_columns(model_features.columns)
    if forbidden:
        raise M2ValidationError(
            f"Internal leakage assertion failed; model_features contains {forbidden}"
        )
    model_outcomes = select_handoff_columns(base, MODEL_OUTCOME_COLUMNS)

    handoff_key_mismatch = (
        model_features.select("event_id", "ticker", "ex_date")
        .subtract(model_outcomes.select("event_id", "ticker", "ex_date"))
        .limit(1)
        .count()
        + model_outcomes.select("event_id", "ticker", "ex_date")
        .subtract(model_features.select("event_id", "ticker", "ex_date"))
        .limit(1)
        .count()
    )
    if handoff_key_mismatch:
        raise M2ValidationError(
            "Internal handoff assertion failed: feature and outcome keys differ"
        )

    input_rows = [
        ("run_id", run_id, "Requested immutable run ID"),
        ("grain_path", str(config["grain_path"]), "M1 event-grain input"),
        ("panel_path", str(config["panel_path"]), "M1 event-panel input"),
        ("metadata_path", str(config["metadata_path"]), "SIC metadata fallback"),
        ("min_ex_date", str(min_ex_date), "Minimum event ex-date"),
        ("max_ex_date", str(max_ex_date), "Maximum event ex-date"),
        ("history_span_days", str(history_span_days), "Calendar-day history span"),
        (
            "distinct_ex_ym",
            str(coverage["distinct_ex_ym"]),
            "Distinct event year-month count",
        ),
        ("grain_row_count", str(grain_count), "Input grain rows"),
        ("panel_row_count", str(panel_count), "Input panel rows"),
        ("sic_source", str(sic_info["mode"]), "grain or metadata enrichment"),
        (
            "market_ticker_excluded",
            market_ticker,
            "Benchmark excluded from all analytical samples and handoffs",
        ),
        (
            "market_ticker_event_count",
            str(market_ticker_event_count),
            "Input grain rows matching the excluded benchmark ticker",
        ),
        ("run_root", run_root, "Run-versioned HDFS output root"),
    ]
    input_summary = spark.createDataFrame(
        input_rows, "metric string, value string, description string"
    )
    schema_summary = spark.createDataFrame(
        schema_data,
        "dataset string, column string, data_type string, required boolean",
    )

    output_paths = {
        "analysis_base": f"{run_root}/analysis_base",
        "model_features": f"{run_root}/model_features",
        "model_outcomes": f"{run_root}/model_outcomes",
        "audit_input_summary": f"{run_root}/audit/input_summary",
        "audit_schema_summary": f"{run_root}/audit/schema_summary",
        "audit_sample_funnel": f"{run_root}/audit/sample_funnel",
        "audit_sic_coverage": f"{run_root}/audit/sic_coverage",
        "audit_bucket_boundaries": f"{run_root}/audit/bucket_boundaries",
        "audit_metric_identities": f"{run_root}/audit/metric_identities",
        "core_overall": f"{run_root}/core/overall",
        "core_yield": f"{run_root}/core/yield",
        "core_volatility": f"{run_root}/core/volatility",
        "core_liquidity": f"{run_root}/core/liquidity",
        "core_sic_description": f"{run_root}/core/sic_description",
        "core_event_time_daily": f"{run_root}/core/event_time_daily",
        "core_event_time_overnight": f"{run_root}/core/event_time_overnight",
        "manifest_run_metadata": f"{run_root}/manifest/run_metadata",
    }
    manifest = create_manifest(
        spark, config, run_id, run_root, coverage, sic_info, output_paths
    )

    tables = {
        "analysis_base": base,
        "model_features": model_features,
        "model_outcomes": model_outcomes,
        "audit_input_summary": input_summary,
        "audit_schema_summary": schema_summary,
        "audit_sample_funnel": sample_funnel,
        "audit_sic_coverage": sic_coverage,
        "audit_bucket_boundaries": bucket_boundaries,
        "audit_metric_identities": metric_summary,
        "core_overall": overall,
        "core_yield": yield_summary,
        "core_volatility": volatility_summary,
        "core_liquidity": liquidity_summary,
        "core_sic_description": sic_summary,
        "core_event_time_daily": daily_summary,
        "core_event_time_overnight": overnight_summary,
        "manifest_run_metadata": manifest,
    }
    for name, frame in tables.items():
        destination = output_paths[name]
        print(f"writing {name} -> {destination}")
        write_parquet(frame, destination)

    print(f"=== FINAL WRITE COMPLETE: {run_root} ===")
    print(CORPORATE_ACTION_LIMITATION)
    print(SIC_TEMPORAL_LIMITATION)
    print(TIME_DECAY_DEPENDENCE_LIMITATION)


def main(argv: Optional[Sequence[str]] = None) -> int:
    spark = None
    try:
        args = parse_args(argv)
        validate_run_id(args.run_id)
        config = resolve_runtime_config(load_config(args.config))
        require_pyspark()
        spark = (
            SparkSession.builder.appName(f"divcap-m2-cross-sectional-{args.run_id}")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", "64")
        run_job(spark, config, args.run_id, args.mode)
        return 0
    except M2ValidationError as exc:
        print(f"M2 VALIDATION ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
