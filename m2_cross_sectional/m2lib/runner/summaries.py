"""Grouped summaries, diagnostics, and deterministic quantile buckets."""

from __future__ import annotations

from .sectors import *  # noqa: F401,F403

def summary_expressions() -> List[Any]:
    return [
        F.count("*").cast("long").alias("n_events"),
        F.countDistinct("ticker").cast("long").alias("n_tickers"),
        F.count("capture_ret_abn").cast("long").alias("n_capture_ret_abn"),
        F.count("capture_ret").cast("long").alias("n_capture_ret"),
        F.count("drop_ratio").cast("long").alias("n_drop_ratio"),
        F.avg("capture_ret_abn").alias("mean_capture_ret_abn"),
        F.stddev_samp("capture_ret_abn").alias("stddev_capture_ret_abn"),
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
        F.avg("drop_ratio").alias("mean_drop_ratio"),
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


def grouped_summary(
    frame: DataFrame,
    group_column: Optional[str] = None,
    min_cell_n: int = 1,
    min_report_tickers: int = 1,
) -> DataFrame:
    totals = frame.agg(
        F.count("*").cast("long").alias("analysis_events"),
        F.countDistinct("ticker").cast("long").alias("analysis_tickers"),
    ).collect()[0]
    analysis_events = int(totals["analysis_events"] or 0)
    analysis_tickers = int(totals["analysis_tickers"] or 0)
    if group_column is None:
        summary = (
            frame.agg(*summary_expressions())
            .withColumn("analysis_group", F.lit("ALL"))
        )
        prefix = ["analysis_group"]
    else:
        summary = frame.groupBy(group_column).agg(*summary_expressions())
        prefix = [group_column]

    valid_interval = (
        (F.col("n_capture_ret_abn") > 1)
        & F.col("stddev_capture_ret_abn").isNotNull()
    )
    summary = (
        summary.withColumn(
            "event_share_of_analysis",
            F.when(
                F.lit(analysis_events) > 0,
                F.col("n_events") / F.lit(float(analysis_events)),
            ),
        )
        .withColumn(
            "ticker_share_of_analysis",
            F.when(
                F.lit(analysis_tickers) > 0,
                F.col("n_tickers") / F.lit(float(analysis_tickers)),
            ),
        )
        .withColumn(
            "se_capture_ret_abn",
            F.when(
                valid_interval,
                F.col("stddev_capture_ret_abn")
                / F.sqrt(F.col("n_capture_ret_abn").cast("double")),
            ),
        )
        .withColumn(
            "ci95_low_capture_ret_abn",
            F.when(
                valid_interval,
                F.col("mean_capture_ret_abn")
                - F.lit(1.96) * F.col("se_capture_ret_abn"),
            ),
        )
        .withColumn(
            "ci95_high_capture_ret_abn",
            F.when(
                valid_interval,
                F.col("mean_capture_ret_abn")
                + F.lit(1.96) * F.col("se_capture_ret_abn"),
            ),
        )
        .withColumn("low_n_flag", F.col("n_events") < F.lit(int(min_cell_n)))
        .withColumn(
            "low_ticker_flag",
            F.col("n_tickers") < F.lit(int(min_report_tickers)),
        )
        .withColumn(
            "report_eligible_flag",
            (~F.col("low_n_flag")) & (~F.col("low_ticker_flag")),
        )
    )
    return summary.select(*prefix, *GROUP_SUMMARY_COLUMNS)


def assert_summary_population(
    summary: DataFrame, population_events: int, label: str
) -> None:
    summarized = summary.agg(F.sum("n_events").alias("n_events")).collect()[0][
        "n_events"
    ]
    summarized_events = int(summarized or 0)
    if summarized_events != int(population_events):
        raise M2ValidationError(
            f"{label} summary population mismatch: summary_events="
            f"{summarized_events}, expected_events={population_events}"
        )


def build_dimension_diagnostics(
    spark: SparkSession,
    summaries: Sequence[Tuple[str, DataFrame, str]],
) -> DataFrame:
    rows = []
    for dimension, summary, label_column in summaries:
        records = [row.asDict() for row in summary.orderBy(label_column).collect()]
        if not records:
            raise M2ValidationError(
                f"Cannot build {dimension} diagnostics from an empty summary"
            )
        diagnostic = ordered_diagnostic_from_records(
            dimension, records, label_column
        )
        rows.append(tuple(diagnostic[name] for name in (
            "dimension",
            "actual_bucket_count",
            "low_bucket_mean_bps",
            "high_bucket_mean_bps",
            "high_minus_low_bps",
            "mean_range_bps",
            "median_range_bps",
            "positive_rate_range_pp",
            "monotonic_step_count",
            "spearman_bucket_mean",
        )))
    return spark.createDataFrame(
        rows,
        "dimension string, actual_bucket_count integer, low_bucket_mean_bps double, "
        "high_bucket_mean_bps double, high_minus_low_bps double, "
        "mean_range_bps double, median_range_bps double, "
        "positive_rate_range_pp double, monotonic_step_count integer, "
        "spearman_bucket_mean double",
    )


def sector_diagnostic_from_records(
    taxonomy: str,
    records: Sequence[Mapping[str, Any]],
    group_column: str,
    analysis_events: int,
    analysis_tickers: int,
) -> Dict[str, Any]:
    eligible = [
        row for row in records if bool(row.get("report_eligible_flag", False))
    ]
    sorted_counts = sorted(
        (int(row["n_events"]) for row in records), reverse=True
    )
    highest = (
        max(eligible, key=lambda row: (float(row["mean_capture_ret_abn"]), str(row[group_column])))
        if eligible
        else None
    )
    lowest = (
        min(eligible, key=lambda row: (float(row["mean_capture_ret_abn"]), str(row[group_column])))
        if eligible
        else None
    )
    means = [float(row["mean_capture_ret_abn"]) for row in eligible]
    return {
        "taxonomy": taxonomy,
        "analysis_events": int(analysis_events),
        "analysis_tickers": int(analysis_tickers),
        "category_count": len(records),
        "eligible_category_count": len(eligible),
        "top_1_event_share": (
            sorted_counts[0] / analysis_events if sorted_counts and analysis_events else None
        ),
        "top_5_event_share": (
            sum(sorted_counts[:5]) / analysis_events
            if sorted_counts and analysis_events
            else None
        ),
        "highest_observed_eligible_group": (
            str(highest[group_column]) if highest else None
        ),
        "highest_observed_mean_bps": (
            float(highest["mean_capture_ret_abn"]) * 10000.0 if highest else None
        ),
        "highest_observed_n_events": int(highest["n_events"]) if highest else None,
        "highest_observed_n_tickers": int(highest["n_tickers"]) if highest else None,
        "lowest_observed_eligible_group": (
            str(lowest[group_column]) if lowest else None
        ),
        "lowest_observed_mean_bps": (
            float(lowest["mean_capture_ret_abn"]) * 10000.0 if lowest else None
        ),
        "lowest_observed_n_events": int(lowest["n_events"]) if lowest else None,
        "lowest_observed_n_tickers": int(lowest["n_tickers"]) if lowest else None,
        "eligible_mean_range_bps": (
            (max(means) - min(means)) * 10000.0 if means else None
        ),
    }


def build_sector_diagnostics(
    spark: SparkSession,
    direct_known_summary: DataFrame,
    pseudo_summary: DataFrame,
    sector_counts: Mapping[str, Any],
) -> DataFrame:
    direct_records = [row.asDict() for row in direct_known_summary.collect()]
    pseudo_records = [row.asDict() for row in pseudo_summary.collect()]
    diagnostics = [
        sector_diagnostic_from_records(
            "direct_sic_current_reference",
            direct_records,
            "sic_description",
            int(sector_counts["direct_known_events"]),
            int(sector_counts["direct_known_tickers"]),
        ),
        sector_diagnostic_from_records(
            "model_predicted_pseudo_sector",
            pseudo_records,
            "pseudo_sector",
            int(sector_counts["pseudo_recovered_events"]),
            int(sector_counts["pseudo_recovered_tickers"]),
        ),
    ]
    schema = (
        "taxonomy string, analysis_events long, analysis_tickers long, "
        "category_count integer, eligible_category_count integer, "
        "top_1_event_share double, top_5_event_share double, "
        "highest_observed_eligible_group string, highest_observed_mean_bps double, "
        "highest_observed_n_events long, highest_observed_n_tickers long, "
        "lowest_observed_eligible_group string, lowest_observed_mean_bps double, "
        "lowest_observed_n_events long, lowest_observed_n_tickers long, "
        "eligible_mean_range_bps double"
    )
    ordered_names = [part.strip().split()[0] for part in schema.split(",")]
    return spark.createDataFrame(
        [tuple(row[name] for name in ordered_names) for row in diagnostics], schema
    )


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
