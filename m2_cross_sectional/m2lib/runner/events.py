"""Event-time outputs, model handoffs, and run-manifest construction."""

from __future__ import annotations

from .summaries import *  # noqa: F401,F403

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
            F.to_date("bar_date").alias("bar_date"),
            F.col("offset").cast("integer").alias("offset"),
            F.col("abn_ret_cc").cast("double").alias("abn_ret_cc"),
        )
        .filter(F.col("offset").between(minimum_offset, maximum_offset))
        .filter(F.col("abn_ret_cc").isNotNull())
    )
    joined = usable_panel.join(keys, ["ticker", "ex_date"], "inner").cache()
    date_concentration = (
        joined.groupBy("offset", "bar_date")
        .agg(F.count("*").alias("date_events"))
        .groupBy("offset")
        .agg(
            F.count("*").cast("long").alias("n_calendar_dates"),
            F.max("date_events").cast("long").alias("top_calendar_date_events"),
        )
    )
    summary = (
        joined
        .groupBy("offset")
        .agg(
            F.count("*").cast("long").alias("n_events"),
            F.countDistinct("ticker").cast("long").alias("n_tickers"),
            F.count("abn_ret_cc").cast("long").alias("n_abn_ret_cc"),
            F.avg("abn_ret_cc").alias("mean_abn_ret_cc"),
            F.stddev_samp("abn_ret_cc").alias("stddev_abn_ret_cc"),
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
        .withColumn(
            "se_abn_ret_cc",
            F.when(
                (F.col("n_abn_ret_cc") > 1)
                & F.col("stddev_abn_ret_cc").isNotNull(),
                F.col("stddev_abn_ret_cc")
                / F.sqrt(F.col("n_abn_ret_cc").cast("double")),
            ),
        )
        .withColumn(
            "ci95_low_abn_ret_cc",
            F.when(
                F.col("se_abn_ret_cc").isNotNull(),
                F.col("mean_abn_ret_cc") - 1.96 * F.col("se_abn_ret_cc"),
            ),
        )
        .withColumn(
            "ci95_high_abn_ret_cc",
            F.when(
                F.col("se_abn_ret_cc").isNotNull(),
                F.col("mean_abn_ret_cc") + 1.96 * F.col("se_abn_ret_cc"),
            ),
        )
        .join(date_concentration, "offset", "left")
        .withColumn(
            "top_calendar_date_share",
            F.col("top_calendar_date_events") / F.col("n_events").cast("double"),
        )
        .orderBy("offset")
    )
    return summary


def event_time_overnight_summary(base: DataFrame) -> DataFrame:
    return (
        base.agg(
            F.count("*").cast("long").alias("n_events"),
            F.countDistinct("ticker").cast("long").alias("n_tickers"),
            F.count("stock_overnight_ret").cast("long").alias(
                "n_stock_overnight_ret"
            ),
            F.avg("stock_overnight_ret").alias("mean_stock_overnight_ret"),
            F.stddev_samp("stock_overnight_ret").alias(
                "stddev_stock_overnight_ret"
            ),
            F.expr("percentile_approx(stock_overnight_ret, 0.50, 10000)").alias(
                "median_stock_overnight_ret"
            ),
            F.expr("percentile_approx(stock_overnight_ret, 0.25, 10000)").alias(
                "p25_stock_overnight_ret"
            ),
            F.expr("percentile_approx(stock_overnight_ret, 0.75, 10000)").alias(
                "p75_stock_overnight_ret"
            ),
            F.count("abn_overnight_ret").cast("long").alias(
                "n_abn_overnight_ret"
            ),
            F.avg("abn_overnight_ret").alias("mean_abn_overnight_ret"),
            F.stddev_samp("abn_overnight_ret").alias(
                "stddev_abn_overnight_ret"
            ),
            F.expr("percentile_approx(abn_overnight_ret, 0.50, 10000)").alias(
                "median_abn_overnight_ret"
            ),
            F.expr("percentile_approx(abn_overnight_ret, 0.25, 10000)").alias(
                "p25_abn_overnight_ret"
            ),
            F.expr("percentile_approx(abn_overnight_ret, 0.75, 10000)").alias(
                "p75_abn_overnight_ret"
            ),
            F.count("capture_ret").cast("long").alias("n_capture_ret"),
            F.avg("capture_ret").alias("mean_capture_ret"),
            F.expr("percentile_approx(capture_ret, 0.50, 10000)").alias(
                "median_capture_ret"
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
    repo_root = Path(__file__).resolve().parents[3]
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
    pseudo_info: Mapping[str, Any],
    sector_counts: Mapping[str, Any],
    output_paths: Mapping[str, str],
) -> DataFrame:
    application_id = getattr(spark.sparkContext, "applicationId", "unavailable")
    row = {
        "run_id": run_id,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "grain_path": str(config["grain_path"]),
        "panel_path": str(config["panel_path"]),
        "metadata_path": str(config["metadata_path"]),
        "pseudo_sector_path": str(config["pseudo_sector_path"]),
        "run_root": run_root,
        "min_ex_date": str(coverage["min_ex_date"]),
        "max_ex_date": str(coverage["max_ex_date"]),
        "history_span_days": int(coverage["history_span_days"]),
        "grain_row_count": int(coverage["grain_row_count"]),
        "panel_row_count": int(coverage["panel_row_count"]),
        "sic_enrichment_mode": str(sic_info["mode"]),
        "base_events": int(sector_counts["base_events"]),
        "base_tickers": int(sector_counts["base_tickers"]),
        "direct_sic_known_events": int(sector_counts["direct_known_events"]),
        "direct_sic_unknown_events": int(sector_counts["direct_unknown_events"]),
        "pseudo_sector_label_level": str(config["pseudo_sector_label_level"]),
        "pseudo_source_rows": int(pseudo_info["source_rows"]),
        "pseudo_source_tickers": int(pseudo_info["source_tickers"]),
        "pseudo_source_labels": int(pseudo_info["source_labels"]),
        "pseudo_source_observed_label_levels_json": str(
            pseudo_info["observed_label_levels_json"]
        ),
        "pseudo_source_schema_json": str(pseudo_info["source_schema_json"]),
        "pseudo_source_modification_time_utc": str(
            pseudo_info["source_modification_time_utc"]
        ),
        "pseudo_source_duplicate_identical_rows": int(
            pseudo_info["duplicate_identical_row_count"]
        ),
        "pseudo_source_blank_label_rows": int(pseudo_info["blank_label_count"]),
        "pseudo_source_conflicting_tickers": int(
            pseudo_info["conflicting_ticker_count"]
        ),
        "pseudo_join_row_delta": int(sector_counts["join_row_delta"]),
        "pseudo_recovered_events": int(sector_counts["pseudo_recovered_events"]),
        "pseudo_recovery_rate_of_sic_unknown": float(
            sector_counts["pseudo_recovery_rate_of_sic_unknown"]
        ),
        "still_unresolved_events": int(sector_counts["still_unresolved_events"]),
        "coverage_after_recovery_share": float(
            sector_counts["coverage_after_recovery_share"]
        ),
        "pseudo_model_version_available": bool(
            pseudo_info["model_version_available"]
        ),
        "pseudo_prediction_confidence_available": bool(
            pseudo_info["prediction_confidence_available"]
        ),
        "pseudo_training_timestamp_available": bool(
            pseudo_info["training_timestamp_available"]
        ),
        "upstream_documented_model_metrics": UPSTREAM_MODEL_METRICS,
        "taxonomy_separation_statement": TAXONOMY_SEPARATION_STATEMENT,
        "market_ticker_excluded": str(config["market_ticker"]),
        "market_ticker_event_count": int(coverage["market_ticker_event_count"]),
        "sic_temporal_limitation": SIC_TEMPORAL_LIMITATION,
        "corporate_action_limitation": CORPORATE_ACTION_LIMITATION,
        "time_decay_dependence_limitation": TIME_DECAY_DEPENDENCE_LIMITATION,
        "gross_cost_limitation": GROSS_COST_LIMITATION,
        "dependence_limitation": DEPENDENCE_LIMITATION,
        "pseudo_model_limitation": PSEUDO_MODEL_LIMITATION,
        "pseudo_provenance_limitation": PSEUDO_PROVENANCE_LIMITATION,
        "requested_bucket_count": int(config["bucket_count"]),
        "min_cell_n": int(config["min_cell_n"]),
        "min_pseudo_cell_n": int(config["min_pseudo_cell_n"]),
        "min_report_tickers": int(config["min_report_tickers"]),
        "report_top_sic_n": int(config["report_top_sic_n"]),
        "report_top_pseudo_n": int(config["report_top_pseudo_n"]),
        "report_numeric_labels": bool(config["report_numeric_labels"]),
        "insight_min_abs_bps": float(config["insight_min_abs_bps"]),
        "numeric_chart_contract_version": NUMERIC_CHART_CONTRACT_VERSION,
        "insight_generation_version": INSIGHT_GENERATION_VERSION,
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
