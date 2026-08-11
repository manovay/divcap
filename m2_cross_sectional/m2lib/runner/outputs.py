"""Construction and validation of final aggregate and handoff tables."""

from __future__ import annotations

from .events import *  # noqa: F401,F403

def build_analysis_tables(
    *,
    spark: SparkSession,
    config: Mapping[str, Any],
    run_id: str,
    run_root: str,
    market_ticker: str,
    min_ex_date: Any,
    max_ex_date: Any,
    history_span_days: int,
    grain_count: int,
    panel_count: int,
    market_ticker_event_count: int,
    panel: DataFrame,
    analysis_base: DataFrame,
    sample_funnel: DataFrame,
    schema_data: Sequence[Tuple[Any, ...]],
    sector_counts: Mapping[str, Any],
    coverage: Mapping[str, Any],
    sic_info: Mapping[str, Any],
    pseudo_info: Mapping[str, Any],
    sic_coverage: DataFrame,
    pseudo_coverage: DataFrame,
    pseudo_contract: DataFrame,
    sector_bridge: DataFrame,
    metric_summary: DataFrame,
) -> Tuple[Dict[str, DataFrame], DataFrame, DataFrame, Dict[str, str]]:
    yield_sample = analysis_base.filter(F.col("div_yield").isNotNull()).cache()
    volatility_sample = analysis_base.filter(F.col("pre_vol").isNotNull()).cache()
    liquidity_sample = analysis_base.filter(
        F.col("pre_avg_dollar_volume").isNotNull()
    ).cache()
    stable_events = analysis_base.filter(F.col("n_bars") == 9).cache()
    direct_known = analysis_base.filter(F.col("direct_sic_known")).cache()
    pseudo_recovered = analysis_base.filter(F.col("pseudo_recovered")).cache()

    boundary_rows: List[Tuple[Any, ...]] = []
    yield_bucketed, rows = add_quantile_bucket(
        spark, yield_sample, "yield", "div_yield", "div_yield_bucket",
        int(config["bucket_count"]),
    )
    boundary_rows.extend(rows)
    volatility_bucketed, rows = add_quantile_bucket(
        spark, volatility_sample, "volatility", "pre_vol", "pre_vol_bucket",
        int(config["bucket_count"]),
    )
    boundary_rows.extend(rows)
    liquidity_bucketed, rows = add_quantile_bucket(
        spark, liquidity_sample, "liquidity", "pre_avg_dollar_volume",
        "pre_avg_dollar_volume_bucket", int(config["bucket_count"]),
    )
    boundary_rows.extend(rows)
    bucket_boundaries = spark.createDataFrame(
        boundary_rows,
        "dimension string, source_column string, bucket_column string, "
        "bucket_label string, bucket_number integer, lower_bound_exclusive double, "
        "upper_bound_inclusive double, sample_min double, sample_max double, "
        "requested_bucket_count integer, actual_bucket_count integer, n_events long",
    )
    min_n = int(config["min_cell_n"])
    min_pseudo_n = int(config["min_pseudo_cell_n"])
    min_tickers = int(config["min_report_tickers"])
    overall = grouped_summary(analysis_base, min_cell_n=min_n,
                              min_report_tickers=min_tickers)
    yield_summary = grouped_summary(
        yield_bucketed, "div_yield_bucket", min_n, min_tickers
    ).orderBy("div_yield_bucket")
    volatility_summary = grouped_summary(
        volatility_bucketed, "pre_vol_bucket", min_n, min_tickers
    ).orderBy("pre_vol_bucket")
    liquidity_summary = grouped_summary(
        liquidity_bucketed, "pre_avg_dollar_volume_bucket", min_n, min_tickers
    ).orderBy("pre_avg_dollar_volume_bucket")
    sic_summary = grouped_summary(
        analysis_base, "sic_description", min_n, min_tickers
    ).orderBy(F.desc("n_events"), F.asc("sic_description"))
    direct_known_summary = grouped_summary(
        direct_known, "sic_description", min_n, min_tickers
    )
    pseudo_summary = (
        grouped_summary(
            pseudo_recovered, "pseudo_sector", min_pseudo_n, min_tickers
        )
        .withColumn("label_level", F.lit(str(config["pseudo_sector_label_level"])))
        .select("pseudo_sector", "label_level", *GROUP_SUMMARY_COLUMNS)
        .orderBy(F.desc("n_events"), F.asc("pseudo_sector"))
    )

    assert_summary_population(overall, sector_counts["base_events"], "overall")
    assert_summary_population(yield_summary, yield_sample.count(), "yield")
    assert_summary_population(
        volatility_summary, volatility_sample.count(), "volatility"
    )
    assert_summary_population(
        liquidity_summary, liquidity_sample.count(), "liquidity"
    )
    assert_summary_population(
        sic_summary, sector_counts["base_events"], "direct SIC"
    )
    assert_summary_population(
        pseudo_summary, sector_counts["pseudo_recovered_events"], "pseudo-sector"
    )
    dimension_diagnostics = build_dimension_diagnostics(
        spark,
        (
            ("yield", yield_summary, "div_yield_bucket"),
            ("volatility", volatility_summary, "pre_vol_bucket"),
            (
                "liquidity",
                liquidity_summary,
                "pre_avg_dollar_volume_bucket",
            ),
        ),
    )
    sector_diagnostics = build_sector_diagnostics(
        spark, direct_known_summary, pseudo_summary, sector_counts
    )

    daily_summary = event_time_daily_summary(
        stable_events, panel, int(config["event_offset_min"]),
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
            "Event-time output is missing offsets after non-null filtering: "
            f"expected={sorted(required_offsets)}, observed={sorted(observed_offsets)}"
        )
    overnight_summary = event_time_overnight_summary(analysis_base)

    model_features = select_handoff_columns(analysis_base, MODEL_FEATURE_COLUMNS)
    forbidden = validate_model_feature_columns(model_features.columns)
    if forbidden:
        raise M2ValidationError(
            f"Internal leakage assertion failed; model_features contains {forbidden}"
        )
    model_outcomes = select_handoff_columns(analysis_base, MODEL_OUTCOME_COLUMNS)
    handoff_key_mismatch = (
        model_features.select("event_id", "ticker", "ex_date")
        .subtract(model_outcomes.select("event_id", "ticker", "ex_date"))
        .limit(1).count()
        + model_outcomes.select("event_id", "ticker", "ex_date")
        .subtract(model_features.select("event_id", "ticker", "ex_date"))
        .limit(1).count()
    )
    if handoff_key_mismatch:
        raise M2ValidationError(
            "Internal handoff assertion failed: feature and outcome keys differ"
        )

    input_rows = [
        ("run_id", run_id, "Requested immutable run ID"),
        ("grain_path", str(config["grain_path"]), "M1 event-grain input"),
        ("panel_path", str(config["panel_path"]), "M1 event-panel input"),
        ("metadata_path", str(config["metadata_path"]), "Direct SIC fallback"),
        (
            "pseudo_sector_path", str(config["pseudo_sector_path"]),
            "Model-predicted pseudo-sector input",
        ),
        (
            "pseudo_sector_label_level",
            str(config["pseudo_sector_label_level"]),
            "Configured pseudo-sector label level",
        ),
        ("min_ex_date", str(min_ex_date), "Minimum event ex-date"),
        ("max_ex_date", str(max_ex_date), "Maximum event ex-date"),
        ("history_span_days", str(history_span_days), "Calendar-day history span"),
        ("distinct_ex_ym", str(coverage["distinct_ex_ym"]), "Event year-months"),
        ("grain_row_count", str(grain_count), "Input grain rows"),
        ("panel_row_count", str(panel_count), "Input panel rows"),
        ("base_events", str(sector_counts["base_events"]), "Accepted base events"),
        ("base_tickers", str(sector_counts["base_tickers"]), "Accepted base tickers"),
        ("sic_source", str(sic_info["mode"]), "grain or metadata enrichment"),
        (
            "market_ticker_excluded", market_ticker,
            "Benchmark excluded from analyses and model handoffs",
        ),
        (
            "market_ticker_event_count", str(market_ticker_event_count),
            "Input grain rows matching the excluded benchmark",
        ),
        ("run_root", run_root, "Immutable HDFS output root"),
    ]
    input_summary = spark.createDataFrame(
        input_rows, "metric string, value string, description string"
    )
    schema_summary = spark.createDataFrame(
        schema_data,
        "dataset string, column string, data_type string, required boolean",
    )
    output_paths = {
        name: f"{run_root}/{relative}"
        for name, relative in OUTPUT_RELATIVE_PATHS.items()
    }
    manifest = create_manifest(
        spark, config, run_id, run_root, coverage, sic_info, pseudo_info,
        sector_counts, output_paths,
    )
    tables = {
        "analysis_base": analysis_base,
        "model_features": model_features,
        "model_outcomes": model_outcomes,
        "audit_input_summary": input_summary,
        "audit_schema_summary": schema_summary,
        "audit_sample_funnel": sample_funnel,
        "audit_sic_coverage": sic_coverage,
        "audit_pseudo_sector_coverage": pseudo_coverage,
        "audit_pseudo_sector_contract": pseudo_contract,
        "audit_sector_coverage_bridge": sector_bridge,
        "audit_bucket_boundaries": bucket_boundaries,
        "audit_metric_identities": metric_summary,
        "audit_dimension_diagnostics": dimension_diagnostics,
        "audit_sector_diagnostics": sector_diagnostics,
        "core_overall": overall,
        "core_yield": yield_summary,
        "core_volatility": volatility_summary,
        "core_liquidity": liquidity_summary,
        "core_sic_description": sic_summary,
        "core_pseudo_sector": pseudo_summary,
        "core_event_time_daily": daily_summary,
        "core_event_time_overnight": overnight_summary,
        "manifest_run_metadata": manifest,
    }
    if set(tables) != set(OUTPUT_RELATIVE_PATHS):
        raise M2ValidationError(
            "Internal output registry mismatch: tables="
            f"{sorted(tables)}, registry={sorted(OUTPUT_RELATIVE_PATHS)}"
        )
    empty_tables = [name for name, frame in tables.items() if frame.limit(1).count() == 0]
    if empty_tables:
        raise M2ValidationError(
            f"Required final aggregate(s) are empty: {empty_tables}; no output written"
        )

    return tables, dimension_diagnostics, sector_diagnostics, output_paths
