"""Top-level M2 preflight, orchestration, and immutable HDFS writes."""

from __future__ import annotations

from .events import *  # noqa: F401,F403
from .outputs import build_analysis_tables

def run_job(
    spark: SparkSession,
    config: Mapping[str, Any],
    run_id: str,
    mode: str,
) -> None:
    issues: List[str] = []
    run_root = f"{str(config['output_root']).rstrip('/')}/{run_id}"
    market_ticker = str(config["market_ticker"]).strip()

    print("=== M2 cross-sectional V2 preflight ===")
    print(f"mode={mode}")
    print(f"run_id={run_id}")
    print(f"grain_path={config['grain_path']}")
    print(f"panel_path={config['panel_path']}")
    print(f"metadata_path={config['metadata_path']}")
    print(f"pseudo_sector_path={config['pseudo_sector_path']}")
    print(f"pseudo_sector_label_level={config['pseudo_sector_label_level']}")
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
        raise M2ValidationError(
            "Required input schema is incomplete. Inspect the listed columns and "
            "rebuild the M1 grain/panel before retrying preflight."
        )

    grain = grain.withColumn("ex_date", F.to_date("ex_date")).cache()
    panel = (
        panel.withColumn("ex_date", F.to_date("ex_date"))
        .withColumn("bar_date", F.to_date("bar_date"))
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
    history_span_days = (
        (max_ex_date - min_ex_date).days
        if min_ex_date is not None and max_ex_date is not None
        else 0
    )
    if min_ex_date is None or max_ex_date is None:
        issues.append("grain contains no usable ex_date values")
    if history_span_days < int(config["min_history_days"]):
        issues.append(
            f"history span is {history_span_days} days, below configured "
            f"min_history_days={config['min_history_days']}; the full-history "
            "prerequisite is unresolved"
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
            "panel does not cover configured event-time offsets "
            f"{config['event_offset_min']}..{config['event_offset_max']}"
        )

    enriched, sic_info, metadata_schema = enrich_sic_description(
        spark, grain, config, issues
    )
    schema_data.extend(metadata_schema)
    enriched = add_derived_fields(enriched).cache()
    pseudo_lookup, pseudo_contract, pseudo_info, pseudo_schema = load_pseudo_sector(
        spark, config
    )
    schema_data.extend(pseudo_schema)
    market_ticker_event_count = enriched.filter(
        F.col("ticker") == F.lit(market_ticker)
    ).count()
    metric_summary = metric_identity_summary(
        spark, enriched, float(config["metric_tolerance"]), issues
    ).cache()
    sample_funnel, sample_counts = build_sample_funnel(
        spark, enriched, market_ticker
    )
    base = enriched.filter(base_condition(market_ticker)).cache()
    analysis_base, pseudo_coverage, sector_bridge, sector_counts = (
        enrich_sector_states(spark, base, pseudo_lookup, pseudo_info)
    )

    next_order = len(sample_counts) + 1
    pseudo_funnel_rows = [
        (
            next_order,
            "pseudo_recovered",
            "analysis_population",
            int(sector_counts["pseudo_recovered_events"]),
            int(sector_counts["base_events"]),
            float(
                sector_counts["pseudo_recovered_events"]
                / sector_counts["base_events"]
            ),
            float(
                sector_counts["pseudo_recovered_events"]
                / sample_counts["raw"]
            ),
            "Direct-SIC-unknown base events recovered by configured-level "
            "model-predicted pseudo-sector",
        ),
        (
            next_order + 1,
            "still_unresolved",
            "analysis_population",
            int(sector_counts["still_unresolved_events"]),
            int(sector_counts["base_events"]),
            float(
                sector_counts["still_unresolved_events"]
                / sector_counts["base_events"]
            ),
            float(
                sector_counts["still_unresolved_events"] / sample_counts["raw"]
            ),
            "Direct-SIC-unknown base events without a valid pseudo-sector match",
        ),
    ]
    sample_funnel = sample_funnel.unionByName(
        spark.createDataFrame(pseudo_funnel_rows, sample_funnel.schema)
    ).cache()

    for stage in (
        "benchmark_excluded",
        "yield_usable",
        "volatility_usable",
        "liquidity_usable",
        "full_event_time_window",
    ):
        if sample_counts[stage] == 0:
            issues.append(f"analysis sample {stage!r} contains zero events")

    known_count = int(sector_counts["direct_known_events"])
    unknown_count = int(sector_counts["direct_unknown_events"])
    unknown_share = (
        float(unknown_count / sector_counts["base_events"])
        if sector_counts["base_events"]
        else None
    )
    sic_coverage = spark.createDataFrame(
        [
            (
                str(sic_info["mode"]),
                known_count,
                unknown_count,
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
    print("=== direct SIC coverage ===")
    sic_coverage.show(truncate=False)
    print("=== pseudo-sector source contract ===")
    pseudo_contract.show(truncate=False, vertical=True)
    print("=== sector coverage recovery ===")
    pseudo_coverage.show(truncate=False, vertical=True)
    sector_bridge.orderBy("state_order").show(truncate=False)
    print("=== metric identities ===")
    metric_summary.show(truncate=False)
    print("=== sample funnel and analysis populations ===")
    sample_funnel.orderBy("stage_order").show(truncate=False)

    if issues:
        print("=== PREFLIGHT FAILED ===")
        for index, issue in enumerate(issues, 1):
            print(f"{index}. {issue}")
        raise M2ValidationError(
            "Resolve the numbered preflight issue(s), inspect the named path or "
            "source of truth, and retry. No final output was written."
        )

    tables, dimension_diagnostics, sector_diagnostics, output_paths = (
        build_analysis_tables(
            spark=spark,
            config=config,
            run_id=run_id,
            run_root=run_root,
            market_ticker=market_ticker,
            min_ex_date=min_ex_date,
            max_ex_date=max_ex_date,
            history_span_days=history_span_days,
            grain_count=grain_count,
            panel_count=panel_count,
            market_ticker_event_count=market_ticker_event_count,
            panel=panel,
            analysis_base=analysis_base,
            sample_funnel=sample_funnel,
            schema_data=schema_data,
            sector_counts=sector_counts,
            coverage=coverage,
            sic_info=sic_info,
            pseudo_info=pseudo_info,
            sic_coverage=sic_coverage,
            pseudo_coverage=pseudo_coverage,
            pseudo_contract=pseudo_contract,
            sector_bridge=sector_bridge,
            metric_summary=metric_summary,
        )
    )

    print("=== dimension diagnostics ===")
    dimension_diagnostics.show(truncate=False)
    print("=== sector diagnostics (taxonomies remain separate) ===")
    sector_diagnostics.show(truncate=False)
    print("=== PREFLIGHT PASSED ===")
    if mode == "preflight":
        print(
            "No output was written. Review all printed counts, then run --mode "
            "final with the same config and run_id."
        )
        return

    for name in OUTPUT_RELATIVE_PATHS:
        destination = output_paths[name]
        print(f"writing {name} -> {destination}")
        write_parquet(tables[name], destination)

    print(f"=== FINAL WRITE COMPLETE: {run_root} ===")
    print(TAXONOMY_SEPARATION_STATEMENT)
    print(GROSS_COST_LIMITATION)
    print(SIC_TEMPORAL_LIMITATION)
    print(PSEUDO_MODEL_LIMITATION)
    print(PSEUDO_PROVENANCE_LIMITATION)
    print(CORPORATE_ACTION_LIMITATION)
    print(TIME_DECAY_DEPENDENCE_LIMITATION)
