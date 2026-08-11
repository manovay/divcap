"""Compact report-table loading, schema validation, and reconciliation."""

from __future__ import annotations

from .config import *  # noqa: F401,F403

def load_tables(spark: Any, run_root: str) -> Dict[str, Any]:
    tables: Dict[str, Any] = {}
    missing = []
    for name, relative_path in TABLE_PATHS.items():
        path = f"{run_root}/{relative_path}"
        if not hadoop_path_exists(spark, path):
            missing.append(path)
            continue
        try:
            tables[name] = spark.read.parquet(path)
        except Exception as exc:
            raise ReportArtifactError(
                f"Required aggregate exists but is not readable as Parquet: "
                f"{path}: {exc}"
            ) from exc
    if missing:
        raise ReportArtifactError(
            "Required M2 V2 aggregate paths are missing: " + ", ".join(missing)
        )
    return tables


def required_table_columns() -> Dict[str, set]:
    grouped = set(GROUP_SUMMARY_COLUMNS)
    return {
        "input_summary": {"metric", "value", "description"},
        "schema_summary": {"dataset", "column", "data_type", "required"},
        "sample_funnel": {
            "stage_order",
            "stage",
            "stage_type",
            "n_events",
            "prior_stage_n_events",
            "retention_from_prior",
            "cumulative_retention",
            "definition",
        },
        "sic_coverage": {
            "sic_source",
            "known_event_count",
            "unknown_event_count",
            "unknown_event_share",
            "conflicting_ticker_count",
            "events_blocked_by_list_date",
            "temporal_limitation",
        },
        "pseudo_sector_coverage": {
            "configured_label_level",
            "base_events",
            "base_tickers",
            "direct_sic_known_events",
            "direct_sic_unknown_events",
            "pseudo_recovered_events",
            "still_unresolved_events",
            "pseudo_recovery_rate_of_sic_unknown",
            "coverage_after_recovery_share",
            "pseudo_recovered_tickers",
            "still_unresolved_tickers",
            "join_row_delta",
            "event_reconciliation_passed",
        },
        "pseudo_sector_contract": {
            "pseudo_sector_path",
            "configured_label_level",
            "source_rows",
            "source_tickers",
            "source_labels",
            "conflicting_ticker_count",
            "conflicting_level_ticker_count",
            "model_version_available",
            "prediction_confidence_available",
        },
        "sector_coverage_bridge": {
            "state_order",
            "sector_state",
            "n_events",
            "event_share_of_base",
            "n_tickers",
            "ticker_share_of_base",
            "label_source",
        },
        "bucket_boundaries": {
            "dimension",
            "bucket_label",
            "bucket_number",
            "lower_bound_exclusive",
            "upper_bound_inclusive",
        },
        "metric_identities": {"identity", "violation_rows"},
        "dimension_diagnostics": {
            "dimension",
            "actual_bucket_count",
            "high_minus_low_bps",
            "mean_range_bps",
            "median_range_bps",
            "positive_rate_range_pp",
            "monotonic_step_count",
            "spearman_bucket_mean",
        },
        "sector_diagnostics": {
            "taxonomy",
            "analysis_events",
            "analysis_tickers",
            "category_count",
            "eligible_category_count",
            "top_1_event_share",
            "top_5_event_share",
            "highest_observed_eligible_group",
            "highest_observed_mean_bps",
            "lowest_observed_eligible_group",
            "lowest_observed_mean_bps",
            "eligible_mean_range_bps",
        },
        "overall_summary": grouped | {"analysis_group"},
        "yield_summary": grouped | {"div_yield_bucket"},
        "volatility_summary": grouped | {"pre_vol_bucket"},
        "liquidity_summary": grouped | {"pre_avg_dollar_volume_bucket"},
        "sic_description_summary": grouped | {"sic_description"},
        "pseudo_sector_summary": grouped | {"pseudo_sector", "label_level"},
        "event_time_daily": {
            "offset",
            "n_events",
            "n_tickers",
            "n_abn_ret_cc",
            "mean_abn_ret_cc",
            "median_abn_ret_cc",
            "p25_abn_ret_cc",
            "p75_abn_ret_cc",
            "n_calendar_dates",
            "top_calendar_date_share",
        },
        "event_time_overnight": {
            "n_events",
            "n_tickers",
            "n_stock_overnight_ret",
            "mean_stock_overnight_ret",
            "median_stock_overnight_ret",
            "n_abn_overnight_ret",
            "mean_abn_overnight_ret",
            "median_abn_overnight_ret",
            "n_capture_ret",
            "mean_capture_ret",
            "median_capture_ret",
        },
        "run_metadata": {"run_id", "run_root", "taxonomy_separation_statement"},
    }


def sorted_pandas_tables(tables: Mapping[str, Any]) -> Dict[str, Any]:
    frames = {name: frame.toPandas() for name, frame in tables.items()}
    expected_names = set(required_table_columns())
    if set(frames) != expected_names:
        raise ReportArtifactError(
            "Report table registry mismatch: expected="
            f"{sorted(expected_names)}, found={sorted(frames)}"
        )
    sort_contract = {
        "input_summary": (["metric"], [True]),
        "schema_summary": (["dataset", "column"], [True, True]),
        "sample_funnel": (["stage_order"], [True]),
        "sic_coverage": (["sic_source"], [True]),
        "pseudo_sector_coverage": (["configured_label_level"], [True]),
        "pseudo_sector_contract": (["configured_label_level"], [True]),
        "sector_coverage_bridge": (["state_order"], [True]),
        "bucket_boundaries": (["dimension", "bucket_number"], [True, True]),
        "metric_identities": (["identity"], [True]),
        "dimension_diagnostics": (["dimension"], [True]),
        "sector_diagnostics": (["taxonomy"], [True]),
        "yield_summary": (["div_yield_bucket"], [True]),
        "volatility_summary": (["pre_vol_bucket"], [True]),
        "liquidity_summary": (["pre_avg_dollar_volume_bucket"], [True]),
        "sic_description_summary": (["n_events", "sic_description"], [False, True]),
        "pseudo_sector_summary": (["n_events", "pseudo_sector"], [False, True]),
        "event_time_daily": (["offset"], [True]),
    }
    for name, frame in frames.items():
        if frame.empty:
            raise ReportArtifactError(f"Required aggregate {name!r} is empty")
        missing_columns = sorted(required_table_columns()[name] - set(frame.columns))
        if missing_columns:
            raise ReportArtifactError(
                f"Required aggregate {name!r} is missing columns {missing_columns}; "
                f"found {sorted(frame.columns)}"
            )
        if name in sort_contract:
            columns, ascending = sort_contract[name]
            missing = [column for column in columns if column not in frame.columns]
            if missing:
                raise ReportArtifactError(
                    f"Aggregate {name!r} lacks sort-contract columns {missing}"
                )
            frames[name] = frame.sort_values(columns, ascending=ascending).reset_index(
                drop=True
            )
    return frames


def validate_report_reconciliation(
    frames: Mapping[str, Any],
    config: Mapping[str, Any],
    run_id: str,
    run_root: str,
) -> None:
    manifest_ids = set(frames["run_metadata"]["run_id"].astype(str))
    manifest_roots = set(frames["run_metadata"]["run_root"].astype(str))
    if manifest_ids != {run_id} or manifest_roots != {run_root}:
        raise ReportArtifactError(
            "Manifest/run request mismatch: "
            f"requested=({run_id}, {run_root}), manifest_ids={manifest_ids}, "
            f"manifest_roots={manifest_roots}"
        )
    coverage = frames["pseudo_sector_coverage"].iloc[0]
    base_events = int(coverage["base_events"])
    recovered_events = int(coverage["pseudo_recovered_events"])
    if int(coverage["join_row_delta"]) != 0 or not bool(
        coverage["event_reconciliation_passed"]
    ):
        raise ReportArtifactError(
            "Pseudo-sector join/reconciliation audit is not passing; report "
            "generation is blocked"
        )
    direct = frames["sic_description_summary"]
    if int(direct["n_events"].sum()) != base_events:
        raise ReportArtifactError(
            "Direct-SIC summary does not reconcile to accepted base events"
        )
    unknown = direct[direct["sic_description"].astype(str) == "UNKNOWN"]
    if len(unknown) != 1 or int(unknown.iloc[0]["n_events"]) != int(
        coverage["direct_sic_unknown_events"]
    ):
        raise ReportArtifactError(
            "Direct-SIC UNKNOWN row does not reconcile to the coverage audit"
        )
    pseudo = frames["pseudo_sector_summary"]
    if int(pseudo["n_events"].sum()) != recovered_events:
        raise ReportArtifactError(
            "Pseudo-sector summary does not reconcile to recovered events"
        )
    configured_level = str(config["pseudo_sector_label_level"])
    observed_levels = set(pseudo["label_level"].astype(str))
    if observed_levels != {configured_level} or str(
        coverage["configured_label_level"]
    ) != configured_level:
        raise ReportArtifactError(
            "Pseudo-sector report label level differs from config: "
            f"configured={configured_level}, summary={observed_levels}, "
            f"coverage={coverage['configured_label_level']}"
        )
    bridge = frames["sector_coverage_bridge"].sort_values("state_order")
    expected_states = ["direct_sic_known", "pseudo_recovered", "still_unresolved"]
    if bridge["sector_state"].astype(str).tolist() != expected_states:
        raise ReportArtifactError(
            "Coverage bridge must contain exactly the three ordered provenance states"
        )
    if int(bridge["n_events"].sum()) != base_events or not math.isclose(
        float(bridge["event_share_of_base"].sum()), 1.0, abs_tol=1e-9
    ):
        raise ReportArtifactError(
            "Coverage bridge event counts/shares do not reconcile to the base"
        )
    overall = frames["overall_summary"].iloc[0]
    if int(overall["n_events"]) != base_events:
        raise ReportArtifactError("Overall summary does not reconcile to the base")
    offsets = set(frames["event_time_daily"]["offset"].astype(int))
    required_offsets = set(
        range(int(config["event_offset_min"]), int(config["event_offset_max"]) + 1)
    )
    if offsets != required_offsets:
        raise ReportArtifactError(
            f"Event-time offsets mismatch: expected={sorted(required_offsets)}, "
            f"found={sorted(offsets)}"
        )
    violations = int(frames["metric_identities"]["violation_rows"].sum())
    if violations:
        raise ReportArtifactError(
            f"Metric-identity audit contains {violations} violation(s)"
        )
