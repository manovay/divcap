"""Small deterministic pandas aggregates used by V2 report tests."""

from __future__ import annotations

import pandas as pd


def summary_row(mean=0.001, median=0.0008, n=100, tickers=20, eligible=True):
    return {
        "n_events": n,
        "n_tickers": tickers,
        "n_capture_ret_abn": n,
        "n_capture_ret": n,
        "n_drop_ratio": n,
        "event_share_of_analysis": 0.5,
        "ticker_share_of_analysis": 0.5,
        "mean_capture_ret_abn": mean,
        "stddev_capture_ret_abn": 0.01,
        "se_capture_ret_abn": 0.001,
        "ci95_low_capture_ret_abn": mean - 0.00196,
        "ci95_high_capture_ret_abn": mean + 0.00196,
        "median_capture_ret_abn": median,
        "p25_capture_ret_abn": median - 0.002,
        "p75_capture_ret_abn": median + 0.002,
        "positive_capture_ret_abn_rate": 0.55,
        "mean_capture_ret": mean + 0.0005,
        "median_capture_ret": median + 0.0005,
        "mean_drop_ratio": 0.9,
        "median_drop_ratio": 0.88,
        "p25_drop_ratio": 0.7,
        "p75_drop_ratio": 1.1,
        "drop_ratio_lt_1_rate": 0.6,
        "low_n_flag": not eligible,
        "low_ticker_flag": not eligible,
        "report_eligible_flag": eligible,
    }


def frames():
    overall = summary_row(n=300, tickers=60)
    overall["analysis_group"] = "ALL"
    frame_map = {
        "input_summary": pd.DataFrame(
            [
                {"metric": "min_ex_date", "value": "2021-08-09", "description": "min"},
                {"metric": "max_ex_date", "value": "2026-08-07", "description": "max"},
                {"metric": "history_span_days", "value": "1824", "description": "history"},
                {"metric": "market_ticker_excluded", "value": "SPY", "description": "benchmark"},
            ]
        ),
        "schema_summary": pd.DataFrame(
            [
                {"dataset": "grain", "column": "ticker", "data_type": "string", "required": True},
                {"dataset": "pseudo_sector", "column": "pseudo_sector", "data_type": "string", "required": True},
            ]
        ),
        "overall_summary": pd.DataFrame([overall]),
        "sample_funnel": pd.DataFrame(
            [
                {"stage_order": 1, "stage": "raw", "stage_type": "sequential_filter", "n_events": 400, "prior_stage_n_events": 400, "retention_from_prior": 1.0, "cumulative_retention": 1.0, "definition": "raw"},
                {"stage_order": 2, "stage": "cash_positive", "stage_type": "sequential_filter", "n_events": 360, "prior_stage_n_events": 400, "retention_from_prior": 0.9, "cumulative_retention": 0.9, "definition": "cash"},
                {"stage_order": 3, "stage": "has_core", "stage_type": "sequential_filter", "n_events": 320, "prior_stage_n_events": 360, "retention_from_prior": 320 / 360, "cumulative_retention": 0.8, "definition": "core"},
                {"stage_order": 4, "stage": "window_contiguous", "stage_type": "sequential_filter", "n_events": 305, "prior_stage_n_events": 320, "retention_from_prior": 305 / 320, "cumulative_retention": 305 / 400, "definition": "contiguous"},
                {"stage_order": 5, "stage": "benchmark_excluded", "stage_type": "sequential_filter", "n_events": 300, "prior_stage_n_events": 305, "retention_from_prior": 300 / 305, "cumulative_retention": 0.75, "definition": "base"},
            ]
        ),
        "metric_identities": pd.DataFrame(
            [{"identity": "capture", "eligible_rows": 300, "violation_rows": 0, "max_abs_error": 0.0, "tolerance": 1e-8}]
        ),
        "sic_coverage": pd.DataFrame(
            [{"sic_source": "metadata", "known_event_count": 100, "unknown_event_count": 200, "unknown_event_share": 2 / 3, "conflicting_ticker_count": 0, "events_blocked_by_list_date": 2, "temporal_limitation": "current"}]
        ),
        "pseudo_sector_coverage": pd.DataFrame(
            [{
                "pseudo_sector_path": "/team/curated/pseudo_sector",
                "configured_label_level": "hybrid",
                "observed_label_levels": '["hybrid"]',
                "source_rows": 100,
                "source_tickers": 100,
                "source_labels": 3,
                "conflicting_ticker_count": 0,
                "conflicting_level_ticker_count": 0,
                "blank_ticker_count": 0,
                "blank_label_count": 0,
                "blank_label_level_count": 0,
                "duplicate_identical_row_count": 0,
                "base_events": 300,
                "base_tickers": 60,
                "direct_sic_known_events": 100,
                "direct_sic_unknown_events": 200,
                "pseudo_recovered_events": 120,
                "still_unresolved_events": 80,
                "pseudo_recovery_rate_of_sic_unknown": 0.6,
                "coverage_after_recovery_share": 220 / 300,
                "pseudo_recovered_tickers": 30,
                "still_unresolved_tickers": 20,
                "pseudo_matches_on_direct_sic_known_events": 5,
                "join_row_delta": 0,
                "event_reconciliation_passed": True,
                "ticker_reconciliation_note": "not additive",
            }]
        ),
        "pseudo_sector_contract": pd.DataFrame(
            [{"pseudo_sector_path": "/team/curated/pseudo_sector", "configured_label_level": "hybrid", "observed_label_levels_json": '["hybrid"]', "source_rows": 100, "source_tickers": 100, "source_labels": 3, "blank_ticker_count": 0, "blank_label_count": 0, "blank_label_level_count": 0, "duplicate_identical_row_count": 0, "configured_level_rows": 100, "configured_level_distinct_rows": 100, "configured_level_tickers": 100, "conflicting_ticker_count": 0, "conflicting_level_ticker_count": 0, "model_version_available": False, "prediction_confidence_available": False, "training_timestamp_available": False, "upstream_documented_model_metrics": "documented", "source_schema_json": "{}", "source_modification_time_utc": "unavailable", "source_path_length_bytes": -1}]
        ),
        "sector_coverage_bridge": pd.DataFrame(
            [
                {"state_order": 1, "sector_state": "direct_sic_known", "n_events": 100, "event_share_of_base": 1 / 3, "n_tickers": 25, "ticker_share_of_base": 25 / 60, "label_source": "direct"},
                {"state_order": 2, "sector_state": "pseudo_recovered", "n_events": 120, "event_share_of_base": 0.4, "n_tickers": 30, "ticker_share_of_base": 0.5, "label_source": "model"},
                {"state_order": 3, "sector_state": "still_unresolved", "n_events": 80, "event_share_of_base": 80 / 300, "n_tickers": 20, "ticker_share_of_base": 1 / 3, "label_source": "unresolved"},
            ]
        ),
        "dimension_diagnostics": pd.DataFrame(
            [
                {"dimension": "yield", "actual_bucket_count": 2, "low_bucket_mean_bps": -5.0, "high_bucket_mean_bps": 15.0, "high_minus_low_bps": 20.0, "mean_range_bps": 20.0, "median_range_bps": 15.0, "positive_rate_range_pp": 5.0, "monotonic_step_count": 1, "spearman_bucket_mean": 1.0},
                {"dimension": "volatility", "actual_bucket_count": 2, "low_bucket_mean_bps": 2.0, "high_bucket_mean_bps": -1.0, "high_minus_low_bps": -3.0, "mean_range_bps": 3.0, "median_range_bps": 2.0, "positive_rate_range_pp": 2.0, "monotonic_step_count": 0, "spearman_bucket_mean": -1.0},
                {"dimension": "liquidity", "actual_bucket_count": 2, "low_bucket_mean_bps": 1.0, "high_bucket_mean_bps": 1.02, "high_minus_low_bps": 0.02, "mean_range_bps": 0.02, "median_range_bps": 0.01, "positive_rate_range_pp": 0.1, "monotonic_step_count": 1, "spearman_bucket_mean": 1.0},
            ]
        ),
        "sector_diagnostics": pd.DataFrame(
            [
                {"taxonomy": "direct_sic_current_reference", "analysis_events": 100, "analysis_tickers": 25, "category_count": 3, "eligible_category_count": 2, "top_1_event_share": 0.5, "top_5_event_share": 1.0, "highest_observed_eligible_group": "BANKS", "highest_observed_mean_bps": 12.0, "highest_observed_n_events": 50, "highest_observed_n_tickers": 10, "lowest_observed_eligible_group": "RETAIL", "lowest_observed_mean_bps": -4.0, "lowest_observed_n_events": 40, "lowest_observed_n_tickers": 9, "eligible_mean_range_bps": 16.0},
                {"taxonomy": "model_predicted_pseudo_sector", "analysis_events": 120, "analysis_tickers": 30, "category_count": 3, "eligible_category_count": 2, "top_1_event_share": 0.5, "top_5_event_share": 1.0, "highest_observed_eligible_group": "SIC 60", "highest_observed_mean_bps": 10.0, "highest_observed_n_events": 60, "highest_observed_n_tickers": 15, "lowest_observed_eligible_group": "Services", "lowest_observed_mean_bps": -2.0, "lowest_observed_n_events": 45, "lowest_observed_n_tickers": 12, "eligible_mean_range_bps": 12.0},
            ]
        ),
        "run_metadata": pd.DataFrame(
            [{"run_id": "fixture_run", "run_root": "/team/m2/cross_sectional/fixture_run", "taxonomy_separation_statement": "separate"}]
        ),
    }
    ordered_specs = (
        ("yield_summary", "div_yield_bucket", -0.0005, 0.0015),
        ("volatility_summary", "pre_vol_bucket", 0.0002, -0.0001),
        ("liquidity_summary", "pre_avg_dollar_volume_bucket", 0.0001, 0.000102),
    )
    for table, column, low, high in ordered_specs:
        rows = []
        for label, mean in (("Q01", low), ("Q02", high)):
            row = summary_row(mean=mean, median=mean * 0.8, n=150, tickers=30)
            row[column] = label
            rows.append(row)
        frame_map[table] = pd.DataFrame(rows)
    direct_rows = []
    for name, mean, n, eligible in (
        ("UNKNOWN", 0.0002, 200, True),
        ("BANKS", 0.0012, 50, True),
        ("RETAIL", -0.0004, 40, True),
        ("TINY", 0.2, 10, False),
    ):
        row = summary_row(mean=mean, median=mean * 0.8, n=n, tickers=10 if eligible else 1, eligible=eligible)
        row["sic_description"] = name
        direct_rows.append(row)
    frame_map["sic_description_summary"] = pd.DataFrame(direct_rows)
    pseudo_rows = []
    for name, mean, n, eligible in (
        ("SIC 60", 0.001, 60, True),
        ("Services", -0.0002, 45, True),
        ("tiny", 0.3, 15, False),
    ):
        row = summary_row(mean=mean, median=mean * 0.8, n=n, tickers=12 if eligible else 1, eligible=eligible)
        row["pseudo_sector"] = name
        row["label_level"] = "hybrid"
        pseudo_rows.append(row)
    frame_map["pseudo_sector_summary"] = pd.DataFrame(pseudo_rows)
    frame_map["bucket_boundaries"] = pd.DataFrame(
        [
            {"dimension": dimension, "source_column": "x", "bucket_column": "bucket", "bucket_label": label, "bucket_number": number, "lower_bound_exclusive": None if number == 1 else 1.0, "upper_bound_inclusive": 1.0 if number == 1 else None, "sample_min": 0.0, "sample_max": 2.0, "requested_bucket_count": 2, "actual_bucket_count": 2, "n_events": 150}
            for dimension in ("yield", "volatility", "liquidity")
            for number, label in ((1, "Q01"), (2, "Q02"))
        ]
    )
    daily_rows = []
    for offset in range(-4, 4):
        mean = -0.008 if offset == 0 else 0.0001 * offset
        daily_rows.append({"offset": offset, "n_events": 250, "n_tickers": 55, "n_abn_ret_cc": 250, "mean_abn_ret_cc": mean, "stddev_abn_ret_cc": 0.01, "se_abn_ret_cc": 0.001, "ci95_low_abn_ret_cc": mean - 0.00196, "ci95_high_abn_ret_cc": mean + 0.00196, "median_abn_ret_cc": mean * 0.8, "p25_abn_ret_cc": mean - 0.002, "p75_abn_ret_cc": mean + 0.002, "n_calendar_dates": 50, "top_calendar_date_events": 10, "top_calendar_date_share": 0.04})
    frame_map["event_time_daily"] = pd.DataFrame(daily_rows)
    frame_map["event_time_overnight"] = pd.DataFrame(
        [{"n_events": 300, "n_tickers": 60, "n_stock_overnight_ret": 300, "mean_stock_overnight_ret": -0.008, "stddev_stock_overnight_ret": 0.02, "median_stock_overnight_ret": -0.007, "p25_stock_overnight_ret": -0.015, "p75_stock_overnight_ret": 0.001, "n_abn_overnight_ret": 300, "mean_abn_overnight_ret": -0.0085, "stddev_abn_overnight_ret": 0.02, "median_abn_overnight_ret": -0.0075, "p25_abn_overnight_ret": -0.016, "p75_abn_overnight_ret": 0.001, "n_capture_ret": 300, "mean_capture_ret": 0.0015, "median_capture_ret": 0.0012, "stock_overnight_definition": "x", "abn_overnight_definition": "y"}]
    )
    return frame_map


def config():
    return {
        "pseudo_sector_label_level": "hybrid",
        "report_top_sic_n": 20,
        "report_top_pseudo_n": 20,
        "report_numeric_labels": True,
        "insight_min_abs_bps": 1.0,
        "event_offset_min": -4,
        "event_offset_max": 3,
    }
