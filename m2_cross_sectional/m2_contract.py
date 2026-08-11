"""Shared M2 V2 path, artifact, section, and limitation contracts.

Both Spark entry points import this module so writer and reader registries cannot
drift.  It intentionally contains no Spark or plotting imports and remains safe
to use from local unit tests.
"""

from __future__ import annotations


OUTPUT_RELATIVE_PATHS = {
    "analysis_base": "analysis_base",
    "model_features": "model_features",
    "model_outcomes": "model_outcomes",
    "audit_input_summary": "audit/input_summary",
    "audit_schema_summary": "audit/schema_summary",
    "audit_sample_funnel": "audit/sample_funnel",
    "audit_sic_coverage": "audit/sic_coverage",
    "audit_pseudo_sector_coverage": "audit/pseudo_sector_coverage",
    "audit_pseudo_sector_contract": "audit/pseudo_sector_contract",
    "audit_sector_coverage_bridge": "audit/sector_coverage_bridge",
    "audit_bucket_boundaries": "audit/bucket_boundaries",
    "audit_metric_identities": "audit/metric_identities",
    "audit_dimension_diagnostics": "audit/dimension_diagnostics",
    "audit_sector_diagnostics": "audit/sector_diagnostics",
    "core_overall": "core/overall",
    "core_yield": "core/yield",
    "core_volatility": "core/volatility",
    "core_liquidity": "core/liquidity",
    "core_sic_description": "core/sic_description",
    "core_pseudo_sector": "core/pseudo_sector",
    "core_event_time_daily": "core/event_time_daily",
    "core_event_time_overnight": "core/event_time_overnight",
    "manifest_run_metadata": "manifest/run_metadata",
}


# The report deliberately excludes analysis_base and both model handoffs.  It
# reads only compact audit/core/manifest aggregates.
REPORT_TABLE_PATHS = {
    "input_summary": OUTPUT_RELATIVE_PATHS["audit_input_summary"],
    "schema_summary": OUTPUT_RELATIVE_PATHS["audit_schema_summary"],
    "sample_funnel": OUTPUT_RELATIVE_PATHS["audit_sample_funnel"],
    "sic_coverage": OUTPUT_RELATIVE_PATHS["audit_sic_coverage"],
    "pseudo_sector_coverage": OUTPUT_RELATIVE_PATHS[
        "audit_pseudo_sector_coverage"
    ],
    "pseudo_sector_contract": OUTPUT_RELATIVE_PATHS[
        "audit_pseudo_sector_contract"
    ],
    "sector_coverage_bridge": OUTPUT_RELATIVE_PATHS[
        "audit_sector_coverage_bridge"
    ],
    "bucket_boundaries": OUTPUT_RELATIVE_PATHS["audit_bucket_boundaries"],
    "metric_identities": OUTPUT_RELATIVE_PATHS["audit_metric_identities"],
    "dimension_diagnostics": OUTPUT_RELATIVE_PATHS[
        "audit_dimension_diagnostics"
    ],
    "sector_diagnostics": OUTPUT_RELATIVE_PATHS["audit_sector_diagnostics"],
    "overall_summary": OUTPUT_RELATIVE_PATHS["core_overall"],
    "yield_summary": OUTPUT_RELATIVE_PATHS["core_yield"],
    "volatility_summary": OUTPUT_RELATIVE_PATHS["core_volatility"],
    "liquidity_summary": OUTPUT_RELATIVE_PATHS["core_liquidity"],
    "sic_description_summary": OUTPUT_RELATIVE_PATHS[
        "core_sic_description"
    ],
    "pseudo_sector_summary": OUTPUT_RELATIVE_PATHS["core_pseudo_sector"],
    "event_time_daily": OUTPUT_RELATIVE_PATHS["core_event_time_daily"],
    "event_time_overnight": OUTPUT_RELATIVE_PATHS[
        "core_event_time_overnight"
    ],
    "run_metadata": OUTPUT_RELATIVE_PATHS["manifest_run_metadata"],
}


CSV_OUTPUTS = {
    table_name: f"{table_name}.csv" for table_name in REPORT_TABLE_PATHS
}


FIGURE_OUTPUTS = {
    "sample_funnel": "01_sample_funnel.png",
    "sector_coverage": "02_sector_coverage_recovery.png",
    "overall": "03_overall_capture_metrics.png",
    "yield": "04_yield_capture_profile.png",
    "volatility": "05_volatility_capture_profile.png",
    "liquidity": "06_liquidity_capture_profile.png",
    "sic_performance": "07_sic_description_capture_profile.png",
    "sic_rates": "08_sic_description_outcome_rates.png",
    "pseudo_performance": "09_pseudo_sector_capture_profile.png",
    "pseudo_rates": "10_pseudo_sector_outcome_rates.png",
    "event_time_daily": "11_event_time_daily_profile.png",
    "event_time_overnight": "12_event_time_overnight_metrics.png",
}


REPORT_SECTIONS = (
    ("executive_summary", "Executive summary"),
    ("data_quality", "Data quality and sample funnel"),
    ("sector_coverage", "Sector coverage and pseudo-sector recovery"),
    ("overall", "Overall dividend-capture performance"),
    ("yield", "Dividend-yield cross section"),
    ("volatility", "Pre-event volatility cross section"),
    (
        "liquidity",
        "Pre-event dollar-volume liquidity / size-proxy cross section",
    ),
    ("direct_sic", "Direct SIC description analysis"),
    ("pseudo_sector", "Pseudo-sector analysis"),
    ("event_time", "Event-time and ex-date overnight behavior"),
    ("business_implications", "Business implications and next steps"),
    (
        "limitations",
        "Limitations, methodology, and reconciliation appendix",
    ),
)


ALLOWED_INSIGHT_STATUSES = frozenset(
    {
        "informative",
        "mixed",
        "flat",
        "limited_coverage",
        "insufficient_eligible_groups",
        "data_quality_blocker",
    }
)


SIC_TEMPORAL_LIMITATION = (
    "Direct SIC is current-reference metadata and is not guaranteed to be a "
    "point-in-time classification."
)
PSEUDO_MODEL_LIMITATION = (
    "Pseudo-sector labels are model-derived from current company descriptions "
    "and SIC-labelled training rows; M2 does not revalidate the upstream model."
)
PSEUDO_PROVENANCE_LIMITATION = (
    "The upstream pseudo-sector output has no prediction-confidence, model-"
    "version, or training-timestamp field and is structurally limited to active "
    "CS/ADRC rows with usable descriptions."
)
UPSTREAM_MODEL_METRICS = (
    "The upstream hybrid model documents held-out accuracy 0.683 and weighted "
    "F1 0.670; these are upstream results, not M2 runtime validation."
)
GROSS_COST_LIMITATION = (
    "Returns are gross and descriptive; transaction costs, taxes, and net "
    "tradeability are not modeled."
)
DEPENDENCE_LIMITATION = (
    "The unadjusted descriptive intervals do not account for dependence within "
    "ticker or calendar date."
)
CORPORATE_ACTION_LIMITATION = (
    "Corporate-action contamination in upstream unadjusted prices remains an "
    "upstream limitation."
)
TAXONOMY_SEPARATION_STATEMENT = (
    "Direct/current-reference SIC and model-predicted pseudo-sector are kept in "
    "separate analytical populations; the coverage bridge is not a blended "
    "sector-performance taxonomy."
)


NUMERIC_CHART_CONTRACT_VERSION = "m2-cross-sectional-v2-1"
INSIGHT_GENERATION_VERSION = "m2-cross-sectional-v2-1"


REQUIRED_CONFIG_KEYS = (
    "team",
    "grain_path",
    "panel_path",
    "metadata_path",
    "pseudo_sector_path",
    "output_root",
    "sic_description_column",
    "pseudo_sector_column",
    "pseudo_sector_label_level",
    "market_ticker",
    "min_history_days",
    "bucket_count",
    "min_cell_n",
    "min_pseudo_cell_n",
    "min_report_tickers",
    "event_offset_min",
    "event_offset_max",
    "primary_metric",
    "secondary_metric",
    "academic_metric",
    "metric_tolerance",
    "report_top_sic_n",
    "report_top_pseudo_n",
    "report_numeric_labels",
    "insight_min_abs_bps",
)


GROUP_SUMMARY_COLUMNS = (
    "n_events",
    "n_tickers",
    "n_capture_ret_abn",
    "n_capture_ret",
    "n_drop_ratio",
    "event_share_of_analysis",
    "ticker_share_of_analysis",
    "mean_capture_ret_abn",
    "stddev_capture_ret_abn",
    "se_capture_ret_abn",
    "ci95_low_capture_ret_abn",
    "ci95_high_capture_ret_abn",
    "median_capture_ret_abn",
    "p25_capture_ret_abn",
    "p75_capture_ret_abn",
    "positive_capture_ret_abn_rate",
    "mean_capture_ret",
    "median_capture_ret",
    "mean_drop_ratio",
    "median_drop_ratio",
    "p25_drop_ratio",
    "p75_drop_ratio",
    "drop_ratio_lt_1_rate",
    "low_n_flag",
    "low_ticker_flag",
    "report_eligible_flag",
)
