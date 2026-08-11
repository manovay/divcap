"""Deterministic construction of the twelve required insight sections."""

from __future__ import annotations

from .insight_core import *  # noqa: F401,F403
from .figures import _truthy_series
from .insight_core import _ordered_section

def build_section_insights(
    frames: Mapping[str, Any],
    config: Mapping[str, Any],
    run_id: str,
    generated_at_utc: Optional[str] = None,
) -> Dict[str, Any]:
    inputs = input_metric_map(frames["input_summary"])
    overall = frames["overall_summary"].iloc[0]
    sic = frames["sic_coverage"].iloc[0]
    pseudo = frames["pseudo_sector_coverage"].iloc[0]
    raw_funnel = row_for(frames["sample_funnel"], "stage", "raw")
    base_funnel = row_for(frames["sample_funnel"], "stage", "benchmark_excluded")
    sequential = frames["sample_funnel"][
        frames["sample_funnel"]["stage_type"] == "sequential_filter"
    ].sort_values("stage_order")
    losses = []
    for _, row in sequential.iterrows():
        if str(row["stage"]) == "raw":
            continue
        losses.append((int(row["prior_stage_n_events"]) - int(row["n_events"]), str(row["stage"])))
    largest_loss, largest_loss_stage = max(losses) if losses else (0, "none")
    identity_violations = int(frames["metric_identities"]["violation_rows"].sum())

    ordered_rows = frames["dimension_diagnostics"]
    strongest = ordered_rows.iloc[
        ordered_rows["high_minus_low_bps"].astype(float).abs().argmax()
    ]
    daily = frames["event_time_daily"].copy()
    largest_daily = daily.iloc[
        daily["mean_abn_ret_cc"].astype(float).abs().argmax()
    ]
    offset_zero = row_for(daily, "offset", 0)
    overnight = frames["event_time_overnight"].iloc[0]
    direct_diag = row_for(
        frames["sector_diagnostics"], "taxonomy", "direct_sic_current_reference"
    )
    pseudo_diag = row_for(
        frames["sector_diagnostics"], "taxonomy", "model_predicted_pseudo_sector"
    )
    direct_categories = frames["sic_description_summary"].copy()
    direct_categories = direct_categories[
        (direct_categories["sic_description"].astype(str) != "UNKNOWN")
        & _truthy_series(direct_categories["report_eligible_flag"])
    ]
    pseudo_categories = frames["pseudo_sector_summary"].copy()
    pseudo_categories = pseudo_categories[
        _truthy_series(pseudo_categories["report_eligible_flag"])
    ]
    pseudo_valid_n = pseudo_categories["n_capture_ret_abn"].astype(float)
    pseudo_weighted_mean = (
        float(
            (
                pseudo_categories["mean_capture_ret_abn"].astype(float)
                * pseudo_valid_n
            ).sum()
            / pseudo_valid_n.sum()
        )
        if not pseudo_categories.empty and pseudo_valid_n.sum()
        else None
    )

    executive_findings = [
        f"{int(overall['n_events']):,} accepted events across {int(overall['n_tickers']):,} tickers from {inputs.get('min_ex_date')} to {inputs.get('max_ex_date')}",
        f"overall mean/median abnormal capture {format_bps(overall['mean_capture_ret_abn'])} / {format_bps(overall['median_capture_ret_abn'])}",
        f"direct-SIC unknown {float(sic['unknown_event_share']):.1%}; pseudo recovery among unknown {float(pseudo['pseudo_recovery_rate_of_sic_unknown']):.1%}",
        f"largest ordered high-minus-low spread {float(strongest['high_minus_low_bps']):.2f} bps in {strongest['dimension']}",
        f"largest absolute daily event-time mean at offset {int(largest_daily['offset']):+d}: {format_bps(largest_daily['mean_abn_ret_cc'])}",
    ]
    executive = section(
        "executive_summary",
        "Executive summary",
        "mixed" if float(overall["mean_capture_ret_abn"]) * float(overall["median_capture_ret_abn"]) < 0 else "informative",
        "; ".join(executive_findings) + ".",
        [
            evidence("base_events", int(overall["n_events"]), format_count(overall["n_events"]), CSV_OUTPUTS["overall_summary"]),
            evidence("base_tickers", int(overall["n_tickers"]), format_count(overall["n_tickers"]), CSV_OUTPUTS["overall_summary"]),
            evidence("min_ex_date", inputs.get("min_ex_date"), str(inputs.get("min_ex_date")), CSV_OUTPUTS["input_summary"]),
            evidence("max_ex_date", inputs.get("max_ex_date"), str(inputs.get("max_ex_date")), CSV_OUTPUTS["input_summary"]),
            evidence("mean_capture_ret_abn", float(overall["mean_capture_ret_abn"]), format_bps(overall["mean_capture_ret_abn"]), CSV_OUTPUTS["overall_summary"]),
            evidence("median_capture_ret_abn", float(overall["median_capture_ret_abn"]), format_bps(overall["median_capture_ret_abn"]), CSV_OUTPUTS["overall_summary"]),
            evidence("direct_sic_unknown_share", float(sic["unknown_event_share"]), format_percent(sic["unknown_event_share"]), CSV_OUTPUTS["sic_coverage"]),
            evidence("pseudo_recovery_rate_of_sic_unknown", float(pseudo["pseudo_recovery_rate_of_sic_unknown"]), format_percent(pseudo["pseudo_recovery_rate_of_sic_unknown"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("strongest_ordered_high_minus_low_bps", float(strongest["high_minus_low_bps"]), f"{float(strongest['high_minus_low_bps']):.2f} bps ({strongest['dimension']})", CSV_OUTPUTS["dimension_diagnostics"]),
            evidence("largest_abs_event_time_mean", float(largest_daily["mean_abn_ret_cc"]), f"offset {int(largest_daily['offset']):+d}: {format_bps(largest_daily['mean_abn_ret_cc'])}", CSV_OUTPUTS["event_time_daily"]),
        ],
        "The accepted aggregates describe where gross capture outcomes differ in "
        "this sample and which data-quality or coverage gaps remain most material.",
        [GROSS_COST_LIMITATION, TAXONOMY_SEPARATION_STATEMENT],
        "Prioritize stability, cost, point-in-time taxonomy, and clustered-inference "
        "checks before treating any descriptive pattern as decision-ready.",
    )

    data_quality = section(
        "data_quality",
        "Data quality and sample funnel",
        "informative" if identity_violations == 0 else "data_quality_blocker",
        f"From {inputs.get('min_ex_date')} to {inputs.get('max_ex_date')} ({inputs.get('history_span_days')} calendar days), the base retained {int(base_funnel['n_events']):,} of {int(raw_funnel['n_events']):,} raw events ({float(base_funnel['cumulative_retention']):.1%}); the largest sequential loss was {largest_loss:,} events at {largest_loss_stage}, and metric-identity violations totaled {identity_violations:,}.",
        [
            evidence("raw_events", int(raw_funnel["n_events"]), format_count(raw_funnel["n_events"]), CSV_OUTPUTS["sample_funnel"]),
            evidence("base_events", int(base_funnel["n_events"]), format_count(base_funnel["n_events"]), CSV_OUTPUTS["sample_funnel"]),
            evidence("base_cumulative_retention", float(base_funnel["cumulative_retention"]), format_percent(base_funnel["cumulative_retention"]), CSV_OUTPUTS["sample_funnel"]),
            evidence("min_ex_date", inputs.get("min_ex_date"), str(inputs.get("min_ex_date")), CSV_OUTPUTS["input_summary"]),
            evidence("max_ex_date", inputs.get("max_ex_date"), str(inputs.get("max_ex_date")), CSV_OUTPUTS["input_summary"]),
            evidence("history_span_days", int(inputs.get("history_span_days")), f"{int(inputs.get('history_span_days')):,} days", CSV_OUTPUTS["input_summary"]),
            evidence("largest_filter_loss_events", largest_loss, f"{largest_loss:,} at {largest_loss_stage}", CSV_OUTPUTS["sample_funnel"]),
            evidence("metric_identity_violations", identity_violations, format_count(identity_violations), CSV_OUTPUTS["metric_identities"]),
        ],
        "The funnel separates acceptance filters from downstream analysis-population diagnostics, preserving exact denominators.",
        [CORPORATE_ACTION_LIMITATION],
        "Confirm the live key, history-span, and identity gates in the accepted-run review and investigate any future retention shift.",
    )

    direct_known = int(pseudo["direct_sic_known_events"])
    direct_unknown = int(pseudo["direct_sic_unknown_events"])
    recovered = int(pseudo["pseudo_recovered_events"])
    unresolved = int(pseudo["still_unresolved_events"])
    coverage = section(
        "sector_coverage",
        "Sector coverage and pseudo-sector recovery",
        "limited_coverage" if unresolved > 0 else "informative",
        f"Direct SIC was known for {direct_known:,} base events and unknown for {direct_unknown:,}; model-predicted pseudo-sector recovered {recovered:,} unknown events ({float(pseudo['pseudo_recovery_rate_of_sic_unknown']):.1%}), leaving {unresolved:,} unresolved.",
        [
            evidence("direct_sic_known_events", direct_known, format_count(direct_known), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("direct_sic_unknown_events", direct_unknown, format_count(direct_unknown), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("pseudo_recovered_events", recovered, format_count(recovered), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("pseudo_recovery_rate_of_sic_unknown", float(pseudo["pseudo_recovery_rate_of_sic_unknown"]), format_percent(pseudo["pseudo_recovery_rate_of_sic_unknown"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("still_unresolved_events", unresolved, format_count(unresolved), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("coverage_after_recovery_share", float(pseudo["coverage_after_recovery_share"]), format_percent(pseudo["coverage_after_recovery_share"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
        ],
        "Recovery expands coverage accounting for otherwise-unknown events while keeping observed/current-reference and model-derived taxonomies analytically separate.",
        [TAXONOMY_SEPARATION_STATEMENT, PSEUDO_PROVENANCE_LIMITATION],
        "Track residual coverage by security type and obtain versioned/confidence-bearing upstream predictions before broader use.",
    )

    overall_status = (
        "mixed"
        if float(overall["mean_capture_ret_abn"]) * float(overall["median_capture_ret_abn"]) < 0
        else "informative"
    )
    overall_section = section(
        "overall",
        "Overall dividend-capture performance",
        overall_status,
        f"Across {int(overall['n_events']):,} events, mean and median gross abnormal capture were {format_bps(overall['mean_capture_ret_abn'])} and {format_bps(overall['median_capture_ret_abn'])}; {format_percent(overall['positive_capture_ret_abn_rate'])} of valid observations were positive.",
        [
            evidence("mean_capture_ret_abn", float(overall["mean_capture_ret_abn"]), format_bps(overall["mean_capture_ret_abn"]), CSV_OUTPUTS["overall_summary"]),
            evidence("median_capture_ret_abn", float(overall["median_capture_ret_abn"]), format_bps(overall["median_capture_ret_abn"]), CSV_OUTPUTS["overall_summary"]),
            evidence("mean_capture_ret", float(overall["mean_capture_ret"]), format_bps(overall["mean_capture_ret"]), CSV_OUTPUTS["overall_summary"]),
            evidence("median_capture_ret", float(overall["median_capture_ret"]), format_bps(overall["median_capture_ret"]), CSV_OUTPUTS["overall_summary"]),
            evidence("positive_capture_ret_abn_rate", float(overall["positive_capture_ret_abn_rate"]), format_percent(overall["positive_capture_ret_abn_rate"]), CSV_OUTPUTS["overall_summary"]),
            evidence("median_drop_ratio", float(overall["median_drop_ratio"]), format_ratio(overall["median_drop_ratio"]), CSV_OUTPUTS["overall_summary"]),
            evidence("p25_drop_ratio", float(overall["p25_drop_ratio"]), format_ratio(overall["p25_drop_ratio"]), CSV_OUTPUTS["overall_summary"]),
            evidence("p75_drop_ratio", float(overall["p75_drop_ratio"]), format_ratio(overall["p75_drop_ratio"]), CSV_OUTPUTS["overall_summary"]),
            evidence("drop_ratio_lt_1_rate", float(overall["drop_ratio_lt_1_rate"]), format_percent(overall["drop_ratio_lt_1_rate"]), CSV_OUTPUTS["overall_summary"]),
        ],
        "Mean, median, rate, and drop-ratio evidence should be read together because each summarizes a different aspect of the outcome distribution.",
        [GROSS_COST_LIMITATION, DEPENDENCE_LIMITATION],
        "Add transaction-cost/tax scenarios and ticker/date-clustered uncertainty in a separately specified validation stage.",
    )

    threshold = float(config["insight_min_abs_bps"])
    yield_section = _ordered_section(frames, "yield", "Dividend-yield cross section", "yield_summary", "div_yield_bucket", threshold)
    volatility_section = _ordered_section(frames, "volatility", "Pre-event volatility cross section", "volatility_summary", "pre_vol_bucket", threshold)
    liquidity_section = _ordered_section(frames, "liquidity", "Pre-event dollar-volume liquidity / size-proxy cross section", "liquidity_summary", "pre_avg_dollar_volume_bucket", threshold)

    direct_eligible = int(direct_diag["eligible_category_count"])
    direct_status = "informative" if direct_eligible else "insufficient_eligible_groups"
    if direct_eligible:
        direct_high = row_for(
            direct_categories,
            "sic_description",
            direct_diag["highest_observed_eligible_group"],
        )
        direct_low = row_for(
            direct_categories,
            "sic_description",
            direct_diag["lowest_observed_eligible_group"],
        )
        direct_agrees = (
            float(direct_high["mean_capture_ret_abn"])
            * float(direct_high["median_capture_ret_abn"])
            >= 0
            and float(direct_low["mean_capture_ret_abn"])
            * float(direct_low["median_capture_ret_abn"])
            >= 0
            and float(direct_high["positive_capture_ret_abn_rate"])
            >= float(direct_low["positive_capture_ret_abn_rate"])
        )
        agreement_text = (
            "broadly aligned" if direct_agrees else "mixed across mean, median, and rate"
        )
        direct_headline = (
            f"Among {direct_eligible} report-eligible direct SIC categories, the highest- and lowest-observed means were {float(direct_diag['highest_observed_mean_bps']):.2f} and {float(direct_diag['lowest_observed_mean_bps']):.2f} bps; the eligible range was {float(direct_diag['eligible_mean_range_bps']):.2f} bps."
            f" Mean, median, and outcome-rate evidence was {agreement_text}."
        )
    else:
        direct_high = direct_low = None
        direct_headline = "No direct SIC category met both report event and ticker thresholds; canonical rows remain available for review."
    direct_section = section(
        "direct_sic",
        "Direct SIC description analysis",
        direct_status,
        direct_headline,
        [
            evidence("known_sic_events", int(direct_diag["analysis_events"]), format_count(direct_diag["analysis_events"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("known_sic_tickers", int(direct_diag["analysis_tickers"]), format_count(direct_diag["analysis_tickers"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("category_count", int(direct_diag["category_count"]), format_count(direct_diag["category_count"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("eligible_category_count", direct_eligible, format_count(direct_eligible), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("top_1_event_share", float(direct_diag["top_1_event_share"]), format_percent(direct_diag["top_1_event_share"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("top_5_event_share", float(direct_diag["top_5_event_share"]), format_percent(direct_diag["top_5_event_share"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("highest_observed_mean_bps", None if is_missing(direct_diag["highest_observed_mean_bps"]) else float(direct_diag["highest_observed_mean_bps"]), "not available" if is_missing(direct_diag["highest_observed_mean_bps"]) else f"{float(direct_diag['highest_observed_mean_bps']):.2f} bps ({direct_diag['highest_observed_eligible_group']})", CSV_OUTPUTS["sector_diagnostics"]),
            evidence("lowest_observed_mean_bps", None if is_missing(direct_diag["lowest_observed_mean_bps"]) else float(direct_diag["lowest_observed_mean_bps"]), "not available" if is_missing(direct_diag["lowest_observed_mean_bps"]) else f"{float(direct_diag['lowest_observed_mean_bps']):.2f} bps ({direct_diag['lowest_observed_eligible_group']})", CSV_OUTPUTS["sector_diagnostics"]),
            evidence("highest_observed_median_capture_ret_abn", None if direct_high is None else float(direct_high["median_capture_ret_abn"]), "not available" if direct_high is None else format_bps(direct_high["median_capture_ret_abn"]), CSV_OUTPUTS["sic_description_summary"]),
            evidence("lowest_observed_median_capture_ret_abn", None if direct_low is None else float(direct_low["median_capture_ret_abn"]), "not available" if direct_low is None else format_bps(direct_low["median_capture_ret_abn"]), CSV_OUTPUTS["sic_description_summary"]),
            evidence("highest_observed_positive_rate", None if direct_high is None else float(direct_high["positive_capture_ret_abn_rate"]), "not available" if direct_high is None else format_percent(direct_high["positive_capture_ret_abn_rate"]), CSV_OUTPUTS["sic_description_summary"]),
            evidence("lowest_observed_positive_rate", None if direct_low is None else float(direct_low["positive_capture_ret_abn_rate"]), "not available" if direct_low is None else format_percent(direct_low["positive_capture_ret_abn_rate"]), CSV_OUTPUTS["sic_description_summary"]),
        ],
        "Current-reference direct SIC patterns describe the covered subset only; UNKNOWN remains in the canonical table and coverage section, not the known-label ranking.",
        [SIC_TEMPORAL_LIMITATION, GROSS_COST_LIMITATION, DEPENDENCE_LIMITATION],
        "Obtain point-in-time sector metadata and repeat the direct-SIC profile across time before using category differences for prioritization.",
    )

    pseudo_eligible = int(pseudo_diag["eligible_category_count"])
    pseudo_status = "informative" if pseudo_eligible else "insufficient_eligible_groups"
    label_level = str(config["pseudo_sector_label_level"])
    if pseudo_eligible:
        pseudo_headline = (
            f"Within {recovered:,} recovered events assigned model-predicted pseudo-sector labels at level {label_level}, {pseudo_eligible} categories were report-eligible; the highest/lowest observed means were {float(pseudo_diag['highest_observed_mean_bps']):.2f}/{float(pseudo_diag['lowest_observed_mean_bps']):.2f} bps versus the recovered eligible-category weighted baseline {format_bps(pseudo_weighted_mean)}, and the eligible range was {float(pseudo_diag['eligible_mean_range_bps']):.2f} bps."
        )
    else:
        pseudo_headline = (
            f"Within {recovered:,} recovered events assigned model-predicted pseudo-sector labels at level {label_level}, no category met both report thresholds."
        )
    pseudo_section = section(
        "pseudo_sector",
        "Pseudo-sector analysis",
        pseudo_status,
        pseudo_headline,
        [
            evidence("pseudo_recovered_events", recovered, format_count(recovered), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("pseudo_recovered_tickers", int(pseudo["pseudo_recovered_tickers"]), format_count(pseudo["pseudo_recovered_tickers"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("pseudo_recovery_rate_of_sic_unknown", float(pseudo["pseudo_recovery_rate_of_sic_unknown"]), format_percent(pseudo["pseudo_recovery_rate_of_sic_unknown"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("category_count", int(pseudo_diag["category_count"]), format_count(pseudo_diag["category_count"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("eligible_category_count", pseudo_eligible, format_count(pseudo_eligible), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("top_1_event_share", float(pseudo_diag["top_1_event_share"]), format_percent(pseudo_diag["top_1_event_share"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("top_5_event_share", float(pseudo_diag["top_5_event_share"]), format_percent(pseudo_diag["top_5_event_share"]), CSV_OUTPUTS["sector_diagnostics"]),
            evidence("eligible_mean_range_bps", None if is_missing(pseudo_diag["eligible_mean_range_bps"]) else float(pseudo_diag["eligible_mean_range_bps"]), "not available" if is_missing(pseudo_diag["eligible_mean_range_bps"]) else f"{float(pseudo_diag['eligible_mean_range_bps']):.2f} bps", CSV_OUTPUTS["sector_diagnostics"]),
            evidence("highest_observed_mean_bps", None if is_missing(pseudo_diag["highest_observed_mean_bps"]) else float(pseudo_diag["highest_observed_mean_bps"]), "not available" if is_missing(pseudo_diag["highest_observed_mean_bps"]) else f"{float(pseudo_diag['highest_observed_mean_bps']):.2f} bps ({pseudo_diag['highest_observed_eligible_group']})", CSV_OUTPUTS["sector_diagnostics"]),
            evidence("lowest_observed_mean_bps", None if is_missing(pseudo_diag["lowest_observed_mean_bps"]) else float(pseudo_diag["lowest_observed_mean_bps"]), "not available" if is_missing(pseudo_diag["lowest_observed_mean_bps"]) else f"{float(pseudo_diag['lowest_observed_mean_bps']):.2f} bps ({pseudo_diag['lowest_observed_eligible_group']})", CSV_OUTPUTS["sector_diagnostics"]),
            evidence("pseudo_recovered_eligible_weighted_mean", pseudo_weighted_mean, format_bps(pseudo_weighted_mean), CSV_OUTPUTS["pseudo_sector_summary"]),
            evidence("configured_label_level", label_level, label_level, CSV_OUTPUTS["pseudo_sector_coverage"]),
        ],
        "Pseudo-sector dispersion is interpreted against the recovered-population baseline and is not compared as though it shared the direct-SIC universe.",
        [PSEUDO_MODEL_LIMITATION, UPSTREAM_MODEL_METRICS, PSEUDO_PROVENANCE_LIMITATION, GROSS_COST_LIMITATION],
        "Version the upstream model/output, add calibrated confidence, and test time/security-type stability within the recovered universe.",
    )

    mean_signs = set(int(float(value) > 0) - int(float(value) < 0) for value in daily["mean_abn_ret_cc"])
    event_status = "mixed" if len(mean_signs - {0}) > 1 else "informative"
    pre_mean = float(
        daily[daily["offset"].astype(int) < 0]["mean_abn_ret_cc"].astype(float).mean()
    )
    post_mean = float(
        daily[daily["offset"].astype(int) > 0]["mean_abn_ret_cc"].astype(float).mean()
    )
    cumulative_mean_path = float(daily["mean_abn_ret_cc"].astype(float).sum())
    event_section = section(
        "event_time",
        "Event-time and ex-date overnight behavior",
        event_status,
        f"The largest absolute daily mean occurred at offset {int(largest_daily['offset']):+d} ({format_bps(largest_daily['mean_abn_ret_cc'])}); offset 0 was {format_bps(offset_zero['mean_abn_ret_cc'])}, average pre/post-offset means were {format_bps(pre_mean)} / {format_bps(post_mean)}, the cumulative descriptive mean path was {format_bps(cumulative_mean_path)}, and the separate ex-open capture mean was {format_bps(overnight['mean_capture_ret'])}.",
        [
            evidence("largest_abs_daily_offset", int(largest_daily["offset"]), f"{int(largest_daily['offset']):+d}", CSV_OUTPUTS["event_time_daily"]),
            evidence("largest_abs_daily_mean_abn_ret_cc", float(largest_daily["mean_abn_ret_cc"]), format_bps(largest_daily["mean_abn_ret_cc"]), CSV_OUTPUTS["event_time_daily"]),
            evidence("offset_zero_mean_abn_ret_cc", float(offset_zero["mean_abn_ret_cc"]), format_bps(offset_zero["mean_abn_ret_cc"]), CSV_OUTPUTS["event_time_daily"]),
            evidence("pre_offset_average_mean_abn_ret_cc", pre_mean, format_bps(pre_mean), CSV_OUTPUTS["event_time_daily"]),
            evidence("post_offset_average_mean_abn_ret_cc", post_mean, format_bps(post_mean), CSV_OUTPUTS["event_time_daily"]),
            evidence("cumulative_descriptive_mean_path", cumulative_mean_path, format_bps(cumulative_mean_path), CSV_OUTPUTS["event_time_daily"]),
            evidence("mean_capture_ret", float(overnight["mean_capture_ret"]), format_bps(overnight["mean_capture_ret"]), CSV_OUTPUTS["event_time_overnight"]),
            evidence("mean_abn_overnight_ret", float(overnight["mean_abn_overnight_ret"]), format_bps(overnight["mean_abn_overnight_ret"]), CSV_OUTPUTS["event_time_overnight"]),
        ],
        "The close-to-close event curve and ex-open strategy exit are separate measurements and should not be conflated.",
        [DEPENDENCE_LIMITATION, GROSS_COST_LIMITATION],
        "Inspect calendar-date concentration, then estimate ticker/date-clustered uncertainty and stability by period.",
    )

    business = section(
        "business_implications",
        "Business implications and next steps",
        "mixed",
        f"The largest ordered descriptive spread was {float(strongest['high_minus_low_bps']):.2f} bps in {strongest['dimension']}, while pseudo-sector left {unresolved:,} base events unresolved; both findings prioritize validation work rather than a trading recommendation.",
        [
            evidence("largest_ordered_spread_bps", float(strongest["high_minus_low_bps"]), f"{float(strongest['high_minus_low_bps']):.2f} bps ({strongest['dimension']})", CSV_OUTPUTS["dimension_diagnostics"]),
            evidence("still_unresolved_events", unresolved, format_count(unresolved), CSV_OUTPUTS["pseudo_sector_coverage"]),
        ],
        "The report identifies where additional measurement can reduce uncertainty; it does not convert descriptive differences into execution decisions.",
        [GROSS_COST_LIMITATION, TAXONOMY_SEPARATION_STATEMENT],
        "Prioritize measured costs/taxes, point-in-time sector sourcing, model version/confidence, clustered inference, and time/security-type stability checks.",
    )

    limitations = section(
        "limitations",
        "Limitations, methodology, and reconciliation appendix",
        "limited_coverage",
        f"All three sector states reconcile to {int(pseudo['base_events']):,} base events with join row delta {int(pseudo['join_row_delta']):,}; interpretation remains constrained by gross returns, taxonomy provenance, dependence, and upstream data limitations.",
        [
            evidence("base_events", int(pseudo["base_events"]), format_count(pseudo["base_events"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("join_row_delta", int(pseudo["join_row_delta"]), format_count(pseudo["join_row_delta"]), CSV_OUTPUTS["pseudo_sector_coverage"]),
            evidence("event_reconciliation_passed", bool(pseudo["event_reconciliation_passed"]), str(bool(pseudo["event_reconciliation_passed"])), CSV_OUTPUTS["pseudo_sector_coverage"]),
        ],
        "The appendix makes population identities and provenance limitations explicit so each claim can be traced without rescanning M1.",
        [
            GROSS_COST_LIMITATION,
            SIC_TEMPORAL_LIMITATION,
            PSEUDO_MODEL_LIMITATION,
            UPSTREAM_MODEL_METRICS,
            PSEUDO_PROVENANCE_LIMITATION,
            DEPENDENCE_LIMITATION,
            CORPORATE_ACTION_LIMITATION,
            "Pseudo-sector coverage is structurally limited to the upstream eligible active CS/ADRC universe.",
        ],
        "Complete live HDFS/YARN output review, numeric/visual reconciliation, and formal sign-off for the accepted immutable run.",
    )

    sections = [
        executive,
        data_quality,
        coverage,
        overall_section,
        yield_section,
        volatility_section,
        liquidity_section,
        direct_section,
        pseudo_section,
        event_section,
        business,
        limitations,
    ]
    expected = [identifier for identifier, _ in REPORT_SECTIONS]
    actual = [item["section_id"] for item in sections]
    if actual != expected:
        raise ReportArtifactError(
            f"Insight section order mismatch: expected={expected}, actual={actual}"
        )
    payload = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc
        or dt.datetime.now(dt.timezone.utc).isoformat(),
        "insight_generation_version": INSIGHT_GENERATION_VERSION,
        "sections": sections,
    }
    validate_insight_payload(payload)
    return payload


def validate_insight_payload(payload: Mapping[str, Any]) -> None:
    sections = list(payload.get("sections", []))
    expected = [identifier for identifier, _ in REPORT_SECTIONS]
    actual = [item.get("section_id") for item in sections]
    if actual != expected:
        raise ReportArtifactError(
            f"section_insights.json sections differ from the required order: {actual}"
        )
    prohibited = (
        "profitable",
        "best sector",
        "causes",
        "statistically significant",
        "predicts returns",
        "tradeable alpha",
    )
    for item in sections:
        required = {
            "section_id",
            "title",
            "status",
            "headline",
            "evidence",
            "business_interpretation",
            "caveats",
            "recommended_next_step",
        }
        missing = sorted(required - set(item))
        if missing:
            raise ReportArtifactError(
                f"Insight section {item.get('section_id')} lacks {missing}"
            )
        if item["status"] not in ALLOWED_INSIGHT_STATUSES:
            raise ReportArtifactError(
                f"Insight section {item['section_id']} has invalid status"
            )
        if not item["evidence"]:
            raise ReportArtifactError(
                f"Insight section {item['section_id']} has no evidence"
            )
        for entry in item["evidence"]:
            if not entry.get("metric") or not entry.get("source_table"):
                raise ReportArtifactError(
                    f"Insight evidence is not traceable in {item['section_id']}"
                )
        affirmative_text = " ".join(
            (
                str(item["headline"]),
                str(item["business_interpretation"]),
                str(item["recommended_next_step"]),
            )
        ).lower()
        matches = [phrase for phrase in prohibited if phrase in affirmative_text]
        if matches:
            raise ReportArtifactError(
                f"Prohibited affirmative claim(s) {matches} in {item['section_id']}"
            )
