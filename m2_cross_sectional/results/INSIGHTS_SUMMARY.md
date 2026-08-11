# M2 Cross-Sectional V2 Insights — m2_v2_20260811T064045Z

Generated at UTC: `2026-08-11T07:00:57.347011+00:00`

## Executive summary

Status: `informative`

161,296 accepted events across 9,010 tickers from 2021-08-09 to 2026-08-07; overall mean/median abnormal capture 9.97 bps / 5.90 bps; direct-SIC unknown 76.4%; pseudo recovery among unknown 5.5%; largest ordered high-minus-low spread 24.28 bps in yield; largest absolute daily event-time mean at offset +0: -87.14 bps.

Evidence:

- `base_events`: 161,296 (source: `overall_summary.csv`)
- `base_tickers`: 9,010 (source: `overall_summary.csv`)
- `min_ex_date`: 2021-08-09 (source: `input_summary.csv`)
- `max_ex_date`: 2026-08-07 (source: `input_summary.csv`)
- `mean_capture_ret_abn`: 9.97 bps (source: `overall_summary.csv`)
- `median_capture_ret_abn`: 5.90 bps (source: `overall_summary.csv`)
- `direct_sic_unknown_share`: 76.4% (source: `sic_coverage.csv`)
- `pseudo_recovery_rate_of_sic_unknown`: 5.5% (source: `pseudo_sector_coverage.csv`)
- `strongest_ordered_high_minus_low_bps`: 24.28 bps (yield) (source: `dimension_diagnostics.csv`)
- `largest_abs_event_time_mean`: offset +0: -87.14 bps (source: `event_time_daily.csv`)

Business interpretation: The accepted aggregates describe where gross capture outcomes differ in this sample and which data-quality or coverage gaps remain most material.

Caveats:

- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.
- Direct/current-reference SIC and model-predicted pseudo-sector are kept in separate analytical populations; the coverage bridge is not a blended sector-performance taxonomy.

Recommended next step: Prioritize stability, cost, point-in-time taxonomy, and clustered-inference checks before treating any descriptive pattern as decision-ready.

## Data quality and sample funnel

Status: `informative`

From 2021-08-09 to 2026-08-07 (1824 calendar days), the base retained 161,296 of 163,358 raw events (98.7%); the largest sequential loss was 2,042 events at has_core, and metric-identity violations totaled 0.

Evidence:

- `raw_events`: 163,358 (source: `sample_funnel.csv`)
- `base_events`: 161,296 (source: `sample_funnel.csv`)
- `base_cumulative_retention`: 98.7% (source: `sample_funnel.csv`)
- `min_ex_date`: 2021-08-09 (source: `input_summary.csv`)
- `max_ex_date`: 2026-08-07 (source: `input_summary.csv`)
- `history_span_days`: 1,824 days (source: `input_summary.csv`)
- `largest_filter_loss_events`: 2,042 at has_core (source: `sample_funnel.csv`)
- `metric_identity_violations`: 0 (source: `metric_identities.csv`)

Business interpretation: The funnel separates acceptance filters from downstream analysis-population diagnostics, preserving exact denominators.

Caveats:

- Corporate-action contamination in upstream unadjusted prices remains an upstream limitation.

Recommended next step: Confirm the live key, history-span, and identity gates in the accepted-run review and investigate any future retention shift.

## Sector coverage and pseudo-sector recovery

Status: `limited_coverage`

Direct SIC was known for 38,123 base events and unknown for 123,173; model-predicted pseudo-sector recovered 6,735 unknown events (5.5%), leaving 116,438 unresolved.

Evidence:

- `direct_sic_known_events`: 38,123 (source: `pseudo_sector_coverage.csv`)
- `direct_sic_unknown_events`: 123,173 (source: `pseudo_sector_coverage.csv`)
- `pseudo_recovered_events`: 6,735 (source: `pseudo_sector_coverage.csv`)
- `pseudo_recovery_rate_of_sic_unknown`: 5.5% (source: `pseudo_sector_coverage.csv`)
- `still_unresolved_events`: 116,438 (source: `pseudo_sector_coverage.csv`)
- `coverage_after_recovery_share`: 27.8% (source: `pseudo_sector_coverage.csv`)

Business interpretation: Recovery expands coverage accounting for otherwise-unknown events while keeping observed/current-reference and model-derived taxonomies analytically separate.

Caveats:

- Direct/current-reference SIC and model-predicted pseudo-sector are kept in separate analytical populations; the coverage bridge is not a blended sector-performance taxonomy.
- The upstream pseudo-sector output has no prediction-confidence, model-version, or training-timestamp field and is structurally limited to active CS/ADRC rows with usable descriptions.

Recommended next step: Track residual coverage by security type and obtain versioned/confidence-bearing upstream predictions before broader use.

## Overall dividend-capture performance

Status: `informative`

Across 161,296 events, mean and median gross abnormal capture were 9.97 bps and 5.90 bps; 54.6% of valid observations were positive.

Evidence:

- `mean_capture_ret_abn`: 9.97 bps (source: `overall_summary.csv`)
- `median_capture_ret_abn`: 5.90 bps (source: `overall_summary.csv`)
- `mean_capture_ret`: 13.12 bps (source: `overall_summary.csv`)
- `median_capture_ret`: 7.82 bps (source: `overall_summary.csv`)
- `positive_capture_ret_abn_rate`: 54.6% (source: `overall_summary.csv`)
- `median_drop_ratio`: 0.870 (source: `overall_summary.csv`)
- `p25_drop_ratio`: 0.270 (source: `overall_summary.csv`)
- `p75_drop_ratio`: 1.365 (source: `overall_summary.csv`)
- `drop_ratio_lt_1_rate`: 59.1% (source: `overall_summary.csv`)

Business interpretation: Mean, median, rate, and drop-ratio evidence should be read together because each summarizes a different aspect of the outcome distribution.

Caveats:

- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.
- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.

Recommended next step: Add transaction-cost/tax scenarios and ticker/date-clustered uncertainty in a separately specified validation stage.

## Dividend-yield cross section

Status: `informative`

Low/high bucket mean abnormal capture was 1.19 bps / 25.47 bps, with medians 0.02 bps / 17.63 bps; the high-minus-low mean spread was 24.28 bps and was monotonic in the observed bucket means.

Evidence:

- `low_bucket_mean_capture_ret_abn`: 1.19 bps (source: `yield_summary.csv`)
- `high_bucket_mean_capture_ret_abn`: 25.47 bps (source: `yield_summary.csv`)
- `low_bucket_median_capture_ret_abn`: 0.02 bps (source: `yield_summary.csv`)
- `high_bucket_median_capture_ret_abn`: 17.63 bps (source: `yield_summary.csv`)
- `high_minus_low_bps`: 24.28 bps (source: `dimension_diagnostics.csv`)
- `mean_range_bps`: 24.28 bps (source: `dimension_diagnostics.csv`)
- `positive_rate_range_pp`: 10.02 percentage points (source: `dimension_diagnostics.csv`)
- `monotonic_step_count`: 4/4 nondecreasing steps (source: `dimension_diagnostics.csv`)
- `actual_bucket_count`: 5 (source: `dimension_diagnostics.csv`)

Business interpretation: The profile prioritizes where deeper stability and implementation-cost checks may be most informative; it is a descriptive association.

Caveats:

- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.
- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.

Recommended next step: Repeat the profile by year and security type, then evaluate measured implementation costs without changing the canonical all-sample table.

## Pre-event volatility cross section

Status: `informative`

Low/high bucket mean abnormal capture was 7.51 bps / 14.49 bps, with medians 3.36 bps / 8.73 bps; the high-minus-low mean spread was 6.98 bps and was monotonic in the observed bucket means.

Evidence:

- `low_bucket_mean_capture_ret_abn`: 7.51 bps (source: `volatility_summary.csv`)
- `high_bucket_mean_capture_ret_abn`: 14.49 bps (source: `volatility_summary.csv`)
- `low_bucket_median_capture_ret_abn`: 3.36 bps (source: `volatility_summary.csv`)
- `high_bucket_median_capture_ret_abn`: 8.73 bps (source: `volatility_summary.csv`)
- `high_minus_low_bps`: 6.98 bps (source: `dimension_diagnostics.csv`)
- `mean_range_bps`: 6.98 bps (source: `dimension_diagnostics.csv`)
- `positive_rate_range_pp`: 3.21 percentage points (source: `dimension_diagnostics.csv`)
- `monotonic_step_count`: 4/4 nondecreasing steps (source: `dimension_diagnostics.csv`)
- `actual_bucket_count`: 5 (source: `dimension_diagnostics.csv`)

Business interpretation: The profile prioritizes where deeper stability and implementation-cost checks may be most informative; it is a descriptive association.

Caveats:

- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.
- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.

Recommended next step: Repeat the profile by year and security type, then evaluate measured implementation costs without changing the canonical all-sample table.

## Pre-event dollar-volume liquidity / size-proxy cross section

Status: `mixed`

Low/high bucket mean abnormal capture was 12.81 bps / 6.23 bps, with medians 6.45 bps / 3.51 bps; the high-minus-low mean spread was -6.58 bps and was non-monotonic across the observed buckets.

Evidence:

- `low_bucket_mean_capture_ret_abn`: 12.81 bps (source: `liquidity_summary.csv`)
- `high_bucket_mean_capture_ret_abn`: 6.23 bps (source: `liquidity_summary.csv`)
- `low_bucket_median_capture_ret_abn`: 6.45 bps (source: `liquidity_summary.csv`)
- `high_bucket_median_capture_ret_abn`: 3.51 bps (source: `liquidity_summary.csv`)
- `high_minus_low_bps`: -6.58 bps (source: `dimension_diagnostics.csv`)
- `mean_range_bps`: 7.19 bps (source: `dimension_diagnostics.csv`)
- `positive_rate_range_pp`: 2.86 percentage points (source: `dimension_diagnostics.csv`)
- `monotonic_step_count`: 1/4 nondecreasing steps (source: `dimension_diagnostics.csv`)
- `actual_bucket_count`: 5 (source: `dimension_diagnostics.csv`)

Business interpretation: The profile prioritizes where deeper stability and implementation-cost checks may be most informative; it is a descriptive association.

Caveats:

- This dimension is pre-event dollar-volume liquidity / size proxy, not historical market capitalization. The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.
- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.

Recommended next step: Repeat the profile by year and security type, then evaluate measured implementation costs without changing the canonical all-sample table.

## Direct SIC description analysis

Status: `informative`

Among 89 report-eligible direct SIC categories, the highest- and lowest-observed means were 47.60 and -18.39 bps; the eligible range was 65.99 bps. Mean, median, and outcome-rate evidence was broadly aligned.

Evidence:

- `known_sic_events`: 38,123 (source: `sector_diagnostics.csv`)
- `known_sic_tickers`: 2,208 (source: `sector_diagnostics.csv`)
- `category_count`: 331 (source: `sector_diagnostics.csv`)
- `eligible_category_count`: 89 (source: `sector_diagnostics.csv`)
- `top_1_event_share`: 15.6% (source: `sector_diagnostics.csv`)
- `top_5_event_share`: 39.3% (source: `sector_diagnostics.csv`)
- `highest_observed_mean_bps`: 47.60 bps (ASSET-BACKED SECURITIES) (source: `sector_diagnostics.csv`)
- `lowest_observed_mean_bps`: -18.39 bps (PIPE LINES (NO NATURAL GAS)) (source: `sector_diagnostics.csv`)
- `highest_observed_median_capture_ret_abn`: 28.58 bps (source: `sic_description_summary.csv`)
- `lowest_observed_median_capture_ret_abn`: -26.20 bps (source: `sic_description_summary.csv`)
- `highest_observed_positive_rate`: 68.4% (source: `sic_description_summary.csv`)
- `lowest_observed_positive_rate`: 41.6% (source: `sic_description_summary.csv`)

Business interpretation: Current-reference direct SIC patterns describe the covered subset only; UNKNOWN remains in the canonical table and coverage section, not the known-label ranking.

Caveats:

- Direct SIC is current-reference metadata and is not guaranteed to be a point-in-time classification.
- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.
- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.

Recommended next step: Obtain point-in-time sector metadata and repeat the direct-SIC profile across time before using category differences for prioritization.

## Pseudo-sector analysis

Status: `informative`

Within 6,735 recovered events assigned model-predicted pseudo-sector labels at level hybrid, 18 categories were report-eligible; the highest/lowest observed means were 45.62/-17.60 bps versus the recovered eligible-category weighted baseline 24.08 bps, and the eligible range was 63.22 bps.

Evidence:

- `pseudo_recovered_events`: 6,735 (source: `pseudo_sector_coverage.csv`)
- `pseudo_recovered_tickers`: 462 (source: `pseudo_sector_coverage.csv`)
- `pseudo_recovery_rate_of_sic_unknown`: 5.5% (source: `pseudo_sector_coverage.csv`)
- `category_count`: 27 (source: `sector_diagnostics.csv`)
- `eligible_category_count`: 18 (source: `sector_diagnostics.csv`)
- `top_1_event_share`: 23.3% (source: `sector_diagnostics.csv`)
- `top_5_event_share`: 56.4% (source: `sector_diagnostics.csv`)
- `eligible_mean_range_bps`: 63.22 bps (source: `sector_diagnostics.csv`)
- `highest_observed_mean_bps`: 45.62 bps (SIC 73) (source: `sector_diagnostics.csv`)
- `lowest_observed_mean_bps`: -17.60 bps (SIC 13) (source: `sector_diagnostics.csv`)
- `pseudo_recovered_eligible_weighted_mean`: 24.08 bps (source: `pseudo_sector_summary.csv`)
- `configured_label_level`: hybrid (source: `pseudo_sector_coverage.csv`)

Business interpretation: Pseudo-sector dispersion is interpreted against the recovered-population baseline and is not compared as though it shared the direct-SIC universe.

Caveats:

- Pseudo-sector labels are model-derived from current company descriptions and SIC-labelled training rows; M2 does not revalidate the upstream model.
- The upstream hybrid model documents held-out accuracy 0.683 and weighted F1 0.670; these are upstream results, not M2 runtime validation.
- The upstream pseudo-sector output has no prediction-confidence, model-version, or training-timestamp field and is structurally limited to active CS/ADRC rows with usable descriptions.
- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.

Recommended next step: Version the upstream model/output, add calibrated confidence, and test time/security-type stability within the recovered universe.

## Event-time and ex-date overnight behavior

Status: `mixed`

The largest absolute daily mean occurred at offset +0 (-87.14 bps); offset 0 was -87.14 bps, average pre/post-offset means were 6.82 bps / 0.10 bps, the cumulative descriptive mean path was -59.57 bps, and the separate ex-open capture mean was 13.12 bps.

Evidence:

- `largest_abs_daily_offset`: +0 (source: `event_time_daily.csv`)
- `largest_abs_daily_mean_abn_ret_cc`: -87.14 bps (source: `event_time_daily.csv`)
- `offset_zero_mean_abn_ret_cc`: -87.14 bps (source: `event_time_daily.csv`)
- `pre_offset_average_mean_abn_ret_cc`: 6.82 bps (source: `event_time_daily.csv`)
- `post_offset_average_mean_abn_ret_cc`: 0.10 bps (source: `event_time_daily.csv`)
- `cumulative_descriptive_mean_path`: -59.57 bps (source: `event_time_daily.csv`)
- `mean_capture_ret`: 13.12 bps (source: `event_time_overnight.csv`)
- `mean_abn_overnight_ret`: -75.58 bps (source: `event_time_overnight.csv`)

Business interpretation: The close-to-close event curve and ex-open strategy exit are separate measurements and should not be conflated.

Caveats:

- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.
- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.

Recommended next step: Inspect calendar-date concentration, then estimate ticker/date-clustered uncertainty and stability by period.

## Business implications and next steps

Status: `mixed`

The largest ordered descriptive spread was 24.28 bps in yield, while pseudo-sector left 116,438 base events unresolved; both findings prioritize validation work rather than a trading recommendation.

Evidence:

- `largest_ordered_spread_bps`: 24.28 bps (yield) (source: `dimension_diagnostics.csv`)
- `still_unresolved_events`: 116,438 (source: `pseudo_sector_coverage.csv`)

Business interpretation: The report identifies where additional measurement can reduce uncertainty; it does not convert descriptive differences into execution decisions.

Caveats:

- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.
- Direct/current-reference SIC and model-predicted pseudo-sector are kept in separate analytical populations; the coverage bridge is not a blended sector-performance taxonomy.

Recommended next step: Prioritize measured costs/taxes, point-in-time sector sourcing, model version/confidence, clustered inference, and time/security-type stability checks.

## Limitations, methodology, and reconciliation appendix

Status: `limited_coverage`

All three sector states reconcile to 161,296 base events with join row delta 0; interpretation remains constrained by gross returns, taxonomy provenance, dependence, and upstream data limitations.

Evidence:

- `base_events`: 161,296 (source: `pseudo_sector_coverage.csv`)
- `join_row_delta`: 0 (source: `pseudo_sector_coverage.csv`)
- `event_reconciliation_passed`: True (source: `pseudo_sector_coverage.csv`)

Business interpretation: The appendix makes population identities and provenance limitations explicit so each claim can be traced without rescanning M1.

Caveats:

- Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.
- Direct SIC is current-reference metadata and is not guaranteed to be a point-in-time classification.
- Pseudo-sector labels are model-derived from current company descriptions and SIC-labelled training rows; M2 does not revalidate the upstream model.
- The upstream hybrid model documents held-out accuracy 0.683 and weighted F1 0.670; these are upstream results, not M2 runtime validation.
- The upstream pseudo-sector output has no prediction-confidence, model-version, or training-timestamp field and is structurally limited to active CS/ADRC rows with usable descriptions.
- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.
- Corporate-action contamination in upstream unadjusted prices remains an upstream limitation.
- Pseudo-sector coverage is structurally limited to the upstream eligible active CS/ADRC universe.

Recommended next step: Complete live HDFS/YARN output review, numeric/visual reconciliation, and formal sign-off for the accepted immutable run.
