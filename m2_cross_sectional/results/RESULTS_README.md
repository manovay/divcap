# M2 Cross-Sectional V2 Results — m2_v2_20260811T064045Z

This immutable package was generated from compact, run-versioned V2 audit/core/manifest aggregates. It does not rescan M1 inputs or the M2 analysis base.

## Run snapshot

- HDFS run root: `/user/ms16965_nyu_edu/divcap/m2/cross_sectional/m2_v2_20260811T064045Z`
- Ex-date range: 2021-08-09 to 2026-08-07
- Base population: 161,296 events / 9,010 tickers
- Mean / median gross abnormal capture: 9.97 bps / 5.90 bps
- Direct-SIC UNKNOWN share: 76.4%
- Pseudo recovery among direct-SIC unknown: 5.5%
- Residual unresolved share: 72.2%
- Pseudo label level: `hybrid`

Returns are gross and descriptive; transaction costs, taxes, and net tradeability are not modeled.

## Narrative

- `INSIGHTS_SUMMARY.md` — deterministic twelve-section professional report.
- `section_insights.json` — traceable section objects and numeric evidence.
- `report_metrics.json` — machine-readable copy of every loaded aggregate.

## CSV tables

- `input_summary.csv`
- `schema_summary.csv`
- `sample_funnel.csv`
- `sic_coverage.csv`
- `pseudo_sector_coverage.csv`
- `pseudo_sector_contract.csv`
- `sector_coverage_bridge.csv`
- `bucket_boundaries.csv`
- `metric_identities.csv`
- `dimension_diagnostics.csv`
- `sector_diagnostics.csv`
- `overall_summary.csv`
- `yield_summary.csv`
- `volatility_summary.csv`
- `liquidity_summary.csv`
- `sic_description_summary.csv`
- `pseudo_sector_summary.csv`
- `event_time_daily.csv`
- `event_time_overnight.csv`
- `run_metadata.csv`

## Figures

- `figures/01_sample_funnel.png`
- `figures/02_sector_coverage_recovery.png`
- `figures/03_overall_capture_metrics.png`
- `figures/04_yield_capture_profile.png`
- `figures/05_volatility_capture_profile.png`
- `figures/06_liquidity_capture_profile.png`
- `figures/07_sic_description_capture_profile.png`
- `figures/08_sic_description_outcome_rates.png`
- `figures/09_pseudo_sector_capture_profile.png`
- `figures/10_pseudo_sector_outcome_rates.png`
- `figures/11_event_time_daily_profile.png`
- `figures/12_event_time_overnight_metrics.png`

Figure filenames retain the report registry numbers, while visible titles and labels use plain business language. Two-digit model labels are decoded in the figures—for example, `SIC 73` is displayed as `Business Services (SIC 73)`—without changing the canonical CSV value.

## Interpretation and provenance

- Direct/current-reference SIC and model-predicted pseudo-sector are kept in separate analytical populations; the coverage bridge is not a blended sector-performance taxonomy.
- Direct-SIC performance figures use report-eligible known labels only; the canonical CSV retains UNKNOWN and every low-N category.
- Pseudo-sector figures use only direct-SIC-unknown events recovered by a valid configured-level model prediction; direct-known rows are excluded.
- A pseudo-sector label such as `SIC 73` is a model-predicted two-digit SIC major group, not an observed four-digit company SIC. Hybrid `(other)` labels are division-level fallbacks.
- Liquidity means pre-event dollar-volume liquidity / size proxy, not historical market capitalization.
- Event-time offset 0 is ex-date close-to-close; the ex-open strategy exit is shown separately in the overnight table and overnight figure.
- Direct SIC is current-reference metadata and is not guaranteed to be a point-in-time classification.
- Pseudo-sector labels are model-derived from current company descriptions and SIC-labelled training rows; M2 does not revalidate the upstream model.
- The upstream hybrid model documents held-out accuracy 0.683 and weighted F1 0.670; these are upstream results, not M2 runtime validation.
- The upstream pseudo-sector output has no prediction-confidence, model-version, or training-timestamp field and is structurally limited to active CS/ADRC rows with usable descriptions.
- The unadjusted descriptive intervals do not account for dependence within ticker or calendar date.
- Corporate-action contamination in upstream unadjusted prices remains an upstream limitation.

Every narrative number names a CSV source in `INSIGHTS_SUMMARY.md`; use the accepted manifest to trace that CSV back to the canonical HDFS aggregate and input contract.
