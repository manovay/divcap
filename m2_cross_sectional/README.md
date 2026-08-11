# M2 Cross-Sectional Analysis V2

This directory implements the descriptive M2 dividend-capture cross-sectional
analysis. V2 preserves the direct/current-reference `sic_description` analysis
and separately analyzes model-predicted `pseudo_sector` labels for otherwise
unknown events that the upstream pseudo-sector asset can recover.

The taxonomies never become one performance dimension:

```text
base_event
├── direct_sic_known       -> direct SIC analysis only
└── direct_sic_unknown
    ├── pseudo_recovered   -> pseudo-sector analysis only
    └── still_unresolved   -> coverage accounting only
```

The coverage bridge reconciles those three states, but is not a blended sector
ranking. The build is descriptive: it does not establish causality, statistical
significance, predictive power, net profitability, or tradeability.

## Files

- `cross_sectional_config.json` — canonical paths and V2 thresholds.
- `m2_contract.py` — shared HDFS/report/figure/section registries.
- `run_cross_sectional.py` — Spark preflight and immutable final build.
- `make_report_artifacts.py` — compact aggregates to an immutable local report.
- `m2lib/runner/` — runner-only configuration, validation, Spark sources,
  metrics, sectors, summaries, event time, outputs, and orchestration.
- `m2lib/report/` — report-only loading, reconciliation, figures, insights,
  and immutable local-output writers.
- Every implementation module is kept near 400 lines or less.
- `tests/` — pure, report, chart, and dependency-aware Spark contract tests.
- `.m2_cross_sectional_build_package_1/` — archived V1 build package; unchanged.
- `.m2_cross_sectional_build_package_2/` — V2 implementation/review source.
- `.m2_cross_sectional_build_package_2/operator_handoff/MANUAL_TERMINAL_RUNBOOK.md`
  — exact Linux/HDFS/YARN operating procedure.
- `.m2_cross_sectional_build_package_2/operator_handoff/BUILD_HANDOFF.md`
  — final static-build record (written after implementation and review).

## Inputs and populations

Canonical defaults are:

```text
$TEAM/curated/div_event_grain
$TEAM/curated/div_event_panel
$TEAM/reference/tickers_all_5y_metadata.jsonl
$TEAM/curated/pseudo_sector
```

The grain key remains `(ticker, ex_date)` and the panel key remains
`(ticker, ex_date, offset)`. The accepted base is unchanged:

```text
cash_amount > 0
AND has_core = true
AND window_contiguous = true
AND ticker != market_ticker
```

Direct SIC is enriched before pseudo-sector. If the grain lacks
`sic_description`, metadata is projected to `ticker`, `active`, `list_date`,
and `sic_description`; conflicting direct labels block the run, the listing-date
guard is retained, and blank/unresolved labels become `UNKNOWN`. Direct SIC is
current-reference metadata and is not guaranteed point-in-time.

The pseudo source must be readable Parquet with:

```text
ticker
pseudo_sector
label_level
```

`sec_type` is optional and audit-only. Tickers are trimmed and uppercased to
match the validated event-grain key; label text and level are trimmed without
case folding. Blank rows and exact duplicates are audited. Conflicting labels
or levels by ticker, an absent configured label level, non-uniqueness after
filtering, a row-multiplying join, zero recovered events, or reconciliation
failure blocks both preflight and final mode. Matches on direct-SIC-known rows
are counted but excluded from pseudo analysis.

The upstream writer is documented as a hybrid SIC-derived classifier for active
CS/ADRC rows with usable current descriptions. Its documented held-out hybrid
accuracy is 0.683 and weighted F1 is 0.670; M2 does not revalidate those values.
The source lacks prediction confidence, model version, and training timestamp,
and is structurally unable to recover every unknown event.

## Configuration

| Key | Default / contract |
|---|---|
| `team` | `/user/ms16965_nyu_edu/divcap` |
| `grain_path` | `$TEAM/curated/div_event_grain` canonical path |
| `panel_path` | `$TEAM/curated/div_event_panel` canonical path |
| `metadata_path` | `$TEAM/reference/tickers_all_5y_metadata.jsonl` |
| `pseudo_sector_path` | `$TEAM/curated/pseudo_sector` |
| `output_root` | `$TEAM/m2/cross_sectional` |
| `sic_description_column` | exactly `sic_description` |
| `pseudo_sector_column` | exactly `pseudo_sector` |
| `pseudo_sector_label_level` | `hybrid` |
| `market_ticker` | `SPY`, excluded from every analysis/handoff |
| `min_history_days` | `1640` |
| `bucket_count` | `5` (allowed 2–20; duplicate cuts collapse) |
| `min_cell_n` | `30`, direct/ordered event threshold |
| `min_pseudo_cell_n` | `30`, pseudo-sector event threshold |
| `min_report_tickers` | `5`, applies with the event threshold |
| `event_offset_min`, `event_offset_max` | exactly `-4`, `3` |
| `primary_metric` | exactly `capture_ret_abn` |
| `secondary_metric` | exactly `capture_ret` |
| `academic_metric` | exactly `drop_ratio` |
| `metric_tolerance` | `1e-8` |
| `report_top_sic_n` | `20` known direct categories at most |
| `report_top_pseudo_n` | `20` pseudo categories at most |
| `report_numeric_labels` | `true` for accepted numeric figures |
| `insight_min_abs_bps` | `1.0`, wording materiality only |

An exported `TEAM` replaces the configured team prefix for all canonical input
and output paths, including `pseudo_sector_path`. It does not rewrite unrelated
explicit paths.

## Local checks

From the repository root:

```bash
python3 -m json.tool m2_cross_sectional/cross_sectional_config.json >/dev/null
python3 -m py_compile m2_cross_sectional/m2_contract.py
python3 -m py_compile m2_cross_sectional/run_cross_sectional.py
python3 -m py_compile m2_cross_sectional/make_report_artifacts.py
python3 -m compileall -q m2_cross_sectional/m2lib
python3 -m unittest discover -s m2_cross_sectional/tests -p 'test_*.py' -v
```

On a Spark gateway, exercise the two tiny-DataFrame integration tests through
Spark's launcher so Java and Spark environment variables are configured:

```bash
spark-submit --master local[1] --deploy-mode client \
  m2_cross_sectional/tests/test_spark_contract.py -v
```

Plain `python3` discovery skips those two tests when PySpark is missing or its
local Java gateway cannot start. The explicit `spark-submit` run is the runtime
acceptance check and must pass on the cluster.

Pure tests do not require HDFS. Tiny-Spark tests skip with a recorded reason when
PySpark is unavailable; figure smoke tests do the same when matplotlib is
unavailable. Those skips are pending runtime checks, not passes.

## Preflight

The documented YARN client-mode commands need no `--py-files` archive. Both
entrypoints import the sibling `m2lib` package from the submitted repository
checkout on the driver, and the implementation uses Spark SQL/DataFrame
expressions rather than Python UDFs that would require shipping those modules
to executors.

Choose a new run ID:

```bash
export TEAM=/user/ms16965_nyu_edu/divcap
RUN_ID="m2_v2_$(date -u +%Y%m%dT%H%M%SZ)"

spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  m2_cross_sectional/run_cross_sectional.py \
  --config m2_cross_sectional/cross_sectional_config.json \
  --run-id "$RUN_ID" \
  --mode preflight
```

Preflight reads live inputs and performs the same validation/analysis work as
final mode: schemas and keys, history/offset coverage, direct SIC conflicts and
listing-date guard, pseudo schema/levels/conflicts/duplicates, join cardinality,
three-state reconciliation, metric identities, sample funnel, bucket summaries,
eligibility, diagnostics, event offsets, model-handoff leakage, output-registry
completeness, and nonempty required tables. It prints exact coverage values and
writes nothing.

If the intended run root already exists, choose a new ID. Do not reuse a partial
or accepted root.

## Immutable final build

After reviewing a passing preflight, change only the mode:

```bash
spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  m2_cross_sectional/run_cross_sectional.py \
  --config m2_cross_sectional/cross_sectional_config.json \
  --run-id "$RUN_ID" \
  --mode final
```

Final mode repeats all checks before any write. Each Parquet destination uses
`errorifexists`. A failure after a partial write requires a new run ID after the
root is inspected; never overwrite or merge a partial run.

## HDFS output contract

```text
$TEAM/m2/cross_sectional/<RUN_ID>/
    analysis_base/
    model_features/
    model_outcomes/
    audit/
        input_summary/
        schema_summary/
        sample_funnel/
        sic_coverage/
        pseudo_sector_coverage/
        pseudo_sector_contract/
        sector_coverage_bridge/
        bucket_boundaries/
        metric_identities/
        dimension_diagnostics/
        sector_diagnostics/
    core/
        overall/
        yield/
        volatility/
        liquidity/
        sic_description/
        pseudo_sector/
        event_time_daily/
        event_time_overnight/
    manifest/
        run_metadata/
```

`core/sic_description` uses the full base and retains `UNKNOWN` and low-N rows.
`core/pseudo_sector` uses only `pseudo_recovered` events and persists the
configured `label_level`. The coverage bridge has exactly the three ordered
provenance states and is never consumed as a performance taxonomy.

All grouped tables include metric-specific valid N, event/ticker shares, mean,
sample standard deviation, standard error, unadjusted descriptive 95% bounds,
median/IQR, outcome rates, and event/ticker/report-eligibility flags. The bounds
do not adjust for repeated tickers or clustered calendar dates.

`analysis_base` may carry separate direct/pseudo provenance fields for audit.
`model_features` continues to use its explicit whitelist and excludes direct
SIC, pseudo-sector, label level, state/source fields, any combined-sector field,
outcomes, and full-sample bucket labels. Outcomes remain in `model_outcomes`.

## Local report package

Confirm the report runtime, then use a new local directory:

```bash
python3 -c "import pandas, matplotlib; print(pandas.__version__, matplotlib.__version__)"

spark-submit --master yarn --deploy-mode client \
  m2_cross_sectional/make_report_artifacts.py \
  --config m2_cross_sectional/cross_sectional_config.json \
  --run-id "$RUN_ID" \
  --output-dir "m2_cross_sectional/results/$RUN_ID"
```

The report reads every compact audit/core/manifest path, never raw M1 data or
`analysis_base`. A nonempty output directory is rejected. It exports one CSV per
loaded aggregate, including:

```text
input_summary.csv
schema_summary.csv
sample_funnel.csv
sic_coverage.csv
pseudo_sector_coverage.csv
pseudo_sector_contract.csv
sector_coverage_bridge.csv
bucket_boundaries.csv
metric_identities.csv
dimension_diagnostics.csv
sector_diagnostics.csv
overall_summary.csv
yield_summary.csv
volatility_summary.csv
liquidity_summary.csv
sic_description_summary.csv
pseudo_sector_summary.csv
event_time_daily.csv
event_time_overnight.csv
run_metadata.csv
```

It also writes:

```text
report_metrics.json
section_insights.json
INSIGHTS_SUMMARY.md
RESULTS_README.md
figures/01_sample_funnel.png
figures/02_sector_coverage_recovery.png
figures/03_overall_capture_metrics.png
figures/04_yield_capture_profile.png
figures/05_volatility_capture_profile.png
figures/06_liquidity_capture_profile.png
figures/07_sic_description_capture_profile.png
figures/08_sic_description_outcome_rates.png
figures/09_pseudo_sector_capture_profile.png
figures/10_pseudo_sector_outcome_rates.png
figures/11_event_time_daily_profile.png
figures/12_event_time_overnight_metrics.png
```

F07/F08 exclude `UNKNOWN` and select the largest report-eligible known direct
SIC descriptions; F09/F10 independently select pseudo categories from the
recovered population. Canonical CSVs retain every category. Paired performance
and rate figures use the same deterministic ordering. `section_insights.json`
and `INSIGHTS_SUMMARY.md` always contain the twelve required sections, source
each numeric item to a named CSV, suppress low-N headlines, and template the
gross/cost and taxonomy limitations.

## Failure recovery and limitations

- Missing/unreadable pseudo source: verify the configured HDFS path and rerun
  the upstream writer; no alternate label level is selected automatically.
- Pseudo conflicts: inspect the reported ticker examples and rebuild a unique
  one-label/one-level-per-ticker asset. Exact duplicates are audited/deduped.
- Join/reconciliation failure: stop; inspect ticker normalization/source
  uniqueness. Do not write or manually repair aggregates.
- Existing/partial HDFS root: inspect it, preserve evidence, and select a new
  run ID after fixing the source of truth.
- Existing/partial local report: choose a new output directory and regenerate
  the entire package; never manually edit a PNG or narrative.
- Returns are gross and descriptive; transaction costs and taxes are not
  modeled.
- Direct SIC is current-reference rather than guaranteed point-in-time.
- Pseudo-sector is model-derived from current descriptions/SIC-labelled rows;
  upstream validation is documented but not revalidated by M2, and confidence,
  model version, and training timestamp are unavailable.
- Pseudo coverage is limited to the upstream eligible universe.
- Unadjusted intervals ignore within-ticker/calendar-date dependence.
- Corporate-action contamination remains an upstream limitation.

See the V2 operator runbook for source inspection, HDFS verification, report QA,
and formal sign-off steps.
