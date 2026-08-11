# M2 Cross-Sectional V2 — Build Handoff

## Build status

```text
STATIC CODE BUILD: PASS
UPLOADED-CSV CHART REGENERATION / LOCAL VISUAL QA: PASS
LIVE SPARK/HDFS/YARN PREFLIGHT: NOT RUN
FINAL HDFS OUTPUTS: NOT RUN
LIVE ACCEPTED-RUN REPORT GENERATION / VISUAL QA: NOT RUN
FORMAL ANALYSIS ACCEPTANCE: PENDING OPERATOR RUNTIME
```

The repository implementation and every locally applicable V2 gate are
complete as of 2026-08-11. This handoff was written after code, tests, README,
operator runbook, and static review, as required. It does not claim an HDFS/YARN
run or accepted analysis result.

Repository HEAD inspected during the build:

```text
34c36221e2ace2c942099a3d40de947133ca0d4d
```

The worktree contains the V2 implementation and pre-existing user changes, so
the operator must record the final committed implementation SHA after review.
Current config SHA-256 at handoff:

```text
8ED8E69556AD72008A4D9CFC8008402D86A3A7CA96FCFCA9986D1CD0B2CA93B9
```

## Scope implemented

- Preserved the full-base direct/current-reference `sic_description` analysis,
  including `UNKNOWN` and low-N canonical rows.
- Added a config-driven `$TEAM/curated/pseudo_sector` Parquet contract with
  required schema, explicit label-level selection, normalization, blank and
  duplicate audits, conflict blocking, and one-row-per-ticker validation.
- Preserved case-sensitive vendor ticker identifiers in the pseudo join after
  live diagnostics found 7,313 legitimate mixed-case preferred-share events
  and six uppercase collision groups (for example `CPK` versus `CpK`). Join
  keys are trim-only; M1 grain/panel identifiers are not rewritten.
- Added a many-to-one pseudo join with a hard row-count assertion and separate
  `direct_sic_known`, `pseudo_recovered`, and `still_unresolved` states.
- Audited pseudo matches on direct-known rows while excluding them from pseudo
  performance analysis.
- Added pseudo source/coverage/bridge audits, event reconciliations, separate
  pseudo summaries, dimension diagnostics, sector diagnostics, and a richer
  manifest.
- Expanded every standard summary with metric-specific N, event/ticker shares,
  sample standard deviation, standard error, unadjusted descriptive 95% bounds,
  median/IQR, rates, and event/ticker/report eligibility.
- Retained benchmark exclusion, key/history/offset/identity gates, immutable
  run roots, separate feature/outcome handoffs, and explicit feature leakage
  controls.
- Added one authoritative HDFS/report/artifact registry shared by both jobs.
- Refactored the two long scripts into stable 46/59-line entrypoints with the
  runner implementation isolated under `m2lib/runner/` and report
  implementation isolated under `m2lib/report/`. Every implementation module
  is 420 lines or fewer; the documented YARN client-mode commands are unchanged
  and require no `--py-files` archive because no Python UDF code is shipped to
  executors.
- Implemented all F01–F12 report functions, full CSV/JSON exports, immutable
  local-output checks, report reconciliation, deterministic category selection,
  twelve stable insight sections, evidence traceability, claim safeguards, and
  required limitations. Figure numbers remain stable internal artifact IDs and
  filenames; visible chart titles and labels use business-friendly language.
- Updated the user README and created the V2 manual operator runbook against the
  actual CLI, config, paths, outputs, failure recovery, and review workflow.
- Made static discovery independent of the ignored build-package directory and
  made an unavailable plain-Python Java gateway a recorded Spark-test skip;
  the README/runbook require those two tests to be executed separately with
  `spark-submit --master local[1] --deploy-mode client` on the cluster.

No pseudo-sector retraining, combined sector-performance taxonomy, transaction-
cost backtest, options work, dashboard, or predictive-return model was added.

## Files changed by this build

Implementation/config/documentation:

```text
m2_cross_sectional/cross_sectional_config.json
m2_cross_sectional/m2_contract.py
m2_cross_sectional/run_cross_sectional.py
m2_cross_sectional/make_report_artifacts.py
m2_cross_sectional/m2lib/__init__.py
m2_cross_sectional/m2lib/runner/__init__.py
m2_cross_sectional/m2lib/runner/config.py
m2_cross_sectional/m2lib/runner/validation.py
m2_cross_sectional/m2lib/runner/sources.py
m2_cross_sectional/m2lib/runner/metrics.py
m2_cross_sectional/m2lib/runner/sectors.py
m2_cross_sectional/m2lib/runner/summaries.py
m2_cross_sectional/m2lib/runner/events.py
m2_cross_sectional/m2lib/runner/outputs.py
m2_cross_sectional/m2lib/runner/pipeline.py
m2_cross_sectional/m2lib/report/__init__.py
m2_cross_sectional/m2lib/report/config.py
m2_cross_sectional/m2lib/report/tables.py
m2_cross_sectional/m2lib/report/figures_core.py
m2_cross_sectional/m2lib/report/figures.py
m2_cross_sectional/m2lib/report/labels.py
m2_cross_sectional/m2lib/report/insight_core.py
m2_cross_sectional/m2lib/report/insights.py
m2_cross_sectional/m2lib/report/output.py
m2_cross_sectional/README.md
m2_cross_sectional/.m2_cross_sectional_build_package_2/operator_handoff/MANUAL_TERMINAL_RUNBOOK.md
m2_cross_sectional/.m2_cross_sectional_build_package_2/operator_handoff/BUILD_HANDOFF.md
```

Tests/fixtures:

```text
m2_cross_sectional/tests/test_feature_leakage.py
m2_cross_sectional/tests/test_group_summary.py
m2_cross_sectional/tests/test_chart_smoke.py
m2_cross_sectional/tests/test_config_contract.py
m2_cross_sectional/tests/test_contract_consistency.py
m2_cross_sectional/tests/test_group_summary_v2.py
m2_cross_sectional/tests/test_insight_generation.py
m2_cross_sectional/tests/test_pseudo_sector_contract.py
m2_cross_sectional/tests/test_report_contract.py
m2_cross_sectional/tests/test_sector_population_rules.py
m2_cross_sectional/tests/test_spark_contract.py
m2_cross_sectional/tests/v2_fixtures.py
```

Protected worktree evidence:

- `.gitignore` was already modified before this build and was not edited.
- `.m2_cross_sectional_build_package_1` remains present and unchanged. All eight
  file SHA-256 values rechecked exactly against the pre-edit baseline.
- The seven V2 source Markdown files and package README were used as the build
  and review source and were not modified.
- No unrelated repository area was edited.

## Checks actually run

### Pre-edit baseline

```text
python -m json.tool m2_cross_sectional/cross_sectional_config.json
  PASS (exit 0)

python -m py_compile m2_cross_sectional/run_cross_sectional.py
  PASS (exit 0)

python -m py_compile m2_cross_sectional/make_report_artifacts.py
  PASS (exit 0)

python -m unittest discover -s m2_cross_sectional/tests -p 'test_*.py' -v
  PASS: 13 run, 13 passed, 0 skipped
```

### Final local checks

```text
python -m json.tool m2_cross_sectional/cross_sectional_config.json
  PASS (exit 0)

python -m py_compile m2_cross_sectional/m2_contract.py \
  m2_cross_sectional/run_cross_sectional.py \
  m2_cross_sectional/make_report_artifacts.py
  PASS (exit 0)

python -m compileall -q m2_cross_sectional/m2lib
  PASS (exit 0)

python -m unittest discover -s m2_cross_sectional/tests -p 'test_*.py' -v
  PASS (exit 0)
  63 tests discovered
  60 passed
  3 skipped for unavailable PySpark
  0 failed
  0 errors
```

The three exact skips were:

```text
3 tiny local-Spark join/summary/case-preservation tests:
  PySpark is not installed in the local interpreter
```

The chart tests ran under a disposable isolated QA environment with matplotlib
3.10.9 and pandas 2.3.3. All four chart tests passed; the environment is not a
project dependency and was removed after verification.

Pure/pandas tests did run pseudo schema/conflict/level/duplicate behavior,
sector-state truth tables, reconciliation failures, metric-specific N and exact
summary math, eligibility, ordered and sector diagnostics, leakage, registries,
immutable local output, unit conversions, JSON nonfinite handling, category
selection/order, nonfigure report generation, report reconciliation, all twelve
insight sections, evidence links, low-N suppression, language separation,
flat/mixed behavior, prohibited claims, limitations, and reproducibility.

Additional checks actually run:

```text
python m2_cross_sectional/run_cross_sectional.py --help
  PASS; flags are --config, --run-id, --mode {preflight,final}

python m2_cross_sectional/make_report_artifacts.py --help
  PASS; flags are --config, --run-id, --output-dir

git ... diff --check
  PASS; no whitespace errors (Git printed only Windows LF/CRLF warnings)

runtime forbidden combined-sector search
  PASS; no coalesce/direct+pseudo, sector_final, effective_sector, or
  blended_sector runtime matches

claim-language search
  REVIEWED; matches are the generator rejection list and its negative test

shared registry assertion
  PASS: 23 HDFS outputs, 20 compact report tables, 20 CSVs,
  12 figures, and 12 insight sections; writer/reader sets reconcile

implementation layout and line-limit assertions
  PASS: runner and report implementations occupy separate packages, both
  stable entrypoints are below 100 lines, and every implementation module is
  at most 420 lines (the enforced ceiling is 425)

module global-reference scan
  PASS: all 16 implementation modules imported without PySpark installed and
  every function LOAD_GLOBAL reference resolved to its defining module or a
  Python builtin

package _1 SHA-256 comparison
  PASS; all baseline hashes unchanged
```

Local dependency inventory actually observed:

```text
Python 3.10.5
pandas 2.2.0
PySpark unavailable
matplotlib unavailable
pyarrow unavailable
hdfs command unavailable
spark-submit command unavailable
```

The inventory above describes the default Windows interpreter. A disposable
isolated QA environment supplied matplotlib only for the chart correction and
visual verification described below.

Ruff, Flake8, and mypy were not installed; their absence is not represented as
a pass. Python compile, unit/contract tests, source searches, and diff checks are
the locally available static evidence.

## Checks not run

The following remain explicitly pending and must not be marked passed from this
handoff:

- live HDFS existence/readability/schema inspection for grain, panel, metadata,
  and pseudo-sector;
- live pseudo row/ticker/label/level/blank/duplicate/conflict counts and sample;
- Spark/YARN V2 preflight and its live key/history/identity/coverage values;
- many-to-one join and expanded summary tests on the real Spark runtime;
- immutable final HDFS write and all Parquet path/count reconciliations;
- model handoff schema/key review on final Parquet outputs;
- accepted-run report generation in the live report runtime, PNG visual
  inspection, numeric sampling,
  narrative-to-JSON-to-CSV-to-HDFS traceability, and formal sign-off.

The Windows workspace has neither `hdfs` nor `spark-submit`; PySpark and
matplotlib imports are also unavailable in its default interpreter. No live
count, recovery percentage, runtime UNKNOWN share, HDFS output, accepted-run
figure, or accepted run ID was fabricated.

## Exact operator commands

Run from the Linux repository root. The manual runbook contains the concrete
pseudo-source inspection script and the full review workflow.

```bash
set -euo pipefail
export TEAM=/user/ms16965_nyu_edu/divcap
CONFIG=m2_cross_sectional/cross_sectional_config.json
RUN_ID="m2_v2_$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_DIR="m2_cross_sectional/results/$RUN_ID"
RUN_ROOT="$TEAM/m2/cross_sectional/$RUN_ID"

python3 -m json.tool "$CONFIG" >/dev/null
python3 -m py_compile m2_cross_sectional/m2_contract.py
python3 -m py_compile m2_cross_sectional/run_cross_sectional.py
python3 -m py_compile m2_cross_sectional/make_report_artifacts.py
python3 -m compileall -q m2_cross_sectional/m2lib
python3 -m unittest discover -s m2_cross_sectional/tests -p 'test_*.py' -v
spark-submit --master local[1] --deploy-mode client \
  m2_cross_sectional/tests/test_spark_contract.py -v
python3 -c "import pandas, matplotlib; print('pandas', pandas.__version__, 'matplotlib', matplotlib.__version__)"

hdfs dfs -test -e "$TEAM/curated/div_event_grain"
hdfs dfs -test -e "$TEAM/curated/div_event_panel"
hdfs dfs -test -e "$TEAM/reference/tickers_all_5y_metadata.jsonl"
hdfs dfs -test -e "$TEAM/curated/pseudo_sector"

if hdfs dfs -test -e "$RUN_ROOT"; then
  printf 'STOP: run root already exists: %s\n' "$RUN_ROOT" >&2
  exit 2
fi
```

Inspect the live pseudo source using the exact temporary Spark script in
`MANUAL_TERMINAL_RUNBOOK.md` section 4. It prints schema, total/distinct counts,
label levels, blanks, exact duplicates, conflicts by ticker, and samples.

Preflight (writes nothing):

```bash
spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  m2_cross_sectional/run_cross_sectional.py \
  --config "$CONFIG" \
  --run-id "$RUN_ID" \
  --mode preflight 2>&1 | tee "m2_v2_${RUN_ID}_preflight.log"
```

Final, using the same config/run ID only after passing preflight review:

```bash
spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  m2_cross_sectional/run_cross_sectional.py \
  --config "$CONFIG" \
  --run-id "$RUN_ID" \
  --mode final 2>&1 | tee "m2_v2_${RUN_ID}_final.log"
```

Verify inventory:

```bash
hdfs dfs -ls -R "$RUN_ROOT"

for relative in \
  analysis_base model_features model_outcomes \
  audit/input_summary audit/schema_summary audit/sample_funnel audit/sic_coverage \
  audit/pseudo_sector_coverage audit/pseudo_sector_contract \
  audit/sector_coverage_bridge audit/bucket_boundaries audit/metric_identities \
  audit/dimension_diagnostics audit/sector_diagnostics \
  core/overall core/yield core/volatility core/liquidity core/sic_description \
  core/pseudo_sector core/event_time_daily core/event_time_overnight \
  manifest/run_metadata
do
  hdfs dfs -test -e "$RUN_ROOT/$relative" || exit 2
  hdfs dfs -count "$RUN_ROOT/$relative"
done
```

Generate the report into a new directory:

```bash
spark-submit --master yarn --deploy-mode client \
  m2_cross_sectional/make_report_artifacts.py \
  --config "$CONFIG" \
  --run-id "$RUN_ID" \
  --output-dir "$REPORT_DIR" 2>&1 | tee "m2_v2_${RUN_ID}_report.log"

find "$REPORT_DIR" -type f -printf '%P\t%s bytes\n' | sort
```

## Expected HDFS outputs

Under `$TEAM/m2/cross_sectional/<RUN_ID>/`:

```text
analysis_base
model_features
model_outcomes
audit/input_summary
audit/schema_summary
audit/sample_funnel
audit/sic_coverage
audit/pseudo_sector_coverage
audit/pseudo_sector_contract
audit/sector_coverage_bridge
audit/bucket_boundaries
audit/metric_identities
audit/dimension_diagnostics
audit/sector_diagnostics
core/overall
core/yield
core/volatility
core/liquidity
core/sic_description
core/pseudo_sector
core/event_time_daily
core/event_time_overnight
manifest/run_metadata
```

Required final identities:

```text
join_row_delta = 0
base = direct known + direct unknown
direct unknown = pseudo recovered + still unresolved
base = direct known + pseudo recovered + still unresolved
sum core/sic_description.n_events = base
core/sic_description UNKNOWN = direct unknown audit
sum core/pseudo_sector.n_events = pseudo recovered
bridge event shares sum to 1 within tolerance
event-time offsets = -4 through +3
```

`core/sic_description` must retain `UNKNOWN`; `core/pseudo_sector` must contain
only recovered rows and one configured label level. A zero recovered population
is a blocking source/data issue.

## Expected local report outputs

Twenty CSV files:

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

Machine/narrative files:

```text
report_metrics.json
section_insights.json
INSIGHTS_SUMMARY.md
RESULTS_README.md
```

Figures:

```text
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

## Failure recovery

- Missing/unreadable pseudo path: verify `pseudo_sector_path` and upstream write;
  do not switch label levels silently.
- Missing schema/configured level: inspect live schema/distribution and rebuild
  the upstream asset; there is no fallback.
- Conflicting labels/levels: inspect reported ticker examples and rebuild one
  usable label/level per ticker. Exact duplicate rows may be deduplicated only
  with their audit count retained.
- Join delta/reconciliation/leakage failure: stop before writes; fix source/code,
  rerun local checks and preflight.
- Existing or partial HDFS root: preserve it for review and select a new run ID.
  Do not delete/overwrite it to retry.
- Existing/nonempty local report directory: use a new directory and regenerate
  every artifact. Never manually repair CSV/JSON/Markdown/PNG output.
- Final failure after any path write: fix the source of truth and rerun under a
  new immutable ID.

## Pseudo-sector provenance questions

The operator/upstream owner should record, not infer:

1. Exact producer code commit, training-data snapshot, training UTC time, and
   source checksum for the current Parquet asset.
2. Whether the writer was invoked with `hybrid --write` and whether the asset
   was overwritten after the documented 0.683 accuracy / 0.670 weighted-F1 run.
3. Whether model version, prediction confidence, training timestamp, and source
   checksum can be added to a future output.
4. Whether prediction stability has been checked by time and security type.
5. Whether point-in-time direct sector metadata is available for historical
   events.

Until answered, the manifest/report must continue to disclose missing model
version/confidence/training time and that M2 did not revalidate the upstream
metrics.

## Report review steps

1. Reconcile all CSV population counts, metric-specific N, shares, bridge, and
   manifest values before looking at charts.
2. Confirm `report_metrics.json` contains every compact table and no nonfinite
   JSON literal.
3. Trace every `section_insights.json` evidence item to its named CSV.
4. Trace every `INSIGHTS_SUMMARY.md` headline number through JSON, CSV, named
   HDFS aggregate, and accepted manifest/run ID.
5. Open F01–F12 and inspect resolution, clipping/overlap, long labels, units,
   signs, valid N, ticker counts, zero lines, provenance titles, and footnotes.
6. Sample at least three plotted values per figure, including a negative/null
   value when available, and reconcile after documented rounding.
7. Confirm F07/F08 exclude `UNKNOWN`; F09/F10 use only recovered pseudo rows;
   each paired taxonomy figure has identical category order.
8. Confirm low-N rows remain in CSVs but never become report headlines.
9. Confirm direct SIC and pseudo-sector are never presented as equivalent
   universes and the bridge is coverage-only.
10. Confirm gross/cost/tax, current-reference SIC, model-derived pseudo,
    upstream-metric-not-revalidated, missing-confidence/version, structural
    coverage, dependence, and corporate-action limitations are present.
11. Reject unsupported causal, significance, predictive, net-profitability,
    superlative, or trading-recommendation language.
12. Complete the V2 review sign-off with run ID, committed SHA, config checksum,
    live counts/rates, HDFS/local paths, reviewer, and UTC date.

## Static review conclusion

R01–R11 pass for the local code build:

- scope/archive and unrelated work preserved;
- CLI/config/TEAM resolution consistent;
- direct SIC behavior and UNKNOWN retained;
- pseudo source, uniqueness, label-level, join, and provenance controls present;
- three states disjoint and reconciled; no combined performance sector exists;
- expanded summary/eligibility/low-N contracts present;
- V1 keys, identities, benchmark, history, immutability, and leakage controls
  retained;
- all required HDFS and local artifacts are registered;
- F01–F12 and twelve traceable insight sections implemented;
- long implementations are organized into separate bounded `m2lib/runner` and
  `m2lib/report` packages while the existing client-mode `spark-submit`
  interfaces remain unchanged;
- all runnable local checks pass with exact dependency skips recorded;
- README/runbook/handoff match the implementation.

R12–R30 remain operator-runtime/report-acceptance work and cannot inherit the
static pass.

## Post-build chart readability correction

The uploaded local report `m2_v2_20260811T051406Z` exposed presentation
problems that were not visible while matplotlib was unavailable in the default
Windows interpreter. The correction is presentation-only: canonical CSV
values, raw taxonomy labels, metric calculations, eligibility rules, HDFS
contracts, and artifact filenames are unchanged.

Changes made:

- Removed internal F01–F12 identifiers from every visible chart title while
  preserving numbered filenames and registry keys for traceability.
- Replaced technical abbreviations in titles, axes, legends, annotations, and
  tick labels with business-friendly wording.
- Translated two-digit pseudo-sector SIC major groups into readable names while
  retaining each SIC number in parentheses; translated division fallbacks such
  as `Manufacturing (other)` to `Other Manufacturing` for display only.
- Moved dense sector metric values into a separate right-hand value column so
  confidence intervals cannot cover signs or digits.
- Added explicit units and readable quantile ranges, event-day descriptions,
  sample definitions, and average/median terminology.
- Corrected title, legend, footer, and boundary-label spacing and expanded the
  overnight chart limits so endpoint annotations are not clipped.

Verification actually performed:

```text
m2_cross_sectional/.qa_venv/Scripts/python.exe -m unittest discover \
  -s m2_cross_sectional/tests -p 'test_*.py' -v
  PASS: 63 discovered, 60 passed, 3 skipped (PySpark unavailable),
  0 failed, 0 errors

12 PNGs regenerated locally from the uploaded report CSVs
  PASS: all 12 opened and visually inspected
  PASS: displayed values and signs checked, including a negative sector value
  PASS: titles contain no visible F01–F12 identifiers
  PASS: labels, units, legends, annotations, and long sector names are readable
```

The regenerated PNGs were disposable review artifacts and were removed after
inspection. The user-uploaded report was preserved unchanged. The operator must
regenerate the complete report into a new, empty local output directory from
the accepted immutable run; do not replace individual PNGs in an existing
report. No Spark, HDFS, or YARN check was rerun for this presentation-only
correction, so R12–R30 and formal analysis acceptance remain pending.
