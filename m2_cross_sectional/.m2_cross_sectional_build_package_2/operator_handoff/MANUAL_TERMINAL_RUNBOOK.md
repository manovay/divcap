# M2 Cross-Sectional V2 — Manual Terminal Runbook

Run from the Linux repository root. This procedure separates source inspection,
non-writing preflight, immutable final output, report generation, and acceptance.
Do not reuse a partial HDFS run ID or a nonempty local report directory.

## 1. Establish the environment

```bash
set -euo pipefail
export TEAM=/user/ms16965_nyu_edu/divcap
CONFIG=m2_cross_sectional/cross_sectional_config.json
RUN_ID="m2_v2_$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_DIR="m2_cross_sectional/results/$RUN_ID"
RUN_ROOT="$TEAM/m2/cross_sectional/$RUN_ID"
printf 'RUN_ID=%s\nRUN_ROOT=%s\nREPORT_DIR=%s\n' "$RUN_ID" "$RUN_ROOT" "$REPORT_DIR"
```

Keep the printed values with the operator log. `RUN_ID` may be reused between a
passing preflight and its final run only while `RUN_ROOT` does not exist.

## 2. Run local static checks

```bash
python3 -m json.tool "$CONFIG" >/dev/null
python3 -m py_compile m2_cross_sectional/m2_contract.py
python3 -m py_compile m2_cross_sectional/run_cross_sectional.py
python3 -m py_compile m2_cross_sectional/make_report_artifacts.py
python3 -m compileall -q m2_cross_sectional/m2lib
python3 -m unittest discover -s m2_cross_sectional/tests -p 'test_*.py' -v
python3 -c "import pandas, matplotlib; print('pandas', pandas.__version__, 'matplotlib', matplotlib.__version__)"
spark-submit --master local[1] --deploy-mode client \
  m2_cross_sectional/tests/test_spark_contract.py -v
```

Record the test count and every skip. A dependency-based skip remains pending
until run in the Spark/report environment. Plain Python discovery may skip the
three Spark tests when its local Java gateway is unavailable; the explicit
`spark-submit` command is required to execute them on the cluster.

The stable scripts load their implementation from the separate sibling
`m2lib/runner` and `m2lib/report` packages. With the required
`--deploy-mode client` commands below, no
`--py-files` bundle is needed: the driver runs from the repository checkout,
and the current implementation does not send Python UDF code to executors.

## 3. Verify all canonical paths

```bash
hdfs dfs -test -e "$TEAM/curated/div_event_grain"
hdfs dfs -test -e "$TEAM/curated/div_event_panel"
hdfs dfs -test -e "$TEAM/reference/tickers_all_5y_metadata.jsonl"
hdfs dfs -test -e "$TEAM/curated/pseudo_sector"

if hdfs dfs -test -e "$RUN_ROOT"; then
  printf 'STOP: run root already exists: %s\n' "$RUN_ROOT" >&2
  exit 2
fi

if [ -e "$REPORT_DIR" ] && [ -n "$(find "$REPORT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  printf 'STOP: local report directory is nonempty: %s\n' "$REPORT_DIR" >&2
  exit 2
fi
```

Do not delete an existing root merely to reuse its ID. Preserve it for failure
review and select a new run ID.

## 4. Inspect the live pseudo-sector source

The following creates a temporary inspection script, runs it with Spark, and
removes it on exit. It prints schema, row/ticker/label counts, label levels,
blank counts, exact duplicates, conflicting labels/levels by ticker, and sample
rows.

```bash
INSPECT_SCRIPT="$(mktemp /tmp/m2_v2_pseudo_inspect.XXXXXX.py)"
trap 'rm -f "$INSPECT_SCRIPT"' EXIT

cat >"$INSPECT_SCRIPT" <<'PY'
import os
from pyspark.sql import SparkSession, functions as F

team = os.environ["TEAM"].rstrip("/")
path = f"{team}/curated/pseudo_sector"
spark = SparkSession.builder.appName("m2-v2-pseudo-inspect").getOrCreate()
frame = spark.read.parquet(path)
print("PATH", path)
frame.printSchema()
missing = sorted({"ticker", "pseudo_sector", "label_level"} - set(frame.columns))
print("MISSING_REQUIRED_COLUMNS", missing)
if missing:
    raise SystemExit(2)
normalized = frame.select(
    F.trim(F.col("ticker").cast("string")).alias("ticker"),
    F.trim(F.col("pseudo_sector").cast("string")).alias("pseudo_sector"),
    F.trim(F.col("label_level").cast("string")).alias("label_level"),
)
normalized.agg(
    F.count("*").alias("rows"),
    F.countDistinct("ticker").alias("distinct_tickers"),
    F.countDistinct("pseudo_sector").alias("distinct_pseudo_labels"),
    F.sum(F.when(F.col("ticker").isNull() | (F.col("ticker") == ""), 1).otherwise(0)).alias("blank_ticker_rows"),
    F.sum(F.when(F.col("pseudo_sector").isNull() | (F.col("pseudo_sector") == ""), 1).otherwise(0)).alias("blank_label_rows"),
    F.sum(F.when(F.col("label_level").isNull() | (F.col("label_level") == ""), 1).otherwise(0)).alias("blank_level_rows"),
).show(truncate=False)
normalized.groupBy("label_level").count().orderBy("label_level").show(100, truncate=False)
print("EXACT_DUPLICATE_GROUPS")
normalized.groupBy("ticker", "pseudo_sector", "label_level").count().filter("count > 1").orderBy(F.desc("count")).show(100, truncate=False)
print("CONFLICTING_LABEL_TICKERS")
normalized.groupBy("ticker").agg(F.countDistinct("pseudo_sector").alias("n"), F.sort_array(F.collect_set("pseudo_sector")).alias("values")).filter("n > 1").show(100, truncate=False)
print("CONFLICTING_LEVEL_TICKERS")
normalized.groupBy("ticker").agg(F.countDistinct("label_level").alias("n"), F.sort_array(F.collect_set("label_level")).alias("values")).filter("n > 1").show(100, truncate=False)
print("SAMPLE")
frame.orderBy("ticker").show(20, truncate=False)
spark.stop()
PY

spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-memory 2g --executor-cores 1 \
  "$INSPECT_SCRIPT" | tee "m2_v2_${RUN_ID}_pseudo_inspect.log"
```

Pass requirements:

- required columns are present;
- configured `hybrid` level is present;
- no ticker has conflicting nonblank labels or levels;
- configured-level usable rows are unique by normalized ticker;
- sample values are recognizably label text, not confidence scores or another
  field.

Exact duplicate rows may exist; the M2 loader audits and deduplicates them.
Blank/malformed rows are audited and excluded from the usable lookup. A source
that recovers zero direct-SIC-unknown base events still blocks V2.

## 5. Run non-writing preflight

```bash
spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  m2_cross_sectional/run_cross_sectional.py \
  --config "$CONFIG" \
  --run-id "$RUN_ID" \
  --mode preflight 2>&1 | tee "m2_v2_${RUN_ID}_preflight.log"
```

Confirm the log includes `=== PREFLIGHT PASSED ===` and explicitly review:

- grain/panel key counts and date/history range;
- panel offset coverage;
- metric-identity violation counts (all zero above tolerance);
- direct SIC source, conflicts, listing-date blocks, known/UNKNOWN counts/share;
- pseudo schema, row/ticker/label counts, levels, blanks, duplicates, conflicts;
- pseudo join row delta (zero);
- known/unknown/recovered/unresolved counts and all three event identities;
- pseudo matches on direct-known events (audited, excluded from pseudo analysis);
- nonzero recovered population;
- report-eligible group counts and dimension/sector diagnostics;
- output registry/nonempty-table checks;
- the statement that no output was written.

The planning value 76.36% is not an expected constant. Record the recomputed
UNKNOWN share from this run.

### Preflight failure

1. Do not run final mode.
2. Classify the numbered failure as config/code, M1 grain/panel, direct metadata,
   pseudo source, or history coverage.
3. Inspect/fix the source of truth; do not patch a planned output.
4. If `RUN_ROOT` exists for any reason, keep it and choose a new `RUN_ID`.
5. Rerun static checks, source inspection when relevant, and preflight.

For a missing pseudo source, run the upstream producer only after confirming its
intended label level and overwrite implications. For a conflict, inspect the
reported tickers and rebuild a unique source. M2 never selects an arbitrary row
or falls back to another level.

## 6. Run immutable final mode

Use the same config and run ID as the accepted preflight:

```bash
spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  m2_cross_sectional/run_cross_sectional.py \
  --config "$CONFIG" \
  --run-id "$RUN_ID" \
  --mode final 2>&1 | tee "m2_v2_${RUN_ID}_final.log"
```

If final mode fails after any destination is written, do not retry with the same
ID. Inspect the partial root and choose a new ID after correcting the cause.

## 7. Verify every HDFS output

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
  hdfs dfs -test -e "$RUN_ROOT/$relative" || {
    printf 'MISSING %s\n' "$RUN_ROOT/$relative" >&2
    exit 2
  }
  hdfs dfs -count "$RUN_ROOT/$relative"
done
```

Use Spark to inspect the compact audit/core tables and confirm:

```text
sum core/sic_description.n_events = base events
UNKNOWN n_events = audit/sic_coverage unknown events
sum core/pseudo_sector.n_events = pseudo-recovered events
bridge has exactly three states and event shares sum to 1
event-time offsets are exactly -4 through +3
model_features and model_outcomes keys/row counts reconcile
model_features contains no sector, state/source, outcome, or bucket leakage
manifest values and output paths match the accepted run
```

## 8. Generate the immutable local report

```bash
if [ -e "$REPORT_DIR" ] && [ -n "$(find "$REPORT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  printf 'STOP: report directory is nonempty: %s\n' "$REPORT_DIR" >&2
  exit 2
fi

spark-submit --master yarn --deploy-mode client \
  m2_cross_sectional/make_report_artifacts.py \
  --config "$CONFIG" \
  --run-id "$RUN_ID" \
  --output-dir "$REPORT_DIR" 2>&1 | tee "m2_v2_${RUN_ID}_report.log"

find "$REPORT_DIR" -type f -printf '%P\t%s bytes\n' | sort
```

The job requires every compact V2 audit/core/manifest path and rejects a
nonempty local directory. If generation fails, fix code/environment/data and
regenerate the whole package in a new directory. Never edit a PNG, CSV, JSON, or
narrative by hand.

## 9. Numeric, visual, and narrative QA

Review in order:

1. Reconcile CSV event counts, metric-specific N, shares, and three sector states.
2. Confirm `report_metrics.json` contains every exported aggregate and valid JSON
   with no `NaN`/`Infinity`.
3. Trace each `section_insights.json` evidence item to its named CSV.
4. Trace every `INSIGHTS_SUMMARY.md` headline number to the JSON evidence.
5. Open all figures F01–F12; inspect clipping, overlap, long labels, units,
   numeric labels, valid N/tickers, zero lines, and readable resolution.
6. Sample at least three plotted values per figure (including negative/null when
   available) and reconcile them after documented rounding.
7. Confirm F07/F08 exclude `UNKNOWN`; F09/F10 use recovered pseudo rows only;
   each taxonomy pair has the same category order.
8. Confirm direct and pseudo titles/colors/populations cannot be confused.
9. Reject any affirmative claim of causality, statistical significance,
   predictive power, net profitability, or an equivalent-population direct/pseudo
   comparison.
10. Confirm all required limitations appear in executive, taxonomy, conclusion,
    and appendix sections.

## 10. Acceptance record

Record in the V2 review form:

```text
run ID
code commit
config path and checksum
input date range
base event/ticker counts
direct-SIC known and UNKNOWN shares
pseudo label level
pseudo recovery rate among direct-SIC unknown
residual unresolved share
pseudo provenance limitations
HDFS run root
local report path
static/Spark/report test results and skips
reviewer and UTC date
```

The repository build may be accepted after static implementation review. The
analysis is not accepted until live preflight, immutable final output, numeric
and visual report QA, and formal V2 review all pass.

## 11. Provenance questions for the operator/upstream owner

Record answers if available; do not infer them:

1. What exact code commit, training-data snapshot, and UTC training time created
   `$TEAM/curated/pseudo_sector`?
2. Was the writer invoked with `hybrid --write`, and was the output replaced
   after the documented 0.683 accuracy / 0.670 weighted-F1 run?
3. Can model version, prediction confidence, training timestamp, and source
   checksum be added to a future asset?
4. Has prediction stability been checked across time and security type?
5. Is a point-in-time direct-sector source available for historical events?

Until answered, the manifest/report must continue to say those provenance
fields are unavailable and that M2 did not revalidate the upstream model.
