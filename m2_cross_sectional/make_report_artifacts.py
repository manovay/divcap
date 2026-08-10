#!/usr/bin/env python3
"""Export compact M2 Spark results to report-ready local artifacts.

The script reads only run-specific aggregated audit/core/manifest outputs.  It
does not read raw prices, the M1 panel, or the event-grain analysis base.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

TABLE_PATHS = {
    "input_summary": "audit/input_summary",
    "sample_funnel": "audit/sample_funnel",
    "bucket_boundaries": "audit/bucket_boundaries",
    "sic_coverage": "audit/sic_coverage",
    "metric_identities": "audit/metric_identities",
    "overall_summary": "core/overall",
    "yield_summary": "core/yield",
    "volatility_summary": "core/volatility",
    "liquidity_summary": "core/liquidity",
    "sic_description_summary": "core/sic_description",
    "event_time_daily": "core/event_time_daily",
    "event_time_overnight": "core/event_time_overnight",
    "run_metadata": "manifest/run_metadata",
}

CSV_OUTPUTS = {
    "input_summary": "input_summary.csv",
    "sample_funnel": "sample_funnel.csv",
    "overall_summary": "overall_summary.csv",
    "yield_summary": "yield_summary.csv",
    "volatility_summary": "volatility_summary.csv",
    "liquidity_summary": "liquidity_summary.csv",
    "sic_description_summary": "sic_description_summary.csv",
    "event_time_daily": "event_time_daily.csv",
    "event_time_overnight": "event_time_overnight.csv",
}

FIGURE_OUTPUTS = {
    "yield": "yield_capture_ret_abn.png",
    "volatility": "volatility_capture_ret_abn.png",
    "liquidity": "liquidity_capture_ret_abn.png",
    "sic_description": "sic_description_capture_ret_abn.png",
    "event_time_daily": "event_time_daily_abnormal_return.png",
}

SIC_TEMPORAL_LIMITATION = (
    "sic_description is current-reference metadata and is not guaranteed "
    "point-in-time classification."
)
CORPORATE_ACTION_LIMITATION = (
    "Corporate-action contamination in upstream unadjusted prices was ignored "
    "by the M2 contract and was not used as a filter or blocker."
)
TIME_DECAY_DEPENDENCE_LIMITATION = (
    "Event-time observations can cluster on the same bar_date and are not "
    "independent; inspect calendar-date concentration, especially at offset -3, "
    "before interpreting the curve or performing significance tests."
)


class ReportArtifactError(RuntimeError):
    """Report-stage prerequisite or output validation failure."""


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create compact local CSV/JSON/figure artifacts for an M2 run."
    )
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--run-id", required=True, help="Accepted M2 run ID")
    parser.add_argument(
        "--output-dir", required=True, help="Local directory for report artifacts"
    )
    return parser.parse_args(argv)


def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError as exc:
        raise ReportArtifactError(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ReportArtifactError(f"Invalid JSON config {path}: {exc}") from exc
    missing = sorted({"team", "output_root", "report_top_sic_n"} - set(config))
    if missing:
        raise ReportArtifactError(f"Config is missing report keys: {missing}")
    if int(config["report_top_sic_n"]) < 1:
        raise ReportArtifactError("report_top_sic_n must be positive")
    return config


def resolve_runtime_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    resolved = dict(config)
    configured_team = str(config["team"]).rstrip("/")
    runtime_team = os.environ.get("TEAM", configured_team).rstrip("/")
    output_root = str(config["output_root"])
    output_root = output_root.replace("${TEAM}", runtime_team).replace(
        "$TEAM", runtime_team
    )
    if runtime_team != configured_team and output_root.startswith(configured_team + "/"):
        output_root = runtime_team + output_root[len(configured_team) :]
    resolved["team"] = runtime_team
    resolved["output_root"] = output_root
    return resolved


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise ReportArtifactError(
            "run_id must start with an alphanumeric character and contain only "
            "letters, digits, '.', '_' or '-'"
        )


def import_runtime_dependencies() -> Tuple[Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise ReportArtifactError(
            "PySpark is unavailable. Run this script with spark-submit on the "
            "Spark/YARN environment."
        ) from exc
    try:
        import pandas as pd
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise ReportArtifactError(
            "The report stage requires pandas and matplotlib. Verify with "
            "`python3 -c \"import pandas, matplotlib\"` and install them using "
            "the cluster-approved Python environment before retrying."
        ) from exc
    return SparkSession, pd, plt


def hadoop_path_exists(spark: Any, path: str) -> bool:
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(fs.exists(jvm_path))


def prepare_output_directory(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    expected = [output_dir / name for name in CSV_OUTPUTS.values()]
    expected.extend(
        [output_dir / "report_metrics.json", output_dir / "RESULTS_README.md"]
    )
    expected.extend(output_dir / "figures" / name for name in FIGURE_OUTPUTS.values())
    existing = [str(path) for path in expected if path.exists()]
    if existing:
        raise ReportArtifactError(
            "Refusing to overwrite existing report artifacts: " + ", ".join(existing)
        )
    (output_dir / "figures").mkdir(parents=True, exist_ok=True)


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
                f"Required aggregate exists but is not readable as Parquet: {path}: {exc}"
            ) from exc
    if missing:
        raise ReportArtifactError(
            "Required M2 aggregate paths are missing: " + ", ".join(missing)
        )
    return tables


def sorted_pandas_tables(tables: Mapping[str, Any]) -> Dict[str, Any]:
    frames = {name: frame.toPandas() for name, frame in tables.items()}
    sort_contract = {
        "input_summary": ["metric"],
        "sample_funnel": ["stage_order"],
        "bucket_boundaries": ["dimension", "bucket_number"],
        "sic_coverage": ["sic_source"],
        "metric_identities": ["identity"],
        "yield_summary": ["div_yield_bucket"],
        "volatility_summary": ["pre_vol_bucket"],
        "liquidity_summary": ["pre_avg_dollar_volume_bucket"],
        "sic_description_summary": ["n_events", "sic_description"],
        "event_time_daily": ["offset"],
    }
    for name, columns in sort_contract.items():
        if name not in frames or frames[name].empty:
            continue
        ascending = [False, True] if name == "sic_description_summary" else True
        frames[name] = frames[name].sort_values(columns, ascending=ascending)
    for name, frame in frames.items():
        if frame.empty:
            raise ReportArtifactError(f"Required aggregate {name!r} is empty")
    return frames


def write_csv_outputs(frames: Mapping[str, Any], output_dir: Path) -> None:
    for table_name, filename in CSV_OUTPUTS.items():
        frames[table_name].to_csv(output_dir / filename, index=False)


def bps(values: Any) -> Any:
    return values.astype(float) * 10000.0


def finish_figure(plt: Any, path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=180, bbox_inches="tight")
    plt.close()


def bucket_bar(
    plt: Any,
    frame: Any,
    label_column: str,
    title: str,
    path: Path,
) -> None:
    labels = frame[label_column].astype(str).tolist()
    values = bps(frame["mean_capture_ret_abn"])
    colors = ["#2f6b8a" if value >= 0 else "#b55252" for value in values]
    plt.figure(figsize=(7.5, 4.5))
    plt.bar(labels, values, color=colors)
    plt.axhline(0, color="#333333", linewidth=0.8)
    plt.xlabel("Quantile bucket (low to high)")
    plt.ylabel("Mean gross abnormal capture return (bps)")
    plt.title(title)
    finish_figure(plt, path)


def sic_bar(plt: Any, frame: Any, top_n: int, path: Path) -> None:
    top = frame.sort_values(
        ["n_events", "sic_description"], ascending=[False, True]
    ).head(top_n)
    top = top.sort_values("mean_capture_ret_abn", ascending=True)
    labels = [
        value if len(value) <= 55 else value[:52] + "..."
        for value in top["sic_description"].astype(str)
    ]
    values = bps(top["mean_capture_ret_abn"])
    colors = ["#2f6b8a" if value >= 0 else "#b55252" for value in values]
    height = max(5.0, 0.35 * len(top) + 1.5)
    plt.figure(figsize=(10, height))
    plt.barh(labels, values, color=colors)
    plt.axvline(0, color="#333333", linewidth=0.8)
    plt.xlabel("Mean gross abnormal capture return (bps)")
    plt.ylabel("sic_description")
    plt.title(f"Top {len(top)} SIC descriptions by event count")
    finish_figure(plt, path)


def event_time_figure(plt: Any, frame: Any, path: Path) -> None:
    ordered = frame.sort_values("offset")
    plt.figure(figsize=(8.5, 4.8))
    plt.plot(
        ordered["offset"],
        bps(ordered["mean_abn_ret_cc"]),
        marker="o",
        label="Mean",
        color="#2f6b8a",
    )
    plt.plot(
        ordered["offset"],
        bps(ordered["median_abn_ret_cc"]),
        marker="s",
        label="Median",
        color="#d28b31",
    )
    plt.axhline(0, color="#333333", linewidth=0.8)
    plt.axvline(0, color="#777777", linestyle="--", linewidth=0.9)
    plt.xticks(ordered["offset"].astype(int).tolist())
    plt.xlabel("Trading-day offset from ex-date")
    plt.ylabel("Abnormal close-to-close return (bps)")
    plt.title("Daily event-time / time-decay curve")
    plt.legend()
    finish_figure(plt, path)


def write_figures(
    plt: Any,
    frames: Mapping[str, Any],
    output_dir: Path,
    top_sic_n: int,
) -> None:
    figure_dir = output_dir / "figures"
    bucket_bar(
        plt,
        frames["yield_summary"],
        "div_yield_bucket",
        "Dividend yield and gross abnormal capture return",
        figure_dir / FIGURE_OUTPUTS["yield"],
    )
    bucket_bar(
        plt,
        frames["volatility_summary"],
        "pre_vol_bucket",
        "Pre-event volatility and gross abnormal capture return",
        figure_dir / FIGURE_OUTPUTS["volatility"],
    )
    bucket_bar(
        plt,
        frames["liquidity_summary"],
        "pre_avg_dollar_volume_bucket",
        "Pre-event dollar-volume liquidity / size proxy",
        figure_dir / FIGURE_OUTPUTS["liquidity"],
    )
    sic_bar(
        plt,
        frames["sic_description_summary"],
        top_sic_n,
        figure_dir / FIGURE_OUTPUTS["sic_description"],
    )
    event_time_figure(
        plt,
        frames["event_time_daily"],
        figure_dir / FIGURE_OUTPUTS["event_time_daily"],
    )


def json_safe(value: Any, pd: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): json_safe(item, pd) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item, pd) for item in value]
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def records(frame: Any, pd: Any) -> Any:
    return json_safe(frame.to_dict(orient="records"), pd)


def input_metric_map(frame: Any) -> Dict[str, Any]:
    return {
        str(row["metric"]): row["value"]
        for _, row in frame.iterrows()
    }


def write_report_metrics(
    pd: Any,
    frames: Mapping[str, Any],
    run_id: str,
    output_dir: Path,
) -> None:
    metrics = {
        "run_id": run_id,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input_summary": input_metric_map(frames["input_summary"]),
        "overall_summary": records(frames["overall_summary"], pd)[0],
        "sample_funnel": records(frames["sample_funnel"], pd),
        "sic_coverage": records(frames["sic_coverage"], pd)[0],
        "bucket_boundaries": records(frames["bucket_boundaries"], pd),
        "metric_identities": records(frames["metric_identities"], pd),
        "event_time_daily": records(frames["event_time_daily"], pd),
        "limitations": [
            SIC_TEMPORAL_LIMITATION,
            CORPORATE_ACTION_LIMITATION,
            TIME_DECAY_DEPENDENCE_LIMITATION,
        ],
        "figure_files": [
            f"figures/{filename}" for filename in FIGURE_OUTPUTS.values()
        ],
    }
    with open(output_dir / "report_metrics.json", "w", encoding="utf-8") as handle:
        json.dump(json_safe(metrics, pd), handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_results_readme(
    frames: Mapping[str, Any], run_id: str, run_root: str, output_dir: Path
) -> None:
    overall = frames["overall_summary"].iloc[0]
    sic = frames["sic_coverage"].iloc[0]
    inputs = input_metric_map(frames["input_summary"])
    mean_abnormal_bps = float(overall["mean_capture_ret_abn"]) * 10000.0
    lines = [
        f"# M2 Cross-Sectional Results - {run_id}",
        "",
        "These artifacts were generated from the compact, run-versioned M2 "
        "aggregate tables. They do not rescan M1 price or event inputs.",
        "",
        "## Run snapshot",
        "",
        f"- HDFS run root: `{run_root}`",
        f"- Input ex-date range: {inputs.get('min_ex_date')} to "
        f"{inputs.get('max_ex_date')}",
        f"- Base events: {int(overall['n_events']):,}",
        f"- Base tickers: {int(overall['n_tickers']):,}",
        f"- Benchmark excluded: {inputs.get('market_ticker_excluded')} "
        f"({inputs.get('market_ticker_event_count')} input events)",
        f"- Mean gross abnormal capture return: {mean_abnormal_bps:.2f} bps",
        f"- SIC source: {sic['sic_source']}",
        f"- SIC UNKNOWN share: {float(sic['unknown_event_share']):.2%}",
        "",
        "The gross abnormal capture return is not a net-profitability result: "
        "transaction costs and taxes are outside this build.",
        "",
        "## Tables",
        "",
        *[f"- `{filename}`" for filename in CSV_OUTPUTS.values()],
        "- `report_metrics.json`",
        "",
        "## Figures",
        "",
        *[f"- `figures/{filename}`" for filename in FIGURE_OUTPUTS.values()],
        "",
        "The SIC figure is limited to the largest categories for readability; "
        "`sic_description_summary.csv` retains every category, including UNKNOWN "
        "and low-N groups.",
        "",
        "## Interpretation notes",
        "",
        "- Bucket results are one-way associations in this sample.",
        "- Liquidity is `pre_avg_dollar_volume`, labeled as a pre-event "
        "dollar-volume liquidity / size proxy; it is not historical market cap.",
        "- Event-time offset 0 is the ex-date close-to-close return. The strategy "
        "exit at the ex-date open is summarized separately in "
        "`event_time_overnight.csv`.",
        f"- {TIME_DECAY_DEPENDENCE_LIMITATION}",
        f"- {SIC_TEMPORAL_LIMITATION}",
        f"- {CORPORATE_ACTION_LIMITATION}",
        "- Use 'associated with', 'differs across', or 'higher/lower in this "
        "sample'; do not infer causality or net tradeability from these outputs.",
        "",
    ]
    with open(output_dir / "RESULTS_README.md", "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main(argv: Optional[Sequence[str]] = None) -> int:
    spark = None
    try:
        args = parse_args(argv)
        validate_run_id(args.run_id)
        config = resolve_runtime_config(load_config(args.config))
        output_dir = Path(args.output_dir)
        prepare_output_directory(output_dir)
        SparkSession, pd, plt = import_runtime_dependencies()
        spark = (
            SparkSession.builder.appName(f"divcap-m2-report-{args.run_id}")
            .getOrCreate()
        )
        run_root = f"{str(config['output_root']).rstrip('/')}/{args.run_id}"
        tables = load_tables(spark, run_root)
        frames = sorted_pandas_tables(tables)
        write_csv_outputs(frames, output_dir)
        write_figures(
            plt,
            frames,
            output_dir,
            int(config["report_top_sic_n"]),
        )
        write_report_metrics(pd, frames, args.run_id, output_dir)
        write_results_readme(frames, args.run_id, run_root, output_dir)
        print(f"Report artifacts written to {output_dir}")
        return 0
    except ReportArtifactError as exc:
        print(f"REPORT ARTIFACT ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
