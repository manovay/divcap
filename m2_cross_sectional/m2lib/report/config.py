"""Report configuration, constants, output safety, and runtime dependencies."""

from __future__ import annotations



from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from m2_contract import (
    ALLOWED_INSIGHT_STATUSES,
    CORPORATE_ACTION_LIMITATION,
    CSV_OUTPUTS,
    DEPENDENCE_LIMITATION,
    FIGURE_OUTPUTS,
    GROSS_COST_LIMITATION,
    GROUP_SUMMARY_COLUMNS,
    INSIGHT_GENERATION_VERSION,
    PSEUDO_MODEL_LIMITATION,
    PSEUDO_PROVENANCE_LIMITATION,
    REPORT_SECTIONS,
    REPORT_TABLE_PATHS,
    REQUIRED_CONFIG_KEYS,
    SIC_TEMPORAL_LIMITATION,
    TAXONOMY_SEPARATION_STATEMENT,
    UPSTREAM_MODEL_METRICS,
)


RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
TABLE_PATHS = REPORT_TABLE_PATHS

DIRECT_COLOR = "#2F6B8A"
PSEUDO_COLOR = "#7A5195"
UNRESOLVED_COLOR = "#8C8C8C"
POSITIVE_COLOR = "#2A7F62"
NEGATIVE_COLOR = "#B55252"
NEUTRAL_COLOR = "#D28B31"


class ReportArtifactError(RuntimeError):
    """Report-stage prerequisite or immutable-output validation failure."""


def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError as exc:
        raise ReportArtifactError(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ReportArtifactError(f"Invalid JSON config {path}: {exc}") from exc
    missing = sorted(set(REQUIRED_CONFIG_KEYS) - set(config))
    if missing:
        raise ReportArtifactError(
            "Config is missing required V2 keys: " + ", ".join(missing)
        )
    positive_integer_keys = (
        "report_top_sic_n",
        "report_top_pseudo_n",
        "min_cell_n",
        "min_pseudo_cell_n",
        "min_report_tickers",
    )
    for key in positive_integer_keys:
        if (
            isinstance(config[key], bool)
            or not isinstance(config[key], int)
            or config[key] < 1
        ):
            raise ReportArtifactError(f"{key} must be a positive integer")
    if not isinstance(config["report_numeric_labels"], bool):
        raise ReportArtifactError("report_numeric_labels must be a JSON boolean")
    try:
        threshold = float(config["insight_min_abs_bps"])
    except (TypeError, ValueError) as exc:
        raise ReportArtifactError(
            "insight_min_abs_bps must be a non-negative number"
        ) from exc
    if not math.isfinite(threshold) or threshold < 0:
        raise ReportArtifactError("insight_min_abs_bps must be non-negative")
    if config["pseudo_sector_column"] != "pseudo_sector":
        raise ReportArtifactError("pseudo_sector_column must be 'pseudo_sector'")
    return config


def resolve_runtime_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    resolved = dict(config)
    configured_team = str(config["team"]).rstrip("/")
    runtime_team = os.environ.get("TEAM", configured_team).rstrip("/")
    resolved["team"] = runtime_team
    for key in ("output_root", "pseudo_sector_path"):
        value = str(config[key])
        value = value.replace("${TEAM}", runtime_team).replace("$TEAM", runtime_team)
        if runtime_team != configured_team and value.startswith(configured_team + "/"):
            value = runtime_team + value[len(configured_team) :]
        resolved[key] = value
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
            "configured Spark/YARN environment."
        ) from exc
    try:
        import pandas as pd
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise ReportArtifactError(
            "The report stage requires pandas and matplotlib. Verify with "
            "`python3 -c \"import pandas, matplotlib\"` in the cluster-approved "
            "environment before retrying."
        ) from exc
    return SparkSession, pd, plt


def hadoop_path_exists(spark: Any, path: str) -> bool:
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(fs.exists(jvm_path))


def required_local_artifacts() -> Tuple[str, ...]:
    artifacts = list(CSV_OUTPUTS.values())
    artifacts.extend(("report_metrics.json", "section_insights.json"))
    artifacts.extend(("INSIGHTS_SUMMARY.md", "RESULTS_README.md"))
    artifacts.extend(f"figures/{name}" for name in FIGURE_OUTPUTS.values())
    return tuple(artifacts)


def prepare_output_directory(output_dir: Path) -> None:
    if output_dir.exists():
        existing = sorted(str(path) for path in output_dir.iterdir())
        if existing:
            raise ReportArtifactError(
                "Refusing to reuse a nonempty report directory; choose a new "
                f"output path. Existing entries: {existing}"
            )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "figures").mkdir(parents=True, exist_ok=True)
