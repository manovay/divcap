"""Runner configuration, constants, and optional Spark runtime bindings."""

from __future__ import annotations



from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from m2_contract import (
    CORPORATE_ACTION_LIMITATION,
    DEPENDENCE_LIMITATION,
    GROSS_COST_LIMITATION,
    GROUP_SUMMARY_COLUMNS,
    INSIGHT_GENERATION_VERSION,
    NUMERIC_CHART_CONTRACT_VERSION,
    OUTPUT_RELATIVE_PATHS,
    PSEUDO_MODEL_LIMITATION,
    PSEUDO_PROVENANCE_LIMITATION,
    REQUIRED_CONFIG_KEYS,
    SIC_TEMPORAL_LIMITATION,
    TAXONOMY_SEPARATION_STATEMENT,
    UPSTREAM_MODEL_METRICS,
)

try:  # Keep pure helpers importable for local tests without PySpark installed.
    from pyspark.sql import DataFrame, SparkSession, functions as F
except ModuleNotFoundError:  # pragma: no cover - exercised on the cluster
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]


GRAIN_REQUIRED_COLUMNS = (
    "ticker",
    "ex_date",
    "cash_amount",
    "prev_close",
    "ex_open",
    "div_yield",
    "drop_ratio",
    "drop_pct",
    "capture_ret",
    "mkt_overnight_ret",
    "capture_ret_abn",
    "pre_avg_ret",
    "pre_avg_abn_ret",
    "pre_vol",
    "pre_avg_dollar_volume",
    "n_bars",
    "has_core",
    "window_contiguous",
)

PANEL_REQUIRED_COLUMNS = (
    "ticker",
    "ex_date",
    "bar_date",
    "offset",
    "abn_ret_cc",
)

METADATA_REQUIRED_COLUMNS = (
    "ticker",
    "active",
    "list_date",
    "sic_description",
)

MODEL_FEATURE_COLUMNS = (
    "event_id",
    "ticker",
    "ex_date",
    "div_yield",
    "pre_avg_ret",
    "pre_avg_abn_ret",
    "pre_vol",
    "pre_avg_dollar_volume",
    "frequency",
    "declaration_lead_days",
    "event_month",
)

MODEL_OUTCOME_COLUMNS = (
    "event_id",
    "ticker",
    "ex_date",
    "capture_ret",
    "capture_ret_abn",
    "drop_ratio",
    "drop_pct",
    "hold_ret",
    "has_core",
    "window_contiguous",
)

FORBIDDEN_MODEL_FEATURE_COLUMNS = frozenset(
    {
        "ex_open",
        "ex_close",
        "post_close",
        "drop_ratio",
        "drop_pct",
        "capture_ret",
        "capture_ret_abn",
        "hold_ret",
        "post_avg_ret",
        "post_avg_abn_ret",
        "div_yield_bucket",
        "pre_vol_bucket",
        "pre_avg_dollar_volume_bucket",
        "sic_description",
        "sic_code",
        "pseudo_sector",
        "label_level",
        "sector_state",
        "sector_source",
        "direct_sic_known",
        "direct_sic_unknown",
        "pseudo_recovered",
        "still_unresolved",
    }
)

OPTIONAL_COLUMN_TYPES = {
    "frequency": "double",
    "declaration_date": "date",
    "declaration_lead_days": "integer",
    "event_month": "integer",
    "hold_ret": "double",
}

RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
TIME_DECAY_DEPENDENCE_LIMITATION = (
    "Event-time observations can cluster on the same bar_date and are not "
    "independent; inspect calendar-date concentration, especially at offset -3, "
    "before interpretation or significance testing."
)


class M2ValidationError(RuntimeError):
    """A runtime prerequisite failed with an operator-actionable message."""


def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except FileNotFoundError as exc:
        raise M2ValidationError(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise M2ValidationError(f"Invalid JSON config {path}: {exc}") from exc

    missing = sorted(set(REQUIRED_CONFIG_KEYS) - set(config))
    if missing:
        raise M2ValidationError(
            f"Config is missing required keys: {', '.join(missing)}"
        )

    for key in (
        "team",
        "grain_path",
        "panel_path",
        "metadata_path",
        "pseudo_sector_path",
        "output_root",
    ):
        if not isinstance(config[key], str) or not config[key].strip():
            raise M2ValidationError(f"Config key {key!r} must be a non-empty string")

    if config["sic_description_column"] != "sic_description":
        raise M2ValidationError(
            "sic_description_column must be exactly 'sic_description'; alternate "
            "sector taxonomies are outside this build"
        )
    if config["pseudo_sector_column"] != "pseudo_sector":
        raise M2ValidationError(
            "pseudo_sector_column must be exactly 'pseudo_sector' so predicted "
            "labels cannot be confused with direct SIC"
        )
    if not isinstance(config["pseudo_sector_label_level"], str) or not config[
        "pseudo_sector_label_level"
    ].strip():
        raise M2ValidationError(
            "pseudo_sector_label_level must be a non-empty string"
        )
    if not isinstance(config["market_ticker"], str) or not config[
        "market_ticker"
    ].strip():
        raise M2ValidationError("market_ticker must be a non-empty string")
    integer_rules = {
        "min_history_days": (1, None),
        "bucket_count": (2, 20),
        "min_cell_n": (1, None),
        "min_pseudo_cell_n": (1, None),
        "min_report_tickers": (1, None),
        "event_offset_min": (-4, -4),
        "event_offset_max": (3, 3),
        "report_top_sic_n": (1, None),
        "report_top_pseudo_n": (1, None),
    }
    for key, (minimum, maximum) in integer_rules.items():
        value = config[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise M2ValidationError(f"{key} must be a JSON integer")
        if value < minimum or (maximum is not None and value > maximum):
            if minimum == maximum:
                raise M2ValidationError(f"{key} must be exactly {minimum}")
            if maximum is None:
                raise M2ValidationError(f"{key} must be at least {minimum}")
            raise M2ValidationError(
                f"{key} must be between {minimum} and {maximum}"
            )
    if config["event_offset_min"] != -4 or config["event_offset_max"] != 3:
        raise M2ValidationError(
            "The proposal contract requires event_offset_min=-4 and "
            "event_offset_max=3"
        )
    tolerance = config["metric_tolerance"]
    if (
        isinstance(tolerance, bool)
        or not isinstance(tolerance, (int, float))
        or not math.isfinite(float(tolerance))
        or float(tolerance) <= 0
    ):
        raise M2ValidationError("metric_tolerance must be a positive finite number")
    if not isinstance(config["report_numeric_labels"], bool):
        raise M2ValidationError("report_numeric_labels must be a JSON boolean")
    if isinstance(config["insight_min_abs_bps"], bool):
        raise M2ValidationError("insight_min_abs_bps must be a non-negative number")
    try:
        insight_threshold = float(config["insight_min_abs_bps"])
    except (TypeError, ValueError) as exc:
        raise M2ValidationError(
            "insight_min_abs_bps must be a non-negative number"
        ) from exc
    if not math.isfinite(insight_threshold) or insight_threshold < 0:
        raise M2ValidationError("insight_min_abs_bps must be non-negative")

    metric_expectations = {
        "primary_metric": "capture_ret_abn",
        "secondary_metric": "capture_ret",
        "academic_metric": "drop_ratio",
    }
    for key, expected in metric_expectations.items():
        if config[key] != expected:
            raise M2ValidationError(
                f"{key} must be {expected!r} for the M2 metric contract"
            )
    return config


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise M2ValidationError(
            "run_id must start with an alphanumeric character and contain only "
            "letters, digits, '.', '_' or '-'; path separators are forbidden"
        )


def resolve_runtime_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    """Apply an optional TEAM environment override to repository-default paths."""
    resolved = dict(config)
    configured_team = str(config["team"]).rstrip("/")
    runtime_team = os.environ.get("TEAM", configured_team).rstrip("/")
    resolved["team"] = runtime_team
    for key in (
        "grain_path",
        "panel_path",
        "metadata_path",
        "pseudo_sector_path",
        "output_root",
    ):
        if key not in config:
            continue
        value = str(config[key])
        value = value.replace("${TEAM}", runtime_team).replace("$TEAM", runtime_team)
        if runtime_team != configured_team and value.startswith(configured_team + "/"):
            value = runtime_team + value[len(configured_team) :]
        resolved[key] = value
    return resolved
