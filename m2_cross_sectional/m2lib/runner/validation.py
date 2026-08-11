"""Pure validation and reference-statistics helpers for the runner."""

from __future__ import annotations

from .config import *  # noqa: F401,F403

def validate_model_feature_columns(columns: Iterable[str]) -> List[str]:
    """Return any outcome/leakage columns found in a feature table schema."""
    found = set(columns) & FORBIDDEN_MODEL_FEATURE_COLUMNS
    for name in columns:
        lowered = name.lower()
        if "sector" in lowered and any(
            token in lowered for token in ("combined", "effective", "blended", "final")
        ):
            found.add(name)
    return sorted(found)


def normalize_ticker_value(value: Any) -> Optional[str]:
    """Trim join keys while preserving case-sensitive vendor identifiers."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_label_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def validate_pseudo_schema(columns: Iterable[str], path: str) -> None:
    found = sorted(set(columns))
    missing = sorted({"ticker", "pseudo_sector", "label_level"} - set(found))
    if missing:
        raise M2ValidationError(
            "Pseudo-sector schema validation failed at "
            f"{path}: missing columns {missing}; found columns {found}. "
            "Inspect with `spark.read.parquet(path).printSchema()` and rebuild "
            "the upstream pseudo-sector asset before retrying."
        )


def validate_pseudo_records(
    records: Sequence[Mapping[str, Any]], configured_level: str, path: str
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Pure counterpart of the Spark pseudo-source contract for local tests."""
    columns = set().union(*(record.keys() for record in records)) if records else set()
    validate_pseudo_schema(columns, path)
    level = normalize_label_value(configured_level)
    if level is None:
        raise M2ValidationError("configured pseudo-sector label level is blank")

    normalized = []
    for record in records:
        normalized.append(
            {
                "ticker": normalize_ticker_value(record.get("ticker")),
                "pseudo_sector": normalize_label_value(record.get("pseudo_sector")),
                "label_level": normalize_label_value(record.get("label_level")),
                "sec_type": normalize_label_value(record.get("sec_type")),
            }
        )
    observed_levels = sorted(
        {row["label_level"] for row in normalized if row["label_level"] is not None}
    )
    if level not in observed_levels:
        raise M2ValidationError(
            f"Configured pseudo-sector label level {level!r} is absent at {path}; "
            f"observed levels={observed_levels}. Re-run the upstream writer for "
            "the configured level; no fallback is allowed."
        )

    labels_by_ticker: Dict[str, set] = {}
    levels_by_ticker: Dict[str, set] = {}
    for row in normalized:
        ticker = row["ticker"]
        if ticker is None:
            continue
        if row["pseudo_sector"] is not None:
            labels_by_ticker.setdefault(ticker, set()).add(row["pseudo_sector"])
        if row["label_level"] is not None:
            levels_by_ticker.setdefault(ticker, set()).add(row["label_level"])
    label_conflicts = sorted(
        ticker for ticker, values in labels_by_ticker.items() if len(values) > 1
    )
    level_conflicts = sorted(
        ticker for ticker, values in levels_by_ticker.items() if len(values) > 1
    )
    if label_conflicts or level_conflicts:
        raise M2ValidationError(
            "Pseudo-sector source conflicts are blocking at "
            f"{path}: conflicting labels for {label_conflicts}; conflicting "
            f"label levels for {level_conflicts}. Inspect those tickers and "
            "rebuild a one-label/one-level source before retrying."
        )

    lookup: Dict[str, Dict[str, Any]] = {}
    for row in normalized:
        if (
            row["ticker"] is None
            or row["pseudo_sector"] is None
            or row["label_level"] != level
        ):
            continue
        lookup[row["ticker"]] = dict(row)
    distinct_normalized_rows = {
        (row["ticker"], row["pseudo_sector"], row["label_level"], row["sec_type"])
        for row in normalized
    }
    audit = {
        "source_rows": len(records),
        "source_tickers": len({row["ticker"] for row in normalized if row["ticker"]}),
        "source_labels": len(
            {row["pseudo_sector"] for row in normalized if row["pseudo_sector"]}
        ),
        "observed_label_levels": observed_levels,
        "blank_ticker_count": sum(row["ticker"] is None for row in normalized),
        "blank_label_count": sum(row["pseudo_sector"] is None for row in normalized),
        "blank_label_level_count": sum(
            row["label_level"] is None for row in normalized
        ),
        "duplicate_identical_row_count": len(records) - len(distinct_normalized_rows),
        "conflicting_ticker_count": len(label_conflicts),
        "conflicting_level_ticker_count": len(level_conflicts),
        "configured_level_tickers": len(lookup),
    }
    return lookup, audit


def classify_sector_state(
    sic_description: Any,
    pseudo_sector: Any,
    label_level: Any,
    configured_level: str,
) -> str:
    direct = normalize_label_value(sic_description) or "UNKNOWN"
    if direct != "UNKNOWN":
        return "direct_sic_known"
    pseudo = normalize_label_value(pseudo_sector)
    level = normalize_label_value(label_level)
    if pseudo is not None and level == normalize_label_value(configured_level):
        return "pseudo_recovered"
    return "still_unresolved"


def reconcile_sector_counts(
    *,
    base_events: int,
    direct_known_events: int,
    direct_unknown_events: int,
    pseudo_recovered_events: int,
    still_unresolved_events: int,
) -> Dict[str, int]:
    residuals = {
        "base_minus_direct": base_events
        - direct_known_events
        - direct_unknown_events,
        "unknown_minus_recovery": direct_unknown_events
        - pseudo_recovered_events
        - still_unresolved_events,
        "base_minus_three_states": base_events
        - direct_known_events
        - pseudo_recovered_events
        - still_unresolved_events,
    }
    if any(residuals.values()):
        raise M2ValidationError(
            "Sector coverage reconciliation failed: "
            + ", ".join(f"{key}={value}" for key, value in residuals.items())
        )
    return residuals


def _rank_with_ties(values: Sequence[float]) -> List[float]:
    ordered = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(ordered):
        end = cursor + 1
        while end < len(ordered) and ordered[end][1] == ordered[cursor][1]:
            end += 1
        rank = (cursor + 1 + end) / 2.0
        for index in range(cursor, end):
            ranks[ordered[index][0]] = rank
        cursor = end
    return ranks


def _pearson(left: Sequence[float], right: Sequence[float]) -> Optional[float]:
    if len(left) < 2 or len(left) != len(right):
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum(
        (x - left_mean) * (y - right_mean) for x, y in zip(left, right)
    )
    left_ss = sum((x - left_mean) ** 2 for x in left)
    right_ss = sum((y - right_mean) ** 2 for y in right)
    if left_ss == 0 or right_ss == 0:
        return 0.0
    return numerator / math.sqrt(left_ss * right_ss)


def ordered_diagnostic_from_records(
    dimension: str, records: Sequence[Mapping[str, Any]], label_column: str
) -> Dict[str, Any]:
    ordered = sorted(records, key=lambda row: str(row[label_column]))
    means = [float(row["mean_capture_ret_abn"]) for row in ordered]
    medians = [float(row["median_capture_ret_abn"]) for row in ordered]
    positive = [float(row["positive_capture_ret_abn_rate"]) for row in ordered]
    monotonic_steps = sum(
        1 for previous, current in zip(means, means[1:]) if current >= previous
    )
    return {
        "dimension": dimension,
        "actual_bucket_count": len(ordered),
        "low_bucket_mean_bps": means[0] * 10000.0,
        "high_bucket_mean_bps": means[-1] * 10000.0,
        "high_minus_low_bps": (means[-1] - means[0]) * 10000.0,
        "mean_range_bps": (max(means) - min(means)) * 10000.0,
        "median_range_bps": (max(medians) - min(medians)) * 10000.0,
        "positive_rate_range_pp": (max(positive) - min(positive)) * 100.0,
        "monotonic_step_count": monotonic_steps,
        "spearman_bucket_mean": _pearson(
            list(range(1, len(means) + 1)), _rank_with_ties(means)
        ),
    }


def _quantile(values: Sequence[float], probability: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize_numeric_records(
    records: Sequence[Mapping[str, Any]],
    *,
    analysis_events: Optional[int] = None,
    analysis_tickers: Optional[int] = None,
    min_cell_n: int = 1,
    min_report_tickers: int = 1,
) -> Dict[str, Any]:
    """Pure reference math for summary-contract unit tests."""
    tickers = {str(row["ticker"]) for row in records}
    abnormal = [
        float(row["capture_ret_abn"])
        for row in records
        if row.get("capture_ret_abn") is not None
    ]
    capture = [
        float(row["capture_ret"])
        for row in records
        if row.get("capture_ret") is not None
    ]
    drop = [
        float(row["drop_ratio"])
        for row in records
        if row.get("drop_ratio") is not None
    ]
    n_events = len(records)
    n_tickers = len(tickers)
    mean_abnormal = sum(abnormal) / len(abnormal) if abnormal else None
    if len(abnormal) > 1:
        variance = sum((value - mean_abnormal) ** 2 for value in abnormal) / (
            len(abnormal) - 1
        )
        stddev = math.sqrt(variance)
        se = stddev / math.sqrt(len(abnormal))
        low = mean_abnormal - 1.96 * se
        high = mean_abnormal + 1.96 * se
    else:
        stddev = se = low = high = None
    total_events = n_events if analysis_events is None else analysis_events
    total_tickers = n_tickers if analysis_tickers is None else analysis_tickers
    low_n = n_events < min_cell_n
    low_tickers = n_tickers < min_report_tickers
    return {
        "n_events": n_events,
        "n_tickers": n_tickers,
        "n_capture_ret_abn": len(abnormal),
        "n_capture_ret": len(capture),
        "n_drop_ratio": len(drop),
        "event_share_of_analysis": n_events / total_events if total_events else None,
        "ticker_share_of_analysis": n_tickers / total_tickers if total_tickers else None,
        "mean_capture_ret_abn": mean_abnormal,
        "stddev_capture_ret_abn": stddev,
        "se_capture_ret_abn": se,
        "ci95_low_capture_ret_abn": low,
        "ci95_high_capture_ret_abn": high,
        "median_capture_ret_abn": _quantile(abnormal, 0.5),
        "p25_capture_ret_abn": _quantile(abnormal, 0.25),
        "p75_capture_ret_abn": _quantile(abnormal, 0.75),
        "positive_capture_ret_abn_rate": (
            sum(value > 0 for value in abnormal) / len(abnormal) if abnormal else None
        ),
        "mean_capture_ret": sum(capture) / len(capture) if capture else None,
        "median_capture_ret": _quantile(capture, 0.5),
        "mean_drop_ratio": sum(drop) / len(drop) if drop else None,
        "median_drop_ratio": _quantile(drop, 0.5),
        "p25_drop_ratio": _quantile(drop, 0.25),
        "p75_drop_ratio": _quantile(drop, 0.75),
        "drop_ratio_lt_1_rate": (
            sum(value < 1 for value in drop) / len(drop) if drop else None
        ),
        "low_n_flag": low_n,
        "low_ticker_flag": low_tickers,
        "report_eligible_flag": not low_n and not low_tickers,
    }


def metric_identity_residuals(row: Mapping[str, Optional[float]]) -> Dict[str, float]:
    """Pure-Python counterpart of Spark preflight identities for unit tests."""
    required = (
        "drop_pct",
        "drop_ratio",
        "div_yield",
        "capture_ret",
        "capture_ret_abn",
        "mkt_overnight_ret",
        "stock_overnight_ret",
        "abn_overnight_ret",
    )
    if any(row.get(name) is None for name in required):
        raise ValueError("metric_identity_residuals requires non-null metrics")
    values = {name: float(row[name]) for name in required}  # type: ignore[arg-type]
    return {
        "drop_pct_equals_drop_ratio_times_div_yield": abs(
            values["drop_pct"]
            - values["drop_ratio"] * values["div_yield"]
        ),
        "capture_ret_equals_div_yield_minus_drop_pct": abs(
            values["capture_ret"] - (values["div_yield"] - values["drop_pct"])
        ),
        "capture_ret_abn_equals_capture_ret_minus_market": abs(
            values["capture_ret_abn"]
            - (values["capture_ret"] - values["mkt_overnight_ret"])
        ),
        "stock_overnight_ret_equals_negative_drop_pct": abs(
            values["stock_overnight_ret"] + values["drop_pct"]
        ),
        "abn_overnight_ret_equals_stock_minus_market": abs(
            values["abn_overnight_ret"]
            - (values["stock_overnight_ret"] - values["mkt_overnight_ret"])
        ),
    }


def deduplicate_cutpoints(
    candidates: Iterable[float], maximum: float, tolerance: float = 1e-12
) -> List[float]:
    """Return ordered usable cutpoints, preserving a minimum-valued cut.

    A cut at the observed maximum cannot create a non-empty upper bucket and is
    removed.  Duplicate quantiles are collapsed instead of fabricating bins.
    """
    cuts: List[float] = []
    for raw in sorted(float(value) for value in candidates if math.isfinite(value)):
        if raw >= maximum or math.isclose(raw, maximum, abs_tol=tolerance):
            continue
        if cuts and math.isclose(raw, cuts[-1], abs_tol=tolerance):
            continue
        cuts.append(raw)
    return cuts
