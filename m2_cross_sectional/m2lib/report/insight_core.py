"""JSON-safe evidence records and reusable insight-section builders."""

from __future__ import annotations

from .figures import *  # noqa: F401,F403

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


def evidence(metric: str, value: Any, display: str, source: str) -> Dict[str, Any]:
    return {
        "metric": metric,
        "value": value,
        "display_value": display,
        "source_table": source,
    }


def section(
    section_id: str,
    title: str,
    status: str,
    headline: str,
    evidence_items: Sequence[Mapping[str, Any]],
    interpretation: str,
    caveats: Sequence[str],
    next_step: str,
) -> Dict[str, Any]:
    if status not in ALLOWED_INSIGHT_STATUSES:
        raise ReportArtifactError(f"Invalid insight status {status!r}")
    return {
        "section_id": section_id,
        "title": title,
        "status": status,
        "headline": headline,
        "evidence": [dict(item) for item in evidence_items],
        "business_interpretation": interpretation,
        "caveats": list(caveats),
        "recommended_next_step": next_step,
    }


def _ordered_section(
    frames: Mapping[str, Any],
    dimension: str,
    title: str,
    summary_table: str,
    label_column: str,
    insight_threshold_bps: float,
) -> Dict[str, Any]:
    diagnostic = row_for(frames["dimension_diagnostics"], "dimension", dimension)
    summary = frames[summary_table].sort_values(label_column)
    low = summary.iloc[0]
    high = summary.iloc[-1]
    spread = float(diagnostic["high_minus_low_bps"])
    steps = int(diagnostic["monotonic_step_count"])
    total_steps = max(0, int(diagnostic["actual_bucket_count"]) - 1)
    if abs(spread) < insight_threshold_bps:
        status = "flat"
        direction = "small relative to the configured materiality threshold"
    elif steps not in (0, total_steps):
        status = "mixed"
        direction = "non-monotonic across the observed buckets"
    else:
        status = "informative"
        direction = "monotonic in the observed bucket means"
    headline = (
        f"Low/high bucket mean abnormal capture was "
        f"{format_bps(low['mean_capture_ret_abn'])} / "
        f"{format_bps(high['mean_capture_ret_abn'])}, with medians "
        f"{format_bps(low['median_capture_ret_abn'])} / "
        f"{format_bps(high['median_capture_ret_abn'])}; the high-minus-low mean "
        f"spread was {spread:.2f} bps and was {direction}."
    )
    caveat = DEPENDENCE_LIMITATION
    if dimension == "liquidity":
        caveat = (
            "This dimension is pre-event dollar-volume liquidity / size proxy, "
            "not historical market capitalization. " + DEPENDENCE_LIMITATION
        )
    return section(
        dimension,
        title,
        status,
        headline,
        [
            evidence(
                "low_bucket_mean_capture_ret_abn",
                float(low["mean_capture_ret_abn"]),
                format_bps(low["mean_capture_ret_abn"]),
                CSV_OUTPUTS[summary_table],
            ),
            evidence(
                "high_bucket_mean_capture_ret_abn",
                float(high["mean_capture_ret_abn"]),
                format_bps(high["mean_capture_ret_abn"]),
                CSV_OUTPUTS[summary_table],
            ),
            evidence(
                "low_bucket_median_capture_ret_abn",
                float(low["median_capture_ret_abn"]),
                format_bps(low["median_capture_ret_abn"]),
                CSV_OUTPUTS[summary_table],
            ),
            evidence(
                "high_bucket_median_capture_ret_abn",
                float(high["median_capture_ret_abn"]),
                format_bps(high["median_capture_ret_abn"]),
                CSV_OUTPUTS[summary_table],
            ),
            evidence(
                "high_minus_low_bps",
                spread,
                f"{spread:.2f} bps",
                CSV_OUTPUTS["dimension_diagnostics"],
            ),
            evidence(
                "mean_range_bps",
                float(diagnostic["mean_range_bps"]),
                f"{float(diagnostic['mean_range_bps']):.2f} bps",
                CSV_OUTPUTS["dimension_diagnostics"],
            ),
            evidence(
                "positive_rate_range_pp",
                float(diagnostic["positive_rate_range_pp"]),
                f"{float(diagnostic['positive_rate_range_pp']):.2f} percentage points",
                CSV_OUTPUTS["dimension_diagnostics"],
            ),
            evidence(
                "monotonic_step_count",
                steps,
                f"{steps}/{total_steps} nondecreasing steps",
                CSV_OUTPUTS["dimension_diagnostics"],
            ),
            evidence(
                "actual_bucket_count",
                int(diagnostic["actual_bucket_count"]),
                format_count(diagnostic["actual_bucket_count"]),
                CSV_OUTPUTS["dimension_diagnostics"],
            ),
        ],
        "The profile prioritizes where deeper stability and implementation-cost "
        "checks may be most informative; it is a descriptive association.",
        [caveat, GROSS_COST_LIMITATION],
        "Repeat the profile by year and security type, then evaluate measured "
        "implementation costs without changing the canonical all-sample table.",
    )
