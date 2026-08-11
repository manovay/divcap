"""Shared numeric formatting and the core report figures."""

from __future__ import annotations

from .tables import *  # noqa: F401,F403

def write_csv_outputs(frames: Mapping[str, Any], output_dir: Path) -> None:
    for table_name, filename in CSV_OUTPUTS.items():
        frames[table_name].to_csv(output_dir / filename, index=False)


def bps(value: Any) -> Any:
    return value * 10000.0


def percent(value: Any) -> Any:
    return value * 100.0


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(math.isnan(float(value)))
    except (TypeError, ValueError):
        return False


def format_bps(value: Any) -> str:
    return "not available" if is_missing(value) else f"{float(value) * 10000.0:.2f} bps"


def format_percent(value: Any, digits: int = 1) -> str:
    return "not available" if is_missing(value) else f"{float(value) * 100.0:.{digits}f}%"


def format_count(value: Any) -> str:
    return "not available" if is_missing(value) else f"{int(value):,}"


def format_ratio(value: Any) -> str:
    return "not available" if is_missing(value) else f"{float(value):.3f}"


def input_metric_map(frame: Any) -> Dict[str, Any]:
    return {str(row["metric"]): row["value"] for _, row in frame.iterrows()}


def row_for(frame: Any, column: str, value: Any) -> Any:
    matches = frame[frame[column] == value]
    if matches.empty:
        raise ReportArtifactError(
            f"Expected row {column}={value!r} was not found in report aggregate"
        )
    return matches.iloc[0]


def finish_figure(plt: Any, figure: Any, path: Path) -> None:
    figure.tight_layout()
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)
    if not path.exists() or path.stat().st_size == 0:
        raise ReportArtifactError(f"Figure was not written or is empty: {path}")


def annotate_empty(axis: Any, message: str) -> None:
    axis.axis("off")
    axis.text(0.5, 0.5, message, ha="center", va="center", fontsize=12)


def sample_funnel_figure(plt: Any, frame: Any, path: Path, numeric_labels: bool) -> None:
    sequential = frame[frame["stage_type"] == "sequential_filter"].copy()
    sequential = sequential.sort_values("stage_order")
    labels = sequential["stage"].astype(str).str.replace("_", " ").tolist()
    values = sequential["n_events"].astype(float).tolist()
    figure, axis = plt.subplots(figsize=(9.5, 5.3))
    bars = axis.barh(labels[::-1], values[::-1], color=DIRECT_COLOR)
    axis.set_xlabel("Event count")
    axis.set_title("F01 — Accepted base-sample funnel")
    axis.grid(axis="x", alpha=0.2)
    if numeric_labels:
        rows = list(sequential.iloc[::-1].iterrows())
        for bar, (_, row) in zip(bars, rows):
            text = (
                f"{int(row['n_events']):,} | prior {float(row['retention_from_prior']):.1%}"
                f" | raw {float(row['cumulative_retention']):.1%}"
            )
            axis.text(
                bar.get_width(), bar.get_y() + bar.get_height() / 2, "  " + text,
                va="center", fontsize=8.5,
            )
    axis.text(
        0.0, -0.16,
        "Base: cash_amount > 0, has_core=true, window_contiguous=true, and the "
        "configured market benchmark excluded.",
        transform=axis.transAxes, fontsize=8.5,
    )
    finish_figure(plt, figure, path)


def sector_coverage_figure(
    plt: Any, bridge: Any, coverage: Any, path: Path, numeric_labels: bool
) -> None:
    ordered = bridge.sort_values("state_order")
    display = {
        "direct_sic_known": "Direct SIC known",
        "pseudo_recovered": "Pseudo-sector recovered",
        "still_unresolved": "Still unresolved",
    }
    colors = [DIRECT_COLOR, PSEUDO_COLOR, UNRESOLVED_COLOR]
    figure, axis = plt.subplots(figsize=(10, 4.8))
    left = 0.0
    for (_, row), color in zip(ordered.iterrows(), colors):
        share = float(row["event_share_of_base"])
        axis.barh([0], [share * 100.0], left=left, color=color, height=0.45)
        if numeric_labels:
            axis.text(
                left + share * 50.0, 0,
                f"{display[str(row['sector_state'])]}\n{int(row['n_events']):,}\n{share:.1%}",
                ha="center", va="center", color="white" if share > 0.12 else "black",
                fontsize=8.5,
            )
        left += share * 100.0
    audit = coverage.iloc[0]
    unknown_share = (
        float(audit["direct_sic_unknown_events"]) / float(audit["base_events"])
    )
    note = (
        f"Direct-SIC unknown: {unknown_share:.1%} | recovery among unknown: "
        f"{float(audit['pseudo_recovery_rate_of_sic_unknown']):.1%} | coverage "
        f"after recovery: {float(audit['coverage_after_recovery_share']):.1%}"
    )
    axis.set_xlim(0, 100)
    axis.set_yticks([])
    axis.set_xlabel("Share of accepted base events (%)")
    axis.set_title("F02 — Sector coverage recovery (sources shown separately)")
    axis.text(0.5, -0.22, note, transform=axis.transAxes, ha="center", fontsize=9)
    axis.text(
        0.5, -0.32,
        "Coverage accounting only; this is not a blended sector-performance ranking.",
        transform=axis.transAxes, ha="center", fontsize=8.5,
    )
    finish_figure(plt, figure, path)


def overall_scorecard_figure(
    plt: Any, frame: Any, path: Path, numeric_labels: bool
) -> None:
    row = frame.iloc[0]
    metrics = [
        ("Mean abnormal capture", format_bps(row["mean_capture_ret_abn"])),
        ("Median abnormal capture", format_bps(row["median_capture_ret_abn"])),
        ("Mean raw capture", format_bps(row["mean_capture_ret"])),
        ("Median raw capture", format_bps(row["median_capture_ret"])),
        ("Positive abnormal rate", format_percent(row["positive_capture_ret_abn_rate"])),
        ("Median drop ratio", format_ratio(row["median_drop_ratio"])),
        ("Drop ratio < 1", format_percent(row["drop_ratio_lt_1_rate"])),
        ("Analysis population", f"{int(row['n_events']):,} events / {int(row['n_tickers']):,} tickers"),
    ]
    figure, axis = plt.subplots(figsize=(10.5, 5.4))
    axis.axis("off")
    axis.set_title("F03 — Overall dividend-capture scorecard", pad=18)
    for index, (label, value) in enumerate(metrics):
        col = index % 2
        row_index = index // 2
        x = 0.04 + col * 0.5
        y = 0.84 - row_index * 0.21
        axis.add_patch(
            plt.Rectangle((x, y - 0.12), 0.44, 0.16, color="#F2F4F5", transform=axis.transAxes)
        )
        axis.text(x + 0.02, y, label, transform=axis.transAxes, fontsize=9, va="top")
        axis.text(
            x + 0.02, y - 0.06, value if numeric_labels else "available in CSV",
            transform=axis.transAxes, fontsize=13, fontweight="bold", va="top",
        )
    axis.text(
        0.5, 0.01, GROSS_COST_LIMITATION,
        transform=axis.transAxes, ha="center", fontsize=8.5,
    )
    finish_figure(plt, figure, path)


def _boundary_note(boundaries: Any, dimension: str) -> str:
    subset = boundaries[boundaries["dimension"] == dimension].sort_values(
        "bucket_number"
    )
    parts = []
    for _, row in subset.iterrows():
        lower = "−∞" if is_missing(row["lower_bound_exclusive"]) else f"{float(row['lower_bound_exclusive']):.6g}"
        upper = "+∞" if is_missing(row["upper_bound_inclusive"]) else f"{float(row['upper_bound_inclusive']):.6g}"
        parts.append(f"{row['bucket_label']}: ({lower}, {upper}]")
    return "Boundaries — " + "; ".join(parts)


def ordered_profile_figure(
    plt: Any,
    frame: Any,
    boundaries: Any,
    diagnostics: Any,
    dimension: str,
    label_column: str,
    title: str,
    path: Path,
    numeric_labels: bool,
) -> None:
    ordered = frame.sort_values(label_column).reset_index(drop=True)
    diagnostic = row_for(diagnostics, "dimension", dimension)
    labels = ordered[label_column].astype(str).tolist()
    x = list(range(len(ordered)))
    means = [bps(float(value)) for value in ordered["mean_capture_ret_abn"]]
    medians = [bps(float(value)) for value in ordered["median_capture_ret_abn"]]
    lower_error = [
        max(0.0, mean - bps(float(low))) if not is_missing(low) else 0.0
        for mean, low in zip(means, ordered["ci95_low_capture_ret_abn"])
    ]
    upper_error = [
        max(0.0, bps(float(high)) - mean) if not is_missing(high) else 0.0
        for mean, high in zip(means, ordered["ci95_high_capture_ret_abn"])
    ]
    figure, (top, bottom) = plt.subplots(2, 1, figsize=(9.5, 8.0), sharex=True)
    top.errorbar(
        x, means, yerr=[lower_error, upper_error], fmt="o-", color=DIRECT_COLOR,
        capsize=4, label="Mean ± unadjusted 95% descriptive interval",
    )
    top.scatter(x, medians, marker="D", color=NEUTRAL_COLOR, label="Median", zorder=4)
    top.axhline(0, color="#333333", linewidth=0.8)
    top.set_ylabel("Abnormal gross capture return (bps)")
    top.set_title(title)
    top.legend(fontsize=8)
    if numeric_labels:
        for position, mean, (_, row) in zip(x, means, ordered.iterrows()):
            top.annotate(
                f"{mean:.2f} bps\nN={int(row['n_capture_ret_abn']):,}",
                (position, mean), textcoords="offset points", xytext=(0, 9),
                ha="center", fontsize=7.5,
            )
    positive_rates = percent(ordered["positive_capture_ret_abn_rate"].astype(float))
    drop_rates = percent(ordered["drop_ratio_lt_1_rate"].astype(float))
    width = 0.36
    bottom.bar([value - width / 2 for value in x], positive_rates, width=width,
               color=POSITIVE_COLOR, label="Positive abnormal capture")
    bottom.bar([value + width / 2 for value in x], drop_rates, width=width,
               color=NEUTRAL_COLOR, label="Drop ratio < 1")
    bottom.set_ylabel("Rate (%)")
    bottom.set_xticks(x, labels)
    bottom.set_xlabel("Quantile bucket (low to high)")
    bottom.legend(fontsize=8)
    if numeric_labels:
        for position, value in zip(x, positive_rates):
            bottom.text(position - width / 2, value, f"{value:.1f}%", ha="center", va="bottom", fontsize=7)
        for position, value in zip(x, drop_rates):
            bottom.text(position + width / 2, value, f"{value:.1f}%", ha="center", va="bottom", fontsize=7)
    monotonic_total = max(0, int(diagnostic["actual_bucket_count"]) - 1)
    note = (
        f"High minus low: {float(diagnostic['high_minus_low_bps']):.2f} bps; "
        f"nondecreasing steps: {int(diagnostic['monotonic_step_count'])}/{monotonic_total}. "
        + _boundary_note(boundaries, dimension)
    )
    figure.text(0.5, 0.015, note, ha="center", fontsize=7.5, wrap=True)
    figure.text(0.5, -0.005, GROSS_COST_LIMITATION, ha="center", fontsize=7.5)
    figure.subplots_adjust(bottom=0.14)
    finish_figure(plt, figure, path)
