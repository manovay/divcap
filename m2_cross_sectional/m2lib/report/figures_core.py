"""Shared numeric formatting and the core report figures."""

from __future__ import annotations

import textwrap

from .tables import *  # noqa: F401,F403
from .labels import boundary_range, funnel_stage_label, quantile_label

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


def finish_figure(
    plt: Any,
    figure: Any,
    path: Path,
    layout_rect: Tuple[float, float, float, float] = (0.0, 0.0, 1.0, 1.0),
) -> None:
    """Apply a layout that reserves space for figure-level notes and save."""

    figure.tight_layout(rect=layout_rect)
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
    labels = [funnel_stage_label(value) for value in sequential["stage"]]
    values = sequential["n_events"].astype(float).tolist()
    figure, axis = plt.subplots(figsize=(9.5, 5.3))
    bars = axis.barh(labels[::-1], values[::-1], color=DIRECT_COLOR)
    axis.set_xlabel("Number of dividend events")
    axis.set_title(
        "Analysis Sample Funnel\n"
        "From all dividend events to the accepted analysis sample"
    )
    axis.grid(axis="x", alpha=0.2)
    if numeric_labels:
        rows = list(sequential.iloc[::-1].iterrows())
        for bar, (_, row) in zip(bars, rows):
            text = (
                f"{int(row['n_events']):,} events | "
                f"{float(row['retention_from_prior']):.1%} kept from prior step"
                f" | {float(row['cumulative_retention']):.1%} of raw"
            )
            axis.text(
                bar.get_width() * 0.985,
                bar.get_y() + bar.get_height() / 2,
                text,
                va="center",
                ha="right",
                color="white",
                fontsize=8.2,
            )
    axis.text(
        0.0, -0.16,
        "Accepted sample: positive cash dividend, complete core returns and event "
        "window, with the configured market benchmark removed.",
        transform=axis.transAxes, fontsize=8.5, ha="left",
    )
    finish_figure(plt, figure, path, layout_rect=(0.0, 0.08, 1.0, 1.0))


def sector_coverage_figure(
    plt: Any, bridge: Any, coverage: Any, path: Path, numeric_labels: bool
) -> None:
    ordered = bridge.sort_values("state_order")
    display = {
        "direct_sic_known": "Direct SIC known (reported industry)",
        "pseudo_recovered": "Pseudo-sector recovered (model-predicted)",
        "still_unresolved": "Industry still unresolved",
    }
    colors = [DIRECT_COLOR, PSEUDO_COLOR, UNRESOLVED_COLOR]
    figure, axis = plt.subplots(figsize=(10, 4.8))
    left = 0.0
    for (_, row), color in zip(ordered.iterrows(), colors):
        share = float(row["event_share_of_base"])
        axis.barh(
            [0], [share * 100.0], left=left, color=color, height=0.45,
            label=display[str(row["sector_state"])],
        )
        if numeric_labels:
            axis.text(
                left + share * 50.0, 0,
                f"{int(row['n_events']):,} events\n{share:.1%}",
                ha="center", va="center", color="white" if share > 0.12 else "black",
                fontsize=7.8 if share <= 0.06 else 8.5,
            )
        left += share * 100.0
    audit = coverage.iloc[0]
    unknown_share = (
        float(audit["direct_sic_unknown_events"]) / float(audit["base_events"])
    )
    note = (
        f"No direct SIC: {unknown_share:.1%} | model recovered "
        f"{float(audit['pseudo_recovery_rate_of_sic_unknown']):.1%} of those events"
        f" | total industry coverage after recovery: "
        f"{float(audit['coverage_after_recovery_share']):.1%}"
    )
    axis.set_xlim(0, 100)
    axis.set_yticks([])
    axis.set_xlabel("Share of accepted analysis events (%)")
    figure.suptitle(
        "Industry Coverage Recovery\n"
        "Accepted analysis events; reported SIC and model predictions shown separately",
        y=0.98,
    )
    figure.text(0.5, 0.085, note, ha="center", fontsize=9)
    figure.text(
        0.5, 0.035,
        "Coverage only: reported SIC and model-predicted sectors remain separate "
        "in performance analysis.",
        ha="center", fontsize=8.5,
    )
    figure.legend(
        loc="upper center", bbox_to_anchor=(0.5, 0.875), ncol=3, fontsize=8,
    )
    finish_figure(plt, figure, path, layout_rect=(0.0, 0.16, 1.0, 0.79))


def overall_scorecard_figure(
    plt: Any, frame: Any, path: Path, numeric_labels: bool
) -> None:
    row = frame.iloc[0]
    metrics = [
        ("Average market-adjusted capture", format_bps(row["mean_capture_ret_abn"]).replace("bps", "basis points")),
        ("Median market-adjusted capture", format_bps(row["median_capture_ret_abn"]).replace("bps", "basis points")),
        ("Average unadjusted capture", format_bps(row["mean_capture_ret"]).replace("bps", "basis points")),
        ("Median unadjusted capture", format_bps(row["median_capture_ret"]).replace("bps", "basis points")),
        ("Positive market-adjusted capture", format_percent(row["positive_capture_ret_abn_rate"])),
        ("Median price-drop / dividend ratio", format_ratio(row["median_drop_ratio"])),
        ("Price drop smaller than dividend", format_percent(row["drop_ratio_lt_1_rate"])),
        ("Analysis population", f"{int(row['n_events']):,} events / {int(row['n_tickers']):,} companies"),
    ]
    figure, axis = plt.subplots(figsize=(10.5, 5.4))
    axis.axis("off")
    axis.set_title(
        "Overall Dividend-Capture Results\nAccepted analysis sample", pad=18
    )
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
    finish_figure(plt, figure, path, layout_rect=(0.0, 0.05, 1.0, 1.0))


def _boundary_note(boundaries: Any, dimension: str) -> str:
    subset = boundaries[boundaries["dimension"] == dimension].sort_values(
        "bucket_number"
    )
    parts = []
    for _, row in subset.iterrows():
        label = quantile_label(row["bucket_label"]).replace("\n", " ")
        interval = boundary_range(
            row["lower_bound_exclusive"], row["upper_bound_inclusive"], dimension
        )
        parts.append(f"{label}: {interval}")
    return "Group boundaries — " + "; ".join(parts)


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
    labels = [quantile_label(value) for value in ordered[label_column]]
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
    figure, (top, bottom) = plt.subplots(2, 1, figsize=(10.5, 8.6), sharex=True)
    top.errorbar(
        x, means, yerr=[lower_error, upper_error], fmt="o-", color=DIRECT_COLOR,
        capsize=4, label="Average ± unadjusted 95% descriptive interval",
    )
    top.scatter(x, medians, marker="D", color=NEUTRAL_COLOR, label="Median", zorder=4)
    top.axhline(0, color="#333333", linewidth=0.8)
    top.set_ylabel("Gross market-adjusted capture return (basis points)")
    top.set_title(title)
    top.legend(fontsize=8)
    if numeric_labels:
        for position, mean, (_, row) in zip(x, means, ordered.iterrows()):
            top.annotate(
                f"Average {mean:.2f} bps\n{int(row['n_capture_ret_abn']):,} events",
                (position, mean), textcoords="offset points", xytext=(0, 9),
                ha="center", fontsize=7.5,
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.82, "pad": 0.7},
            )
    positive_rates = percent(ordered["positive_capture_ret_abn_rate"].astype(float))
    drop_rates = percent(ordered["drop_ratio_lt_1_rate"].astype(float))
    width = 0.36
    bottom.bar([value - width / 2 for value in x], positive_rates, width=width,
               color=POSITIVE_COLOR, label="Positive market-adjusted capture")
    bottom.bar([value + width / 2 for value in x], drop_rates, width=width,
               color=NEUTRAL_COLOR, label="Price drop smaller than dividend")
    bottom.set_ylabel("Share of events (%)")
    bottom.set_xticks(x, labels)
    bottom.set_xlabel("Five equal-sized groups, ordered from low to high")
    bottom.legend(fontsize=8, loc="lower left")
    if numeric_labels:
        for position, value in zip(x, positive_rates):
            bottom.text(position - width / 2, value, f"{value:.1f}%", ha="center", va="bottom", fontsize=7)
        for position, value in zip(x, drop_rates):
            bottom.text(position + width / 2, value, f"{value:.1f}%", ha="center", va="bottom", fontsize=7)
    monotonic_total = max(0, int(diagnostic["actual_bucket_count"]) - 1)
    note = (
        f"Highest minus lowest average: {float(diagnostic['high_minus_low_bps']):.2f} bps; "
        f"average increased between {int(diagnostic['monotonic_step_count'])} of "
        f"{monotonic_total} adjacent groups. {_boundary_note(boundaries, dimension)}"
    )
    figure.text(
        0.5, 0.075, textwrap.fill(note.replace("$", r"\$"), width=155),
        ha="center", va="center", fontsize=7.4, linespacing=1.3,
    )
    figure.text(0.5, 0.018, GROSS_COST_LIMITATION, ha="center", fontsize=7.5)
    finish_figure(plt, figure, path, layout_rect=(0.0, 0.17, 1.0, 1.0))
