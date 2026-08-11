"""Sector and event-time figures plus the figure-output registry."""

from __future__ import annotations

from .figures_core import *  # noqa: F401,F403

def _truthy_series(values: Any) -> Any:
    if str(values.dtype) == "bool":
        return values
    return values.astype(str).str.lower().isin(("true", "1", "yes"))


def select_sector_categories(
    frame: Any,
    taxonomy: str,
    group_column: str,
    top_n: int,
) -> Any:
    candidates = frame.copy()
    if taxonomy == "direct_sic":
        candidates = candidates[candidates[group_column].astype(str) != "UNKNOWN"]
    denominator = float(candidates["n_events"].sum())
    candidates = candidates[_truthy_series(candidates["report_eligible_flag"])]
    selected = candidates.sort_values(
        ["n_events", group_column], ascending=[False, True]
    ).head(top_n)
    selected = selected.sort_values(
        ["mean_capture_ret_abn", group_column], ascending=[True, True]
    ).copy()
    selected["chart_event_share"] = (
        selected["n_events"].astype(float) / denominator if denominator else 0.0
    )
    return selected.reset_index(drop=True)


def _short_label(value: Any, maximum: int = 60) -> str:
    text = str(value)
    return text if len(text) <= maximum else text[: maximum - 1] + "…"


def sector_performance_figure(
    plt: Any,
    selected: Any,
    group_column: str,
    title: str,
    color: str,
    path: Path,
    numeric_labels: bool,
) -> None:
    height = max(5.0, 0.55 * max(1, len(selected)) + 2.1)
    figure, axis = plt.subplots(figsize=(11.5, height))
    if selected.empty:
        annotate_empty(axis, "Insufficient report-eligible groups")
        axis.set_title(title)
        finish_figure(plt, figure, path)
        return
    labels = [_short_label(value) for value in selected[group_column]]
    means = [bps(float(value)) for value in selected["mean_capture_ret_abn"]]
    medians = [bps(float(value)) for value in selected["median_capture_ret_abn"]]
    y = list(range(len(selected)))
    lower = [
        max(0.0, mean - bps(float(value))) if not is_missing(value) else 0.0
        for mean, value in zip(means, selected["ci95_low_capture_ret_abn"])
    ]
    upper = [
        max(0.0, bps(float(value)) - mean) if not is_missing(value) else 0.0
        for mean, value in zip(means, selected["ci95_high_capture_ret_abn"])
    ]
    axis.errorbar(means, y, xerr=[lower, upper], fmt="o", color=color, capsize=4,
                  label="Mean ± unadjusted 95% descriptive interval")
    axis.scatter(medians, y, marker="D", color=NEUTRAL_COLOR, label="Median", zorder=4)
    axis.axvline(0, color="#333333", linewidth=0.8)
    axis.set_yticks(y, labels)
    axis.set_xlabel("Gross abnormal capture return (bps)")
    axis.set_title(title)
    axis.legend(fontsize=8)
    if numeric_labels:
        for position, mean, (_, row) in zip(y, means, selected.iterrows()):
            axis.annotate(
                f"{mean:.2f} bps | N={int(row['n_capture_ret_abn']):,} | "
                f"tickers={int(row['n_tickers']):,} | share={float(row['chart_event_share']):.1%}",
                (mean, position), textcoords="offset points", xytext=(8, 0),
                va="center", fontsize=7.3,
            )
    figure.text(
        0.5, 0.01,
        "Largest report-eligible categories by event count; displayed in mean order. "
        + GROSS_COST_LIMITATION,
        ha="center", fontsize=7.8,
    )
    finish_figure(plt, figure, path)


def sector_rate_figure(
    plt: Any,
    selected: Any,
    group_column: str,
    title: str,
    path: Path,
    numeric_labels: bool,
) -> None:
    height = max(5.0, 0.55 * max(1, len(selected)) + 2.1)
    figure, axis = plt.subplots(figsize=(11.0, height))
    if selected.empty:
        annotate_empty(axis, "Insufficient report-eligible groups")
        axis.set_title(title)
        finish_figure(plt, figure, path)
        return
    labels = [_short_label(value) for value in selected[group_column]]
    y = list(range(len(selected)))
    positive = percent(selected["positive_capture_ret_abn_rate"].astype(float))
    drop = percent(selected["drop_ratio_lt_1_rate"].astype(float))
    height_bar = 0.36
    axis.barh([value - height_bar / 2 for value in y], positive, height=height_bar,
              color=POSITIVE_COLOR, label="Positive abnormal capture")
    axis.barh([value + height_bar / 2 for value in y], drop, height=height_bar,
              color=NEUTRAL_COLOR, label="Drop ratio < 1")
    axis.set_yticks(y, labels)
    axis.set_xlabel("Rate (%)")
    axis.set_title(title)
    axis.legend(fontsize=8)
    if numeric_labels:
        for position, value in zip(y, positive):
            axis.text(value, position - height_bar / 2, f" {value:.1f}%", va="center", fontsize=7.5)
        for position, value in zip(y, drop):
            axis.text(value, position + height_bar / 2, f" {value:.1f}%", va="center", fontsize=7.5)
    figure.text(0.5, 0.01, GROSS_COST_LIMITATION, ha="center", fontsize=8)
    finish_figure(plt, figure, path)


def event_time_daily_figure(
    plt: Any, frame: Any, path: Path, numeric_labels: bool
) -> None:
    ordered = frame.sort_values("offset")
    x = ordered["offset"].astype(int).tolist()
    mean = bps(ordered["mean_abn_ret_cc"].astype(float))
    median = bps(ordered["median_abn_ret_cc"].astype(float))
    p25 = bps(ordered["p25_abn_ret_cc"].astype(float))
    p75 = bps(ordered["p75_abn_ret_cc"].astype(float))
    figure, axis = plt.subplots(figsize=(10.5, 5.8))
    axis.fill_between(x, p25, p75, color="#C7DCE8", alpha=0.65, label="IQR")
    axis.plot(x, mean, marker="o", color=DIRECT_COLOR, label="Mean")
    axis.plot(x, median, marker="D", color=NEUTRAL_COLOR, label="Median")
    axis.axhline(0, color="#333333", linewidth=0.8)
    axis.axvline(0, color=NEGATIVE_COLOR, linestyle="--", linewidth=1.0)
    axis.set_xticks(x)
    axis.set_xlabel("Trading-day offset from ex-date")
    axis.set_ylabel("Abnormal close-to-close return (bps)")
    axis.set_title("F11 — Daily event-time profile (offset 0 is ex-date close-to-close)")
    axis.legend(fontsize=8)
    if numeric_labels:
        for position, value, (_, row) in zip(x, mean, ordered.iterrows()):
            axis.annotate(
                f"{float(value):.2f}\nN={int(row['n_abn_ret_cc']):,}",
                (position, value), textcoords="offset points", xytext=(0, 8),
                ha="center", fontsize=7.2,
            )
    figure.text(
        0.5, 0.01,
        "Offset 0 is not the ex-open strategy exit. " + DEPENDENCE_LIMITATION,
        ha="center", fontsize=7.8,
    )
    finish_figure(plt, figure, path)


def event_time_overnight_figure(
    plt: Any, frame: Any, path: Path, numeric_labels: bool
) -> None:
    row = frame.iloc[0]
    labels = ["Stock overnight", "Abnormal overnight", "Capture return"]
    means = [
        bps(float(row["mean_stock_overnight_ret"])),
        bps(float(row["mean_abn_overnight_ret"])),
        bps(float(row["mean_capture_ret"])),
    ]
    medians = [
        bps(float(row["median_stock_overnight_ret"])),
        bps(float(row["median_abn_overnight_ret"])),
        bps(float(row["median_capture_ret"])),
    ]
    counts = [
        int(row["n_stock_overnight_ret"]),
        int(row["n_abn_overnight_ret"]),
        int(row["n_capture_ret"]),
    ]
    x = list(range(3))
    width = 0.35
    figure, axis = plt.subplots(figsize=(9.5, 5.2))
    axis.bar([value - width / 2 for value in x], means, width=width,
             color=DIRECT_COLOR, label="Mean")
    axis.bar([value + width / 2 for value in x], medians, width=width,
             color=NEUTRAL_COLOR, label="Median")
    axis.axhline(0, color="#333333", linewidth=0.8)
    axis.set_xticks(x, labels)
    axis.set_ylabel("Return (bps)")
    axis.set_title("F12 — Ex-date overnight and ex-open capture metrics")
    axis.legend()
    if numeric_labels:
        for position, mean, median, count in zip(x, means, medians, counts):
            axis.text(position - width / 2, mean, f"{mean:.2f}\nN={count:,}", ha="center", va="bottom" if mean >= 0 else "top", fontsize=7.5)
            axis.text(position + width / 2, median, f"{median:.2f}\nN={count:,}", ha="center", va="bottom" if median >= 0 else "top", fontsize=7.5)
    figure.text(0.5, 0.01, GROSS_COST_LIMITATION, ha="center", fontsize=8)
    finish_figure(plt, figure, path)


def write_figures(
    plt: Any,
    frames: Mapping[str, Any],
    config: Mapping[str, Any],
    output_dir: Path,
) -> None:
    figure_dir = output_dir / "figures"
    numeric = bool(config["report_numeric_labels"])
    sample_funnel_figure(plt, frames["sample_funnel"], figure_dir / FIGURE_OUTPUTS["sample_funnel"], numeric)
    sector_coverage_figure(plt, frames["sector_coverage_bridge"], frames["pseudo_sector_coverage"], figure_dir / FIGURE_OUTPUTS["sector_coverage"], numeric)
    overall_scorecard_figure(plt, frames["overall_summary"], figure_dir / FIGURE_OUTPUTS["overall"], numeric)
    ordered_specs = (
        ("yield", "yield_summary", "div_yield_bucket", "F04 — Dividend-yield cross section"),
        ("volatility", "volatility_summary", "pre_vol_bucket", "F05 — Pre-event volatility cross section"),
        ("liquidity", "liquidity_summary", "pre_avg_dollar_volume_bucket", "F06 — Pre-event dollar-volume liquidity / size-proxy cross section"),
    )
    for dimension, table, label, title in ordered_specs:
        ordered_profile_figure(
            plt, frames[table], frames["bucket_boundaries"],
            frames["dimension_diagnostics"], dimension, label, title,
            figure_dir / FIGURE_OUTPUTS[dimension], numeric,
        )
    direct = select_sector_categories(
        frames["sic_description_summary"], "direct_sic", "sic_description",
        int(config["report_top_sic_n"]),
    )
    pseudo = select_sector_categories(
        frames["pseudo_sector_summary"], "pseudo_sector", "pseudo_sector",
        int(config["report_top_pseudo_n"]),
    )
    sector_performance_figure(
        plt, direct, "sic_description",
        "F07 — Direct SIC (known labels only): current-reference descriptions",
        DIRECT_COLOR, figure_dir / FIGURE_OUTPUTS["sic_performance"], numeric,
    )
    sector_rate_figure(
        plt, direct, "sic_description",
        "F08 — Direct SIC (known labels only): outcome rates",
        figure_dir / FIGURE_OUTPUTS["sic_rates"], numeric,
    )
    level = str(config["pseudo_sector_label_level"])
    sector_performance_figure(
        plt, pseudo, "pseudo_sector",
        f"F09 — Model-predicted pseudo-sector (recovered only; label_level={level})",
        PSEUDO_COLOR, figure_dir / FIGURE_OUTPUTS["pseudo_performance"], numeric,
    )
    sector_rate_figure(
        plt, pseudo, "pseudo_sector",
        f"F10 — Model-predicted pseudo-sector outcome rates (label_level={level})",
        figure_dir / FIGURE_OUTPUTS["pseudo_rates"], numeric,
    )
    event_time_daily_figure(plt, frames["event_time_daily"], figure_dir / FIGURE_OUTPUTS["event_time_daily"], numeric)
    event_time_overnight_figure(plt, frames["event_time_overnight"], figure_dir / FIGURE_OUTPUTS["event_time_overnight"], numeric)
    if plt.get_fignums():
        raise ReportArtifactError(
            f"Chart generation left open matplotlib figures: {plt.get_fignums()}"
        )
