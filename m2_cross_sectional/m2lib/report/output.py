"""Insight validation and immutable local report-package writers."""

from __future__ import annotations

from .insights import *  # noqa: F401,F403


def render_insights_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# M2 Cross-Sectional V2 Insights — {payload['run_id']}",
        "",
        f"Generated at UTC: `{payload['generated_at_utc']}`",
        "",
    ]
    for item in payload["sections"]:
        lines.extend(
            [
                f"## {item['title']}",
                "",
                f"Status: `{item['status']}`",
                "",
                item["headline"],
                "",
                "Evidence:",
                "",
            ]
        )
        for entry in item["evidence"]:
            lines.append(
                f"- `{entry['metric']}`: {entry['display_value']} "
                f"(source: `{entry['source_table']}`)"
            )
        lines.extend(
            [
                "",
                f"Business interpretation: {item['business_interpretation']}",
                "",
                "Caveats:",
                "",
            ]
        )
        lines.extend(f"- {caveat}" for caveat in item["caveats"])
        lines.extend(
            [
                "",
                f"Recommended next step: {item['recommended_next_step']}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_insights(
    pd: Any,
    frames: Mapping[str, Any],
    config: Mapping[str, Any],
    run_id: str,
    output_dir: Path,
) -> Dict[str, Any]:
    payload = build_section_insights(frames, config, run_id)
    with open(output_dir / "section_insights.json", "w", encoding="utf-8") as handle:
        json.dump(json_safe(payload, pd), handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    with open(output_dir / "INSIGHTS_SUMMARY.md", "w", encoding="utf-8") as handle:
        handle.write(render_insights_markdown(payload))
    return payload


def write_report_metrics(
    pd: Any,
    frames: Mapping[str, Any],
    run_id: str,
    output_dir: Path,
) -> None:
    metrics = {
        "run_id": run_id,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "tables": {name: records(frame, pd) for name, frame in frames.items()},
        "limitations": [
            GROSS_COST_LIMITATION,
            SIC_TEMPORAL_LIMITATION,
            PSEUDO_MODEL_LIMITATION,
            UPSTREAM_MODEL_METRICS,
            PSEUDO_PROVENANCE_LIMITATION,
            DEPENDENCE_LIMITATION,
            CORPORATE_ACTION_LIMITATION,
            TAXONOMY_SEPARATION_STATEMENT,
        ],
        "figure_files": [
            f"figures/{filename}" for filename in FIGURE_OUTPUTS.values()
        ],
    }
    with open(output_dir / "report_metrics.json", "w", encoding="utf-8") as handle:
        json.dump(json_safe(metrics, pd), handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")


def write_results_readme(
    frames: Mapping[str, Any],
    run_id: str,
    run_root: str,
    output_dir: Path,
) -> None:
    overall = frames["overall_summary"].iloc[0]
    sic = frames["sic_coverage"].iloc[0]
    pseudo = frames["pseudo_sector_coverage"].iloc[0]
    inputs = input_metric_map(frames["input_summary"])
    lines = [
        f"# M2 Cross-Sectional V2 Results — {run_id}",
        "",
        "This immutable package was generated from compact, run-versioned V2 "
        "audit/core/manifest aggregates. It does not rescan M1 inputs or the "
        "M2 analysis base.",
        "",
        "## Run snapshot",
        "",
        f"- HDFS run root: `{run_root}`",
        f"- Ex-date range: {inputs.get('min_ex_date')} to {inputs.get('max_ex_date')}",
        f"- Base population: {int(overall['n_events']):,} events / {int(overall['n_tickers']):,} tickers",
        f"- Mean / median gross abnormal capture: {format_bps(overall['mean_capture_ret_abn'])} / {format_bps(overall['median_capture_ret_abn'])}",
        f"- Direct-SIC UNKNOWN share: {float(sic['unknown_event_share']):.1%}",
        f"- Pseudo recovery among direct-SIC unknown: {float(pseudo['pseudo_recovery_rate_of_sic_unknown']):.1%}",
        f"- Residual unresolved share: {int(pseudo['still_unresolved_events']) / int(pseudo['base_events']):.1%}",
        f"- Pseudo label level: `{pseudo['configured_label_level']}`",
        "",
        f"{GROSS_COST_LIMITATION}",
        "",
        "## Narrative",
        "",
        "- `INSIGHTS_SUMMARY.md` — deterministic twelve-section professional report.",
        "- `section_insights.json` — traceable section objects and numeric evidence.",
        "- `report_metrics.json` — machine-readable copy of every loaded aggregate.",
        "",
        "## CSV tables",
        "",
        *[f"- `{filename}`" for filename in CSV_OUTPUTS.values()],
        "",
        "## Figures",
        "",
        *[f"- `figures/{filename}`" for filename in FIGURE_OUTPUTS.values()],
        "",
        "## Interpretation and provenance",
        "",
        f"- {TAXONOMY_SEPARATION_STATEMENT}",
        "- Direct-SIC performance figures use report-eligible known labels only; "
        "the canonical CSV retains UNKNOWN and every low-N category.",
        "- Pseudo-sector figures use only direct-SIC-unknown events recovered by "
        "a valid configured-level model prediction; direct-known rows are excluded.",
        "- Liquidity means pre-event dollar-volume liquidity / size proxy, not "
        "historical market capitalization.",
        "- Event-time offset 0 is ex-date close-to-close; the ex-open strategy "
        "exit is shown separately in the overnight table and F12.",
        f"- {SIC_TEMPORAL_LIMITATION}",
        f"- {PSEUDO_MODEL_LIMITATION}",
        f"- {UPSTREAM_MODEL_METRICS}",
        f"- {PSEUDO_PROVENANCE_LIMITATION}",
        f"- {DEPENDENCE_LIMITATION}",
        f"- {CORPORATE_ACTION_LIMITATION}",
        "",
        "Every narrative number names a CSV source in `INSIGHTS_SUMMARY.md`; "
        "use the accepted manifest to trace that CSV back to the canonical HDFS "
        "aggregate and input contract.",
        "",
    ]
    with open(output_dir / "RESULTS_README.md", "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def verify_local_artifacts(output_dir: Path) -> None:
    missing = []
    empty = []
    for relative in required_local_artifacts():
        path = output_dir / relative
        if not path.exists():
            missing.append(relative)
        elif path.stat().st_size == 0:
            empty.append(relative)
    if missing or empty:
        raise ReportArtifactError(
            f"Generated report package is incomplete: missing={missing}, empty={empty}"
        )
    for filename in ("report_metrics.json", "section_insights.json"):
        with open(output_dir / filename, "r", encoding="utf-8") as handle:
            json.load(handle)
