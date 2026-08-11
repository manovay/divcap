import json
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import m2_contract as contract  # noqa: E402
import make_report_artifacts as report  # noqa: E402
from v2_fixtures import config, frames  # noqa: E402


class ReportContractTests(unittest.TestCase):
    def test_report_registry_covers_every_compact_v2_aggregate(self):
        expected = set(contract.OUTPUT_RELATIVE_PATHS.values()) - {
            "analysis_base",
            "model_features",
            "model_outcomes",
        }
        self.assertEqual(set(report.TABLE_PATHS.values()), expected)
        self.assertIn("core/pseudo_sector", report.TABLE_PATHS.values())
        self.assertIn("audit/sector_coverage_bridge", report.TABLE_PATHS.values())

    def test_required_local_registry_is_complete(self):
        artifacts = set(report.required_local_artifacts())
        self.assertEqual(len(contract.FIGURE_OUTPUTS), 12)
        self.assertIn("section_insights.json", artifacts)
        self.assertIn("INSIGHTS_SUMMARY.md", artifacts)
        self.assertIn("pseudo_sector_summary.csv", artifacts)
        self.assertIn("figures/12_event_time_overnight_metrics.png", artifacts)

    def test_nonempty_or_partial_output_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report"
            output.mkdir()
            (output / "partial.txt").write_text("partial", encoding="utf-8")
            with self.assertRaisesRegex(report.ReportArtifactError, "nonempty"):
                report.prepare_output_directory(output)

    def test_numeric_unit_conversion(self):
        self.assertEqual(report.format_bps(0.001), "10.00 bps")
        self.assertEqual(report.format_percent(0.125), "12.5%")
        self.assertEqual(report.format_ratio(0.8), "0.800")

    def test_direct_selection_excludes_unknown_and_low_n(self):
        selected = report.select_sector_categories(
            frames()["sic_description_summary"],
            "direct_sic",
            "sic_description",
            20,
        )
        self.assertNotIn("UNKNOWN", selected["sic_description"].tolist())
        self.assertNotIn("TINY", selected["sic_description"].tolist())

    def test_taxonomy_chart_pairs_use_same_deterministic_order(self):
        source = frames()["pseudo_sector_summary"]
        first = report.select_sector_categories(
            source, "pseudo_sector", "pseudo_sector", 20
        )
        second = report.select_sector_categories(
            source.sample(frac=1.0, random_state=10),
            "pseudo_sector",
            "pseudo_sector",
            20,
        )
        self.assertEqual(
            first["pseudo_sector"].tolist(), second["pseudo_sector"].tolist()
        )

    def test_json_safe_replaces_nonfinite_values(self):
        value = report.json_safe(
            {"nan": float("nan"), "inf": float("inf"), "ok": 2}, pd
        )
        self.assertEqual(value, {"nan": None, "inf": None, "ok": 2})
        text = json.dumps(value, allow_nan=False)
        self.assertNotIn("NaN", text)

    def test_nonfigure_report_artifacts_smoke(self):
        fixture = frames()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report"
            report.prepare_output_directory(output)
            report.write_csv_outputs(fixture, output)
            report.write_report_metrics(pd, fixture, "fixture_run", output)
            payload = report.write_insights(
                pd, fixture, config(), "fixture_run", output
            )
            report.write_results_readme(
                fixture,
                "fixture_run",
                "/team/m2/cross_sectional/fixture_run",
                output,
            )
            self.assertEqual(len(payload["sections"]), 12)
            self.assertGreater((output / "report_metrics.json").stat().st_size, 0)
            self.assertGreater((output / "section_insights.json").stat().st_size, 0)
            self.assertGreater((output / "INSIGHTS_SUMMARY.md").stat().st_size, 0)

    def test_report_reconciliation_accepts_fixture_and_rejects_mismatch(self):
        fixture = frames()
        report.validate_report_reconciliation(
            fixture,
            config(),
            "fixture_run",
            "/team/m2/cross_sectional/fixture_run",
        )
        broken = dict(fixture)
        broken["pseudo_sector_summary"] = fixture["pseudo_sector_summary"].copy()
        broken["pseudo_sector_summary"].loc[0, "n_events"] += 1
        with self.assertRaisesRegex(report.ReportArtifactError, "recovered events"):
            report.validate_report_reconciliation(
                broken,
                config(),
                "fixture_run",
                "/team/m2/cross_sectional/fixture_run",
            )


if __name__ == "__main__":
    unittest.main()
