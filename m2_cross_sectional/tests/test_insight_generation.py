from pathlib import Path
import sys
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import m2_contract as contract  # noqa: E402
import make_report_artifacts as report  # noqa: E402
from v2_fixtures import config, frames  # noqa: E402


class InsightGenerationTests(unittest.TestCase):
    def build(self):
        return report.build_section_insights(
            frames(), config(), "fixture_run", "2026-08-10T00:00:00+00:00"
        )

    def test_required_sections_and_markdown_order(self):
        payload = self.build()
        expected = [identifier for identifier, _ in contract.REPORT_SECTIONS]
        self.assertEqual(
            [item["section_id"] for item in payload["sections"]], expected
        )
        markdown = report.render_insights_markdown(payload)
        positions = [markdown.index(f"## {title}") for _, title in contract.REPORT_SECTIONS]
        self.assertEqual(positions, sorted(positions))

    def test_every_evidence_item_is_traceable(self):
        payload = self.build()
        csv_names = set(contract.CSV_OUTPUTS.values())
        for item in payload["sections"]:
            self.assertTrue(item["evidence"])
            for entry in item["evidence"]:
                self.assertTrue(entry["metric"])
                self.assertIn(entry["source_table"], csv_names)
                self.assertIn(entry["display_value"], report.render_insights_markdown(payload))

    def test_low_n_extreme_is_suppressed(self):
        payload = self.build()
        text = " ".join(item["headline"] for item in payload["sections"])
        self.assertNotIn("TINY", text.upper())
        self.assertNotIn("tiny", text)

    def test_direct_and_pseudo_language_is_separate(self):
        payload = self.build()
        by_id = {item["section_id"]: item for item in payload["sections"]}
        self.assertIn("direct SIC", by_id["direct_sic"]["business_interpretation"])
        self.assertIn("model-predicted", by_id["pseudo_sector"]["headline"])
        self.assertNotIn("blended ranking", by_id["pseudo_sector"]["headline"])

    def test_flat_and_mixed_behavior_and_threshold_wording(self):
        payload = self.build()
        by_id = {item["section_id"]: item for item in payload["sections"]}
        self.assertEqual(by_id["liquidity"]["status"], "flat")
        self.assertIn("materiality threshold", by_id["liquidity"]["headline"])
        self.assertNotIn("statistical", by_id["liquidity"]["headline"])

    def test_prohibited_affirmative_claim_is_rejected(self):
        payload = self.build()
        payload["sections"][0]["headline"] = "This is the best sector."
        with self.assertRaisesRegex(report.ReportArtifactError, "Prohibited"):
            report.validate_insight_payload(payload)

    def test_required_limitations_are_present(self):
        payload = self.build()
        by_id = {item["section_id"]: item for item in payload["sections"]}
        self.assertIn(contract.GROSS_COST_LIMITATION, by_id["executive_summary"]["caveats"])
        self.assertIn(contract.SIC_TEMPORAL_LIMITATION, by_id["direct_sic"]["caveats"])
        self.assertIn(contract.PSEUDO_MODEL_LIMITATION, by_id["pseudo_sector"]["caveats"])
        self.assertIn(contract.PSEUDO_PROVENANCE_LIMITATION, by_id["pseudo_sector"]["caveats"])

    def test_reproducibility_apart_from_timestamp(self):
        first = self.build()
        second = self.build()
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
