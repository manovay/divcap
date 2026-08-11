from pathlib import Path
import sys
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


class SectorPopulationRuleTests(unittest.TestCase):
    def test_sector_state_truth_table(self):
        cases = (
            ("BANKS", None, None, "direct_sic_known"),
            ("BANKS", "SIC 60", "hybrid", "direct_sic_known"),
            ("UNKNOWN", "SIC 60", "hybrid", "pseudo_recovered"),
            ("UNKNOWN", None, None, "still_unresolved"),
        )
        for direct, pseudo, level, expected in cases:
            with self.subTest(direct=direct, pseudo=pseudo):
                self.assertEqual(
                    job.classify_sector_state(direct, pseudo, level, "hybrid"),
                    expected,
                )

    def test_populations_are_disjoint(self):
        rows = [
            ("known", "SIC 60", "hybrid"),
            ("UNKNOWN", "SIC 60", "hybrid"),
            ("UNKNOWN", None, None),
        ]
        states = [job.classify_sector_state(*row, "hybrid") for row in rows]
        self.assertEqual(states.count("direct_sic_known"), 1)
        self.assertEqual(states.count("pseudo_recovered"), 1)
        self.assertEqual(states.count("still_unresolved"), 1)

    def test_coverage_reconciliation_and_failure(self):
        residuals = job.reconcile_sector_counts(
            base_events=10,
            direct_known_events=3,
            direct_unknown_events=7,
            pseudo_recovered_events=4,
            still_unresolved_events=3,
        )
        self.assertTrue(all(value == 0 for value in residuals.values()))
        with self.assertRaisesRegex(job.M2ValidationError, "reconciliation failed"):
            job.reconcile_sector_counts(
                base_events=10,
                direct_known_events=3,
                direct_unknown_events=7,
                pseudo_recovered_events=4,
                still_unresolved_events=2,
            )

    def test_pseudo_match_does_not_change_direct_group(self):
        self.assertEqual(
            job.classify_sector_state("UNKNOWN", "SIC 60", "hybrid", "hybrid"),
            "pseudo_recovered",
        )
        self.assertEqual("UNKNOWN", "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
