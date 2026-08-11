import math
from pathlib import Path
import sys
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


class ExpandedGroupSummaryTests(unittest.TestCase):
    def test_metric_specific_n_and_exact_summary_math(self):
        rows = [
            {"ticker": "A", "capture_ret_abn": -0.01, "capture_ret": 0.01, "drop_ratio": 0.5},
            {"ticker": "A", "capture_ret_abn": 0.01, "capture_ret": None, "drop_ratio": 1.5},
            {"ticker": "B", "capture_ret_abn": None, "capture_ret": 0.03, "drop_ratio": None},
        ]
        result = job.summarize_numeric_records(
            rows,
            analysis_events=6,
            analysis_tickers=4,
            min_cell_n=3,
            min_report_tickers=2,
        )
        self.assertEqual(result["n_events"], 3)
        self.assertEqual(result["n_capture_ret_abn"], 2)
        self.assertEqual(result["n_capture_ret"], 2)
        self.assertEqual(result["n_drop_ratio"], 2)
        self.assertAlmostEqual(result["mean_capture_ret_abn"], 0.0)
        self.assertAlmostEqual(result["stddev_capture_ret_abn"], math.sqrt(0.0002))
        self.assertAlmostEqual(result["se_capture_ret_abn"], 0.01)
        self.assertAlmostEqual(result["ci95_low_capture_ret_abn"], -0.0196)
        self.assertAlmostEqual(result["ci95_high_capture_ret_abn"], 0.0196)
        self.assertAlmostEqual(result["positive_capture_ret_abn_rate"], 0.5)
        self.assertAlmostEqual(result["mean_capture_ret"], 0.02)
        self.assertAlmostEqual(result["mean_drop_ratio"], 1.0)
        self.assertAlmostEqual(result["drop_ratio_lt_1_rate"], 0.5)
        self.assertAlmostEqual(result["event_share_of_analysis"], 0.5)
        self.assertAlmostEqual(result["ticker_share_of_analysis"], 0.5)
        self.assertTrue(result["report_eligible_flag"])

    def test_single_observation_interval_is_null_and_ineligible(self):
        result = job.summarize_numeric_records(
            [{"ticker": "A", "capture_ret_abn": 0.01, "capture_ret": 0.02, "drop_ratio": 0.8}],
            min_cell_n=2,
            min_report_tickers=2,
        )
        self.assertIsNone(result["stddev_capture_ret_abn"])
        self.assertIsNone(result["se_capture_ret_abn"])
        self.assertIsNone(result["ci95_low_capture_ret_abn"])
        self.assertFalse(result["report_eligible_flag"])

    def test_ordered_diagnostics_cover_shapes(self):
        for means, expected_sign, expected_steps in (
            ([0.0, 0.01, 0.02], 1, 2),
            ([0.02, 0.01, 0.0], -1, 0),
            ([0.01, 0.01, 0.01], 0, 2),
            ([0.0, 0.02, 0.01], 1, 1),
        ):
            rows = [
                {
                    "bucket": f"Q{index + 1:02d}",
                    "mean_capture_ret_abn": value,
                    "median_capture_ret_abn": value,
                    "positive_capture_ret_abn_rate": 0.5 + value,
                }
                for index, value in enumerate(means)
            ]
            result = job.ordered_diagnostic_from_records("x", rows, "bucket")
            with self.subTest(means=means):
                actual_sign = (result["high_minus_low_bps"] > 0) - (
                    result["high_minus_low_bps"] < 0
                )
                self.assertEqual(actual_sign, expected_sign)
                self.assertEqual(result["monotonic_step_count"], expected_steps)

    def test_sector_diagnostics_suppress_low_n_extreme(self):
        records = [
            {"sector": "tiny", "n_events": 1, "n_tickers": 1, "mean_capture_ret_abn": 1.0, "report_eligible_flag": False},
            {"sector": "eligible_low", "n_events": 50, "n_tickers": 10, "mean_capture_ret_abn": -0.01, "report_eligible_flag": True},
            {"sector": "eligible_high", "n_events": 60, "n_tickers": 12, "mean_capture_ret_abn": 0.02, "report_eligible_flag": True},
        ]
        result = job.sector_diagnostic_from_records("pseudo", records, "sector", 111, 23)
        self.assertEqual(result["highest_observed_eligible_group"], "eligible_high")
        self.assertEqual(result["lowest_observed_eligible_group"], "eligible_low")
        self.assertEqual(result["eligible_category_count"], 2)


if __name__ == "__main__":
    unittest.main()
