import sys
from pathlib import Path
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


class MetricIdentityTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "drop_pct": 0.008,
            "drop_ratio": 0.8,
            "div_yield": 0.01,
            "capture_ret": 0.002,
            "mkt_overnight_ret": 0.0005,
            "capture_ret_abn": 0.0015,
            "stock_overnight_ret": -0.008,
            "abn_overnight_ret": -0.0085,
        }

    def test_consistent_metrics_have_negligible_residuals(self):
        residuals = job.metric_identity_residuals(self.row)
        self.assertEqual(set(residuals), {
            "drop_pct_equals_drop_ratio_times_div_yield",
            "capture_ret_equals_div_yield_minus_drop_pct",
            "capture_ret_abn_equals_capture_ret_minus_market",
            "stock_overnight_ret_equals_negative_drop_pct",
            "abn_overnight_ret_equals_stock_minus_market",
        })
        self.assertTrue(all(value < 1e-15 for value in residuals.values()))

    def test_broken_capture_identity_is_detected(self):
        broken = dict(self.row)
        broken["capture_ret_abn"] = 0.01
        residuals = job.metric_identity_residuals(broken)
        self.assertGreater(
            residuals["capture_ret_abn_equals_capture_ret_minus_market"], 1e-3
        )

    def test_null_input_is_rejected(self):
        broken = dict(self.row)
        broken["drop_ratio"] = None
        with self.assertRaises(ValueError):
            job.metric_identity_residuals(broken)


if __name__ == "__main__":
    unittest.main()
