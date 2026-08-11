import sys
from pathlib import Path
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


class FeatureLeakageTests(unittest.TestCase):
    def test_declared_feature_whitelist_is_leakage_free(self):
        self.assertEqual(
            job.validate_model_feature_columns(job.MODEL_FEATURE_COLUMNS), []
        )

    def test_realized_and_full_sample_fields_are_rejected(self):
        columns = list(job.MODEL_FEATURE_COLUMNS) + [
            "capture_ret_abn",
            "drop_ratio",
            "sic_description",
            "sic_code",
            "pseudo_sector",
            "label_level",
            "sector_state",
            "sector_source",
            "effective_sector",
            "div_yield_bucket",
        ]
        self.assertEqual(
            job.validate_model_feature_columns(columns),
            [
                "capture_ret_abn",
                "div_yield_bucket",
                "drop_ratio",
                "effective_sector",
                "label_level",
                "pseudo_sector",
                "sector_source",
                "sector_state",
                "sic_code",
                "sic_description",
            ],
        )

    def test_feature_and_outcome_contracts_share_stable_keys(self):
        keys = {"event_id", "ticker", "ex_date"}
        self.assertTrue(keys.issubset(job.MODEL_FEATURE_COLUMNS))
        self.assertTrue(keys.issubset(job.MODEL_OUTCOME_COLUMNS))


if __name__ == "__main__":
    unittest.main()
