import json
from pathlib import Path
import sys
import unittest
from unittest import mock


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


class ConfigContractTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(
            (MODULE_DIR / "cross_sectional_config.json").read_text(encoding="utf-8")
        )

    def load(self, config):
        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(config))):
            return job.load_config("config.json")

    def test_v2_config_loads(self):
        self.assertEqual(self.load(self.config)["pseudo_sector_label_level"], "hybrid")

    def test_each_new_required_key_is_actionable(self):
        new_keys = (
            "pseudo_sector_path",
            "pseudo_sector_column",
            "pseudo_sector_label_level",
            "min_pseudo_cell_n",
            "min_report_tickers",
            "report_top_pseudo_n",
            "report_numeric_labels",
            "insight_min_abs_bps",
        )
        for key in new_keys:
            with self.subTest(key=key):
                broken = dict(self.config)
                broken.pop(key)
                with self.assertRaisesRegex(job.M2ValidationError, key):
                    self.load(broken)

    def test_type_and_range_validation(self):
        invalid = {
            "pseudo_sector_path": " ",
            "pseudo_sector_column": "sector",
            "pseudo_sector_label_level": " ",
            "min_pseudo_cell_n": 0,
            "min_report_tickers": 0,
            "report_top_pseudo_n": 0,
            "report_numeric_labels": "true",
            "insight_min_abs_bps": -0.1,
            "min_cell_n": "30",
            "metric_tolerance": "1e-8",
        }
        for key, value in invalid.items():
            with self.subTest(key=key):
                broken = dict(self.config)
                broken[key] = value
                with self.assertRaisesRegex(job.M2ValidationError, key):
                    self.load(broken)

    def test_team_override_rewrites_pseudo_path(self):
        with mock.patch.dict("os.environ", {"TEAM": "/user/runtime/divcap"}):
            resolved = job.resolve_runtime_config(self.config)
        self.assertEqual(
            resolved["pseudo_sector_path"],
            "/user/runtime/divcap/curated/pseudo_sector",
        )


if __name__ == "__main__":
    unittest.main()
