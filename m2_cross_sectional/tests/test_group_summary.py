import sys
import json
from pathlib import Path
import unittest
from unittest import mock


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


class GroupSummaryContractTests(unittest.TestCase):
    def test_standard_group_summary_columns_match_plan(self):
        self.assertEqual(
            job.GROUP_SUMMARY_COLUMNS,
            (
                "n_events",
                "n_tickers",
                "n_capture_ret_abn",
                "n_capture_ret",
                "n_drop_ratio",
                "event_share_of_analysis",
                "ticker_share_of_analysis",
                "mean_capture_ret_abn",
                "stddev_capture_ret_abn",
                "se_capture_ret_abn",
                "ci95_low_capture_ret_abn",
                "ci95_high_capture_ret_abn",
                "median_capture_ret_abn",
                "p25_capture_ret_abn",
                "p75_capture_ret_abn",
                "positive_capture_ret_abn_rate",
                "mean_capture_ret",
                "median_capture_ret",
                "mean_drop_ratio",
                "median_drop_ratio",
                "p25_drop_ratio",
                "p75_drop_ratio",
                "drop_ratio_lt_1_rate",
                "low_n_flag",
                "low_ticker_flag",
                "report_eligible_flag",
            ),
        )

    def test_duplicate_quantile_cutpoints_are_collapsed(self):
        cuts = job.deduplicate_cutpoints(
            [0.1, 0.1, 0.2, 0.2, 0.5], maximum=0.5
        )
        self.assertEqual(cuts, [0.1, 0.2])

    def test_minimum_cut_can_separate_two_distinct_values(self):
        cuts = job.deduplicate_cutpoints([0.0, 1.0], maximum=1.0)
        self.assertEqual(cuts, [0.0])

    def test_team_environment_override_rewrites_canonical_paths(self):
        config = {
            "team": "/user/default/divcap",
            "grain_path": "/user/default/divcap/curated/div_event_grain",
            "panel_path": "/user/default/divcap/curated/div_event_panel",
            "metadata_path": "/user/default/divcap/reference/meta.jsonl",
            "pseudo_sector_path": "/user/default/divcap/curated/pseudo_sector",
            "output_root": "/user/default/divcap/m2/cross_sectional",
        }
        with mock.patch.dict("os.environ", {"TEAM": "/user/team/divcap"}):
            resolved = job.resolve_runtime_config(config)
        self.assertEqual(resolved["team"], "/user/team/divcap")
        self.assertEqual(
            resolved["grain_path"],
            "/user/team/divcap/curated/div_event_grain",
        )
        self.assertEqual(
            resolved["pseudo_sector_path"],
            "/user/team/divcap/curated/pseudo_sector",
        )

    def test_default_config_excludes_spy_benchmark(self):
        config = json.loads(
            (MODULE_DIR / "cross_sectional_config.json").read_text(encoding="utf-8")
        )
        self.assertEqual(config["market_ticker"], "SPY")

    def test_empty_market_ticker_is_rejected(self):
        config = json.loads(
            (MODULE_DIR / "cross_sectional_config.json").read_text(encoding="utf-8")
        )
        config["market_ticker"] = "  "
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=json.dumps(config))
        ):
            with self.assertRaisesRegex(job.M2ValidationError, "market_ticker"):
                job.load_config("config.json")

    def test_panel_contract_keeps_bar_date_for_dependence_review(self):
        self.assertIn("bar_date", job.PANEL_REQUIRED_COLUMNS)


if __name__ == "__main__":
    unittest.main()
