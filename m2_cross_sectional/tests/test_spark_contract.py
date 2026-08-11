import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(
    HAS_PYSPARK,
    "PySpark is not installed locally; tiny-DataFrame integration runs on the Spark runtime",
)
class SparkContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        try:
            cls.spark = (
                SparkSession.builder.master("local[1]")
                .appName("m2-v2-local-contract-tests")
                .getOrCreate()
            )
        except Exception as exc:
            raise unittest.SkipTest(
                "PySpark is importable but its local Java gateway is unavailable; "
                "run this file with spark-submit --master local[1] "
                "--deploy-mode client"
            ) from exc

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_tiny_join_preserves_rows_and_states(self):
        base = self.spark.createDataFrame(
            [
                ("AAA", "BANKS"),
                ("BBB", "UNKNOWN"),
                ("CCC", "UNKNOWN"),
            ],
            "ticker string, sic_description string",
        )
        lookup = self.spark.createDataFrame(
            [("AAA", "SIC 60", "hybrid", "CS"), ("BBB", "SIC 20", "hybrid", "CS")],
            "_pseudo_join_ticker string, pseudo_sector string, label_level string, pseudo_sec_type string",
        )
        enriched, coverage, bridge, counts = job.enrich_sector_states(
            self.spark,
            base,
            lookup,
            {
                "configured_label_level": "hybrid",
                "pseudo_sector_path": "/fixture",
                "observed_label_levels_json": '["hybrid"]',
                "source_rows": 2,
                "source_tickers": 2,
                "source_labels": 2,
                "conflicting_ticker_count": 0,
                "conflicting_level_ticker_count": 0,
                "blank_ticker_count": 0,
                "blank_label_count": 0,
                "blank_label_level_count": 0,
                "duplicate_identical_row_count": 0,
            },
        )
        self.assertEqual(enriched.count(), 3)
        self.assertEqual(counts["join_row_delta"], 0)
        self.assertEqual(counts["pseudo_recovered_events"], 1)
        self.assertEqual(counts["still_unresolved_events"], 1)
        self.assertEqual(bridge.count(), 3)
        self.assertEqual(coverage.first()["event_reconciliation_passed"], True)

    def test_tiny_group_summary_has_metric_specific_n_and_null_interval(self):
        frame = self.spark.createDataFrame(
            [("A", 0.01, None, 0.8), ("B", None, 0.02, None)],
            "ticker string, capture_ret_abn double, capture_ret double, drop_ratio double",
        )
        row = job.grouped_summary(
            frame, min_cell_n=3, min_report_tickers=2
        ).first()
        self.assertEqual(row["n_events"], 2)
        self.assertEqual(row["n_capture_ret_abn"], 1)
        self.assertEqual(row["n_capture_ret"], 1)
        self.assertIsNone(row["stddev_capture_ret_abn"])
        self.assertFalse(row["report_eligible_flag"])


if __name__ == "__main__":
    unittest.main()
