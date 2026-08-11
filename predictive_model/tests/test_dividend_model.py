import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark_dividend_predictor import prepare_training_frame, is_csv_input_path


class DividendModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]")
                     .appName("dividend-model-tests")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_is_csv_input_path_detects_csv_directories(self):
        self.assertTrue(is_csv_input_path("/user/ms16965_nyu_edu/divcap/curated/div_event_grain_csv"))
        self.assertTrue(is_csv_input_path("/tmp/data.csv"))
        self.assertFalse(is_csv_input_path("/user/ms16965_nyu_edu/divcap/curated/div_event_grain"))

    def test_prepare_training_frame_builds_label_and_features(self):
        sample_rows = [
            {
                "ticker": "AAPL",
                "cash_amount": 0.50,
                "div_yield": 0.01,
                "drop_pct": 0.02,
                "pre_avg_ret": 0.001,
                "pre_avg_abn_ret": 0.0005,
                "pre_vol": 0.01,
                "pre_avg_dollar_volume": 1000000.0,
                "post_avg_ret": 0.002,
                "post_avg_abn_ret": 0.001,
                "n_distributions": 1,
                "frequency": 4,
                "n_bars": 9,
                "n_bars_pre": 5,
                "n_bars_post": 3,
                "has_core": True,
                "window_complete": True,
                "window_contiguous": True,
                "drop_ratio_extreme": False,
                "low_yield": False,
                "capture_ret_abn": 0.004,
            },
            {
                "ticker": "MSFT",
                "cash_amount": 0.30,
                "div_yield": 0.003,
                "drop_pct": -0.01,
                "pre_avg_ret": -0.002,
                "pre_avg_abn_ret": -0.001,
                "pre_vol": 0.03,
                "pre_avg_dollar_volume": 500000.0,
                "post_avg_ret": -0.001,
                "post_avg_abn_ret": -0.0002,
                "n_distributions": 1,
                "frequency": 12,
                "n_bars": 9,
                "n_bars_pre": 5,
                "n_bars_post": 3,
                "has_core": True,
                "window_complete": True,
                "window_contiguous": True,
                "drop_ratio_extreme": False,
                "low_yield": True,
                "capture_ret_abn": -0.002,
            },
        ]

        df = self.spark.createDataFrame(sample_rows)
        prepared = prepare_training_frame(df)

        self.assertIn("label", prepared.columns)
        self.assertIn("cash_amount", prepared.columns)
        self.assertIn("div_yield", prepared.columns)
        self.assertEqual(prepared.count(), 2)
        self.assertEqual(prepared.filter("label = 1").count(), 1)


if __name__ == "__main__":
    unittest.main()
