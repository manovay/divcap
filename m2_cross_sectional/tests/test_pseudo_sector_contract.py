from pathlib import Path
import sys
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import run_cross_sectional as job  # noqa: E402


PATH = "/team/curated/pseudo_sector"


class PseudoSectorContractTests(unittest.TestCase):
    def test_normalization_trims_without_case_folding_tickers_or_labels(self):
        lookup, audit = job.validate_pseudo_records(
            [
                {
                    "ticker": " abc ",
                    "pseudo_sector": " SIC 60 ",
                    "label_level": " hybrid ",
                },
                {
                    "ticker": "blank",
                    "pseudo_sector": "  ",
                    "label_level": "hybrid",
                },
            ],
            "hybrid",
            PATH,
        )
        self.assertEqual(lookup["abc"]["pseudo_sector"], "SIC 60")
        self.assertEqual(audit["blank_label_count"], 1)

    def test_case_distinct_vendor_tickers_remain_distinct(self):
        lookup, _ = job.validate_pseudo_records(
            [
                {"ticker": "CPK", "pseudo_sector": "SIC 49", "label_level": "hybrid"},
                {"ticker": "CpK", "pseudo_sector": "SIC 67", "label_level": "hybrid"},
            ],
            "hybrid",
            PATH,
        )
        self.assertEqual(set(lookup), {"CPK", "CpK"})

    def test_missing_required_schema_names_path_and_found_columns(self):
        for missing in ("ticker", "pseudo_sector", "label_level"):
            columns = {"ticker", "pseudo_sector", "label_level"} - {missing}
            with self.subTest(missing=missing):
                with self.assertRaisesRegex(
                    job.M2ValidationError, f"{missing}.*found columns"
                ):
                    job.validate_pseudo_schema(columns, PATH)

    def test_conflicting_labels_are_blocking(self):
        rows = [
            {"ticker": "ABC", "pseudo_sector": "SIC 60", "label_level": "hybrid"},
            {"ticker": "ABC", "pseudo_sector": "SIC 67", "label_level": "hybrid"},
        ]
        with self.assertRaisesRegex(job.M2ValidationError, "conflicting labels"):
            job.validate_pseudo_records(rows, "hybrid", PATH)

    def test_identical_duplicates_are_deduplicated_and_audited(self):
        row = {"ticker": "ABC", "pseudo_sector": "SIC 60", "label_level": "hybrid"}
        lookup, audit = job.validate_pseudo_records([row, row], "hybrid", PATH)
        self.assertEqual(len(lookup), 1)
        self.assertEqual(audit["duplicate_identical_row_count"], 1)

    def test_conflicting_levels_per_ticker_are_blocking(self):
        rows = [
            {"ticker": "ABC", "pseudo_sector": "SIC 60", "label_level": "hybrid"},
            {"ticker": "ABC", "pseudo_sector": "SIC 60", "label_level": "major"},
        ]
        with self.assertRaisesRegex(job.M2ValidationError, "conflicting label levels"):
            job.validate_pseudo_records(rows, "hybrid", PATH)

    def test_multiple_global_levels_on_distinct_tickers_filter_explicitly(self):
        rows = [
            {"ticker": "ABC", "pseudo_sector": "SIC 60", "label_level": "hybrid"},
            {"ticker": "XYZ", "pseudo_sector": "SIC 20", "label_level": "major"},
        ]
        lookup, audit = job.validate_pseudo_records(rows, "hybrid", PATH)
        self.assertEqual(set(lookup), {"ABC"})
        self.assertEqual(audit["observed_label_levels"], ["hybrid", "major"])

    def test_missing_configured_level_never_falls_back(self):
        rows = [
            {"ticker": "ABC", "pseudo_sector": "SIC 60", "label_level": "major"}
        ]
        with self.assertRaisesRegex(job.M2ValidationError, "hybrid.*absent"):
            job.validate_pseudo_records(rows, "hybrid", PATH)


if __name__ == "__main__":
    unittest.main()
