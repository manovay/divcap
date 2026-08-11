import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import m2_contract as contract  # noqa: E402
import make_report_artifacts as report  # noqa: E402
import m2lib.report.figures as figure_module  # noqa: E402
from m2lib.report.labels import (  # noqa: E402
    boundary_range,
    business_sector_label,
    event_offset_label,
    funnel_stage_label,
    quantile_label,
)
from v2_fixtures import config, frames  # noqa: E402


HAS_MATPLOTLIB = importlib.util.find_spec("matplotlib") is not None


@unittest.skipUnless(
    HAS_MATPLOTLIB,
    "matplotlib is not installed in the local interpreter; run on the report runtime",
)
class ChartSmokeTests(unittest.TestCase):
    def test_all_figures_render_with_numeric_labels_and_close(self):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "figures").mkdir()
            report.write_figures(plt, frames(), config(), output)
            for filename in contract.FIGURE_OUTPUTS.values():
                path = output / "figures" / filename
                self.assertTrue(path.exists(), filename)
                self.assertGreater(path.stat().st_size, 0, filename)
            self.assertEqual(plt.get_fignums(), [])

    def test_large_category_fixture_has_dynamic_height_and_empty_is_controlled(self):
        import pandas as pd

        source = frames()["pseudo_sector_summary"]
        empty = source.copy()
        empty["report_eligible_flag"] = False
        selected = report.select_sector_categories(
            empty, "pseudo_sector", "pseudo_sector", 20
        )
        self.assertTrue(selected.empty)

    def test_canonical_codes_have_business_friendly_display_labels(self):
        self.assertEqual(
            business_sector_label("SIC 73", "pseudo_sector"),
            "Business Services (SIC 73)",
        )
        self.assertEqual(
            business_sector_label("SIC 60", "pseudo_sector"),
            "Depository Institutions (SIC 60)",
        )
        self.assertEqual(
            business_sector_label("Manufacturing (other)", "pseudo_sector"),
            "Other Manufacturing",
        )
        self.assertEqual(
            business_sector_label("SERVICES-BUSINESS SERVICES, NEC", "sic_description"),
            "Services-Business Services, NEC",
        )
        self.assertEqual(funnel_stage_label("has_core"), "Complete core return data")
        self.assertEqual(quantile_label("Q01"), "Lowest 20%\n(Q01)")
        self.assertEqual(event_offset_label(0), "Ex-date")
        self.assertEqual(event_offset_label(-2), "2 days\nbefore")
        self.assertEqual(boundary_range(None, 0.0025, "yield"), "up to 0.250%")
        self.assertEqual(boundary_range(1_000_000, None, "liquidity"), "above $1.00M")

    def test_sector_values_use_a_clear_column_beyond_the_intervals(self):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        selected = report.select_sector_categories(
            frames()["pseudo_sector_summary"],
            "pseudo_sector",
            "pseudo_sector",
            20,
        )
        captured = {}

        def capture(_plt, figure, _path, **kwargs):
            captured["figure"] = figure
            captured["layout_rect"] = kwargs.get("layout_rect")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sector.png"
            with patch.object(figure_module, "finish_figure", side_effect=capture):
                figure_module.sector_performance_figure(
                    plt,
                    selected,
                    "pseudo_sector",
                    "Model-Estimated Industry Capture Return\n"
                    "Model-predicted pseudo-sector; recovered only; label_level=hybrid",
                    "#7A5195",
                    path,
                    True,
                )

        figure = captured["figure"]
        axis = figure.axes[0]
        tick_labels = [item.get_text() for item in axis.get_yticklabels()]
        value_labels = [item for item in axis.texts if " events | " in item.get_text()]
        interval_high = (
            selected["ci95_high_capture_ret_abn"].astype(float) * 10000.0
        ).max()
        self.assertIn("Depository Institutions (SIC 60)", tick_labels)
        self.assertTrue(value_labels)
        self.assertTrue(all(item.get_position()[0] > interval_high for item in value_labels))
        self.assertTrue(all("companies" in item.get_text() for item in value_labels))
        self.assertTrue(any("Avg -2.00 bps" in item.get_text() for item in value_labels))
        self.assertNotIn("F09", axis.get_title())
        self.assertEqual(captured["layout_rect"], (0.0, 0.045, 1.0, 0.97))
        plt.close(figure)


if __name__ == "__main__":
    unittest.main()
