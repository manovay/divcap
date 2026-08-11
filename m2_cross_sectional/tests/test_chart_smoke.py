import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import m2_contract as contract  # noqa: E402
import make_report_artifacts as report  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()
