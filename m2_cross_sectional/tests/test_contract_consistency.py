import json
from pathlib import Path
import sys
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import m2_contract as contract  # noqa: E402


class ContractConsistencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.readme = (MODULE_DIR / "README.md").read_text(encoding="utf-8")
        runbook_path = (
            MODULE_DIR
            / ".m2_cross_sectional_build_package_2"
            / "operator_handoff"
            / "MANUAL_TERMINAL_RUNBOOK.md"
        )
        cls.runbook = (
            runbook_path.read_text(encoding="utf-8")
            if runbook_path.is_file()
            else None
        )
        cls.config = json.loads(
            (MODULE_DIR / "cross_sectional_config.json").read_text(encoding="utf-8")
        )

    def test_config_keys_match_code_and_readme(self):
        self.assertEqual(set(self.config), set(contract.REQUIRED_CONFIG_KEYS))
        for key in contract.REQUIRED_CONFIG_KEYS:
            with self.subTest(key=key):
                self.assertIn(f"`{key}`", self.readme)

    def test_cli_flags_match_code_readme_and_runbook(self):
        runner_source = (MODULE_DIR / "run_cross_sectional.py").read_text(
            encoding="utf-8"
        )
        report_source = (MODULE_DIR / "make_report_artifacts.py").read_text(
            encoding="utf-8"
        )
        for flag in ("--config", "--run-id", "--mode"):
            self.assertIn(flag, runner_source)
            self.assertIn(flag, self.readme)
            if self.runbook is not None:
                self.assertIn(flag, self.runbook)
        for flag in ("--config", "--run-id", "--output-dir"):
            self.assertIn(flag, report_source)
            self.assertIn(flag, self.readme)
            if self.runbook is not None:
                self.assertIn(flag, self.runbook)

    def test_output_and_report_registries_are_documented(self):
        for relative in contract.OUTPUT_RELATIVE_PATHS.values():
            with self.subTest(relative=relative):
                self.assertIn(relative.split("/")[-1], self.readme)
                if self.runbook is not None:
                    self.assertIn(relative, self.runbook)
        for filename in contract.CSV_OUTPUTS.values():
            self.assertIn(filename, self.readme)
        for filename in contract.FIGURE_OUTPUTS.values():
            self.assertIn(filename, self.readme)

    def test_no_combined_sector_performance_field_in_runtime(self):
        runtime_paths = [
            MODULE_DIR / "run_cross_sectional.py",
            MODULE_DIR / "make_report_artifacts.py",
            *(MODULE_DIR / "m2lib").rglob("*.py"),
        ]
        runtime = "\n".join(
            path.read_text(encoding="utf-8") for path in runtime_paths
        ).lower()
        self.assertNotIn("sector_final", runtime)
        self.assertNotIn("blended_sector", runtime)
        self.assertNotIn("coalesce(sic_description", runtime)

    def test_implementation_modules_remain_near_400_lines(self):
        for path in (MODULE_DIR / "m2lib").rglob("*.py"):
            with self.subTest(module=path.name):
                line_count = len(path.read_text(encoding="utf-8").splitlines())
                self.assertLessEqual(line_count, 425)
        for name in ("run_cross_sectional.py", "make_report_artifacts.py"):
            with self.subTest(entrypoint=name):
                line_count = len(
                    (MODULE_DIR / name).read_text(encoding="utf-8").splitlines()
                )
                self.assertLessEqual(line_count, 100)

    def test_runner_and_report_use_separate_packages(self):
        library = MODULE_DIR / "m2lib"
        self.assertEqual(
            [path.name for path in library.glob("*.py")],
            ["__init__.py"],
        )
        for package in ("runner", "report"):
            with self.subTest(package=package):
                package_dir = library / package
                self.assertTrue((package_dir / "__init__.py").is_file())
                self.assertGreater(len(list(package_dir.glob("*.py"))), 1)


if __name__ == "__main__":
    unittest.main()
