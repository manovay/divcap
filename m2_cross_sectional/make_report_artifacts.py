#!/usr/bin/env python3
"""Create the immutable M2 V2 report package from compact Spark aggregates.

Implementation lives in :mod:`m2lib`; this stable entrypoint keeps the documented
``spark-submit`` command and public helper imports backward compatible.
"""

from __future__ import annotations

from m2lib.report.output import *  # noqa: F401,F403


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the immutable M2 V2 local report package."
    )
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--run-id", required=True, help="Accepted M2 run ID")
    parser.add_argument(
        "--output-dir", required=True, help="New local report output directory"
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    spark = None
    try:
        args = parse_args(argv)
        validate_run_id(args.run_id)
        config = resolve_runtime_config(load_config(args.config))
        SparkSession, pd, plt = import_runtime_dependencies()
        spark = (
            SparkSession.builder.appName(f"divcap-m2-v2-report-{args.run_id}")
            .getOrCreate()
        )
        run_root = f"{str(config['output_root']).rstrip('/')}/{args.run_id}"
        tables = load_tables(spark, run_root)
        frames = sorted_pandas_tables(tables)
        validate_report_reconciliation(frames, config, args.run_id, run_root)
        output_dir = Path(args.output_dir)
        prepare_output_directory(output_dir)
        write_csv_outputs(frames, output_dir)
        write_figures(plt, frames, config, output_dir)
        write_report_metrics(pd, frames, args.run_id, output_dir)
        write_insights(pd, frames, config, args.run_id, output_dir)
        write_results_readme(frames, args.run_id, run_root, output_dir)
        verify_local_artifacts(output_dir)
        print(f"M2 V2 report artifacts written to {output_dir}")
        return 0
    except ReportArtifactError as exc:
        print(f"REPORT ARTIFACT ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
