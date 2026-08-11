#!/usr/bin/env python3
"""Run the M2 V2 cross-sectional Spark build.

Implementation lives in :mod:`m2lib`; this stable entrypoint keeps the documented
``spark-submit`` command and public helper imports backward compatible.
"""

from __future__ import annotations

from m2lib.runner.pipeline import *  # noqa: F401,F403


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the run-versioned M2 cross-sectional analysis."
    )
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--run-id", required=True, help="Unique immutable run ID")
    parser.add_argument("--mode", required=True, choices=("preflight", "final"))
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    spark = None
    try:
        args = parse_args(argv)
        validate_run_id(args.run_id)
        config = resolve_runtime_config(load_config(args.config))
        require_pyspark()
        spark = (
            SparkSession.builder.appName(f"divcap-m2-cross-sectional-{args.run_id}")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", "64")
        run_job(spark, config, args.run_id, args.mode)
        return 0
    except M2ValidationError as exc:
        print(f"M2 VALIDATION ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
