"""Derived metrics, identity checks, and sample-funnel construction."""

from __future__ import annotations

from .sources import *  # noqa: F401,F403

def add_derived_fields(grain: DataFrame) -> DataFrame:
    declaration = (
        F.to_date("declaration_date")
        if "declaration_date" in grain.columns
        else F.lit(None).cast("date")
    )
    return (
        grain.withColumn("ex_date", F.to_date("ex_date"))
        .withColumn(
            "event_id",
            F.concat_ws("|", F.col("ticker"), F.date_format("ex_date", "yyyy-MM-dd")),
        )
        .withColumn("event_month", F.month("ex_date"))
        .withColumn(
            "declaration_lead_days", F.datediff(F.col("ex_date"), declaration)
        )
        .withColumn(
            "stock_overnight_ret", F.col("ex_open") / F.col("prev_close") - 1.0
        )
        .withColumn(
            "abn_overnight_ret",
            F.col("stock_overnight_ret") - F.col("mkt_overnight_ret"),
        )
        .withColumn(
            "ex_ym",
            F.coalesce(
                F.col("ex_ym") if "ex_ym" in grain.columns else F.lit(None),
                F.date_format("ex_date", "yyyy-MM"),
            ),
        )
    )


def metric_identity_summary(
    spark: SparkSession,
    grain: DataFrame,
    tolerance: float,
    issues: List[str],
) -> DataFrame:
    specs = (
        (
            "drop_pct_equals_drop_ratio_times_div_yield",
            F.col("drop_pct"),
            F.col("drop_ratio") * F.col("div_yield"),
        ),
        (
            "capture_ret_equals_div_yield_minus_drop_pct",
            F.col("capture_ret"),
            F.col("div_yield") - F.col("drop_pct"),
        ),
        (
            "capture_ret_abn_equals_capture_ret_minus_market",
            F.col("capture_ret_abn"),
            F.col("capture_ret") - F.col("mkt_overnight_ret"),
        ),
        (
            "stock_overnight_ret_equals_negative_drop_pct",
            F.col("stock_overnight_ret"),
            -F.col("drop_pct"),
        ),
        (
            "abn_overnight_ret_equals_stock_minus_market",
            F.col("abn_overnight_ret"),
            F.col("stock_overnight_ret") - F.col("mkt_overnight_ret"),
        ),
    )
    expressions = []
    for name, left, right in specs:
        valid = left.isNotNull() & right.isNotNull()
        difference = F.abs(left - right)
        expressions.extend(
            [
                F.sum(F.when(valid, 1).otherwise(0)).alias(f"{name}__eligible"),
                F.sum(
                    F.when(valid & (difference > F.lit(tolerance)), 1).otherwise(0)
                ).alias(f"{name}__violations"),
                F.max(F.when(valid, difference)).alias(f"{name}__max_error"),
            ]
        )
    values = grain.agg(*expressions).collect()[0].asDict()
    rows = []
    for name, _, _ in specs:
        eligible = int(values[f"{name}__eligible"] or 0)
        violations = int(values[f"{name}__violations"] or 0)
        max_error = values[f"{name}__max_error"]
        rows.append((name, eligible, violations, max_error, float(tolerance)))
        if eligible == 0:
            issues.append(f"metric identity {name} has zero eligible rows")
        if violations:
            issues.append(
                f"metric identity {name} has {violations} violation(s) above "
                f"tolerance {tolerance}"
            )
    return spark.createDataFrame(
        rows,
        "identity string, eligible_rows long, violation_rows long, "
        "max_abs_error double, tolerance double",
    )


def sample_conditions(
    grain: DataFrame, market_ticker: str
) -> List[Tuple[str, Any, str]]:
    cash_positive = F.col("cash_amount") > 0
    core = cash_positive & (F.col("has_core") == F.lit(True))
    contiguous = core & (F.col("window_contiguous") == F.lit(True))
    base = contiguous & (F.col("ticker") != F.lit(market_ticker))
    conditions = [
        ("raw", F.lit(True), "All event-grain rows"),
        ("cash_positive", cash_positive, "cash_amount > 0"),
        ("has_core", core, "cash positive and has_core = true"),
        (
            "window_contiguous",
            contiguous,
            "cash positive, has_core, and window_contiguous = true",
        ),
        (
            "benchmark_excluded",
            base,
            f"Base conditions with benchmark ticker {market_ticker} excluded",
        ),
        ("yield_usable", base & F.col("div_yield").isNotNull(), "Base plus div_yield"),
        (
            "volatility_usable",
            base & F.col("pre_vol").isNotNull(),
            "Base plus pre_vol",
        ),
        (
            "liquidity_usable",
            base & F.col("pre_avg_dollar_volume").isNotNull(),
            "Base plus pre_avg_dollar_volume",
        ),
        (
            "sic_known",
            base & (F.col("sic_description") != "UNKNOWN"),
            "Base with known sic_description",
        ),
        (
            "sic_unknown",
            base & (F.col("sic_description") == "UNKNOWN"),
            "Base with UNKNOWN sic_description",
        ),
        (
            "full_event_time_window",
            base & (F.col("n_bars") == 9),
            "Base with n_bars = 9",
        ),
    ]
    if "sector_state" in grain.columns:
        conditions.extend(
            [
                (
                    "pseudo_recovered",
                    base & (F.col("sector_state") == "pseudo_recovered"),
                    "Base direct-SIC-unknown events with a valid configured-level "
                    "model-predicted pseudo-sector match",
                ),
                (
                    "still_unresolved",
                    base & (F.col("sector_state") == "still_unresolved"),
                    "Base direct-SIC-unknown events without a valid pseudo-sector "
                    "match",
                ),
            ]
        )
    return conditions


def build_sample_funnel(
    spark: SparkSession, grain: DataFrame, market_ticker: str
) -> Tuple[DataFrame, Dict[str, int]]:
    conditions = sample_conditions(grain, market_ticker)
    aggregate = grain.agg(
        *[
            F.sum(F.when(condition, 1).otherwise(0)).alias(name)
            for name, condition, _ in conditions
        ]
    ).collect()[0]
    counts = {name: int(aggregate[name] or 0) for name, _, _ in conditions}
    raw_count = counts["raw"]
    base_count = counts["benchmark_excluded"]
    sequential = {
        "raw",
        "cash_positive",
        "has_core",
        "window_contiguous",
        "benchmark_excluded",
    }
    rows = []
    prior_filter_count = raw_count
    for index, (name, _, description) in enumerate(conditions):
        count = counts[name]
        if name in sequential:
            prior = raw_count if name == "raw" else prior_filter_count
            stage_type = "sequential_filter"
            prior_filter_count = count
        else:
            prior = base_count
            stage_type = "analysis_population"
        retention = float(count / prior) if prior else None
        cumulative = float(count / raw_count) if raw_count else None
        rows.append(
            (
                index + 1,
                name,
                stage_type,
                int(count),
                int(prior),
                retention,
                cumulative,
                description,
            )
        )
    return (
        spark.createDataFrame(
            rows,
            "stage_order integer, stage string, stage_type string, n_events long, "
            "prior_stage_n_events long, retention_from_prior double, "
            "cumulative_retention double, definition string",
        ),
        counts,
    )
