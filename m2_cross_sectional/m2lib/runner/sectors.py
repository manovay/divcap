"""Analysis-base filtering and deliberately separate sector states."""

from __future__ import annotations

from .metrics import *  # noqa: F401,F403

def base_condition(market_ticker: str) -> Any:
    return (
        (F.col("cash_amount") > 0)
        & (F.col("has_core") == F.lit(True))
        & (F.col("window_contiguous") == F.lit(True))
        & (F.col("ticker") != F.lit(market_ticker))
    )


def enrich_sector_states(
    spark: SparkSession,
    base: DataFrame,
    pseudo_lookup: DataFrame,
    pseudo_info: Mapping[str, Any],
) -> Tuple[DataFrame, DataFrame, DataFrame, Dict[str, Any]]:
    """Join pseudo labels many-to-one and create disjoint coverage states."""
    before_rows = base.count()
    base_tickers = base.select("ticker").distinct().count()
    noncanonical_tickers = base.filter(
        F.col("ticker").isNull()
        | (F.col("ticker") != F.upper(F.trim(F.col("ticker").cast("string"))))
    ).count()
    if noncanonical_tickers:
        raise M2ValidationError(
            "Event grain ticker normalization is incompatible with the pseudo "
            f"join: {noncanonical_tickers} base row(s) are null, untrimmed, or "
            "not uppercase. Normalize the upstream grain ticker key and retry."
        )
    joined = (
        base.withColumn(
            "_pseudo_join_ticker",
            F.upper(F.trim(F.col("ticker").cast("string"))),
        )
        .join(F.broadcast(pseudo_lookup), "_pseudo_join_ticker", "left")
        .drop("_pseudo_join_ticker")
        .cache()
    )
    after_rows = joined.count()
    join_row_delta = int(after_rows - before_rows)
    if join_row_delta:
        raise M2ValidationError(
            "Pseudo-sector many-to-one join changed the event grain: "
            f"before={before_rows}, after={after_rows}, delta={join_row_delta}. "
            "Inspect duplicate pseudo tickers and do not write final output."
        )

    valid_pseudo = F.coalesce(
        F.col("pseudo_sector").isNotNull()
        & (F.col("label_level") == F.lit(pseudo_info["configured_label_level"])),
        F.lit(False),
    )
    enriched = (
        joined.withColumn(
            "direct_sic_known", F.col("sic_description") != F.lit("UNKNOWN")
        )
        .withColumn(
            "direct_sic_unknown", F.col("sic_description") == F.lit("UNKNOWN")
        )
        .withColumn(
            "pseudo_recovered", F.col("direct_sic_unknown") & valid_pseudo
        )
        .withColumn(
            "still_unresolved", F.col("direct_sic_unknown") & (~valid_pseudo)
        )
        .withColumn(
            "sector_state",
            F.when(F.col("direct_sic_known"), F.lit("direct_sic_known"))
            .when(F.col("pseudo_recovered"), F.lit("pseudo_recovered"))
            .otherwise(F.lit("still_unresolved")),
        )
        .withColumn(
            "sector_source",
            F.when(
                F.col("direct_sic_known"), F.lit("direct_sic_current_reference")
            )
            .when(
                F.col("pseudo_recovered"),
                F.lit("model_predicted_pseudo_sector"),
            )
            .otherwise(F.lit("unresolved")),
        )
        .cache()
    )
    overlap_count = enriched.filter(
        F.col("direct_sic_known") & F.col("pseudo_recovered")
    ).count()
    if overlap_count:
        raise M2ValidationError(
            "Direct-SIC-known and pseudo-recovered populations overlap by "
            f"{overlap_count} event(s); this violates taxonomy separation."
        )

    aggregate = enriched.agg(
        F.sum(F.col("direct_sic_known").cast("long")).alias("direct_known_events"),
        F.sum(F.col("direct_sic_unknown").cast("long")).alias(
            "direct_unknown_events"
        ),
        F.sum(F.col("pseudo_recovered").cast("long")).alias(
            "pseudo_recovered_events"
        ),
        F.sum(F.col("still_unresolved").cast("long")).alias(
            "still_unresolved_events"
        ),
        F.sum(
            F.when(F.col("direct_sic_known") & valid_pseudo, 1).otherwise(0)
        ).alias("pseudo_matches_on_direct_known_events"),
    ).collect()[0]
    counts: Dict[str, Any] = {
        "base_events": int(before_rows),
        "base_tickers": int(base_tickers),
        "direct_known_events": int(aggregate["direct_known_events"] or 0),
        "direct_unknown_events": int(aggregate["direct_unknown_events"] or 0),
        "pseudo_recovered_events": int(
            aggregate["pseudo_recovered_events"] or 0
        ),
        "still_unresolved_events": int(
            aggregate["still_unresolved_events"] or 0
        ),
        "pseudo_matches_on_direct_known_events": int(
            aggregate["pseudo_matches_on_direct_known_events"] or 0
        ),
        "join_row_delta": join_row_delta,
    }
    reconcile_sector_counts(
        base_events=counts["base_events"],
        direct_known_events=counts["direct_known_events"],
        direct_unknown_events=counts["direct_unknown_events"],
        pseudo_recovered_events=counts["pseudo_recovered_events"],
        still_unresolved_events=counts["still_unresolved_events"],
    )
    if counts["pseudo_recovered_events"] == 0:
        raise M2ValidationError(
            "The configured pseudo-sector source recovered zero direct-SIC-"
            "unknown base events. Verify ticker normalization, label level, "
            "source coverage, and the base date/universe before retrying; zero "
            "recovery is a blocking V2 source/data issue."
        )

    ticker_rows = {
        row["sector_state"]: int(row["n_tickers"])
        for row in enriched.groupBy("sector_state")
        .agg(F.countDistinct("ticker").alias("n_tickers"))
        .collect()
    }
    counts.update(
        {
            "direct_known_tickers": ticker_rows.get("direct_sic_known", 0),
            "pseudo_recovered_tickers": ticker_rows.get("pseudo_recovered", 0),
            "still_unresolved_tickers": ticker_rows.get("still_unresolved", 0),
        }
    )
    unknown = counts["direct_unknown_events"]
    base_events = counts["base_events"]
    recovery_rate = (
        counts["pseudo_recovered_events"] / unknown if unknown else None
    )
    coverage_after = (
        (counts["direct_known_events"] + counts["pseudo_recovered_events"])
        / base_events
        if base_events
        else None
    )
    counts["pseudo_recovery_rate_of_sic_unknown"] = recovery_rate
    counts["coverage_after_recovery_share"] = coverage_after

    bridge_specs = (
        (
            1,
            "direct_sic_known",
            counts["direct_known_events"],
            counts["direct_known_tickers"],
            "direct/current-reference SIC",
        ),
        (
            2,
            "pseudo_recovered",
            counts["pseudo_recovered_events"],
            counts["pseudo_recovered_tickers"],
            "model-predicted pseudo-sector on direct-SIC unknown only",
        ),
        (
            3,
            "still_unresolved",
            counts["still_unresolved_events"],
            counts["still_unresolved_tickers"],
            "no usable direct SIC or configured-level pseudo-sector",
        ),
    )
    bridge_rows = [
        (
            order,
            state,
            int(events),
            float(events / base_events) if base_events else None,
            int(tickers),
            float(tickers / base_tickers) if base_tickers else None,
            source,
        )
        for order, state, events, tickers, source in bridge_specs
    ]
    bridge = spark.createDataFrame(
        bridge_rows,
        "state_order integer, sector_state string, n_events long, "
        "event_share_of_base double, n_tickers long, ticker_share_of_base double, "
        "label_source string",
    )

    coverage_row = {
        "pseudo_sector_path": str(pseudo_info["pseudo_sector_path"]),
        "configured_label_level": str(pseudo_info["configured_label_level"]),
        "observed_label_levels": str(pseudo_info["observed_label_levels_json"]),
        "source_rows": int(pseudo_info["source_rows"]),
        "source_tickers": int(pseudo_info["source_tickers"]),
        "source_labels": int(pseudo_info["source_labels"]),
        "conflicting_ticker_count": int(pseudo_info["conflicting_ticker_count"]),
        "conflicting_level_ticker_count": int(
            pseudo_info["conflicting_level_ticker_count"]
        ),
        "blank_ticker_count": int(pseudo_info["blank_ticker_count"]),
        "blank_label_count": int(pseudo_info["blank_label_count"]),
        "blank_label_level_count": int(
            pseudo_info["blank_label_level_count"]
        ),
        "duplicate_identical_row_count": int(
            pseudo_info["duplicate_identical_row_count"]
        ),
        "base_events": counts["base_events"],
        "base_tickers": counts["base_tickers"],
        "direct_sic_known_events": counts["direct_known_events"],
        "direct_sic_unknown_events": counts["direct_unknown_events"],
        "pseudo_recovered_events": counts["pseudo_recovered_events"],
        "still_unresolved_events": counts["still_unresolved_events"],
        "pseudo_recovery_rate_of_sic_unknown": recovery_rate,
        "coverage_after_recovery_share": coverage_after,
        "pseudo_recovered_tickers": counts["pseudo_recovered_tickers"],
        "still_unresolved_tickers": counts["still_unresolved_tickers"],
        "pseudo_matches_on_direct_sic_known_events": counts[
            "pseudo_matches_on_direct_known_events"
        ],
        "join_row_delta": join_row_delta,
        "event_reconciliation_passed": True,
        "ticker_reconciliation_note": (
            "Ticker state counts need not be additive because a ticker's events "
            "may span direct-known and unknown states under the listing-date guard."
        ),
    }
    coverage = spark.createDataFrame([coverage_row])
    return enriched, coverage, bridge, counts
