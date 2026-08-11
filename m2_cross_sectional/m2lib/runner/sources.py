"""Spark input, schema, pseudo-sector, and direct-SIC source handling."""

from __future__ import annotations

from .validation import *  # noqa: F401,F403

def require_pyspark() -> None:
    if SparkSession is None or F is None:
        raise M2ValidationError(
            "PySpark is not installed in this interpreter. Run this job with "
            "spark-submit on the configured Spark/YARN environment."
        )


def hadoop_path_exists(spark: SparkSession, path: str) -> bool:
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(fs.exists(jvm_path))


def read_required_parquet(
    spark: SparkSession, path: str, label: str
) -> DataFrame:
    if not hadoop_path_exists(spark, path):
        raise M2ValidationError(
            f"{label} path does not exist: {path}. Inspect the matching config "
            "field and the upstream M1 HDFS output."
        )
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        raise M2ValidationError(
            f"{label} path exists but is not readable as Parquet: {path}: {exc}"
        ) from exc


def schema_rows(
    frame: DataFrame, dataset: str, required: Iterable[str]
) -> List[Tuple[str, str, str, bool]]:
    required_set = set(required)
    return [
        (dataset, field.name, field.dataType.simpleString(), field.name in required_set)
        for field in frame.schema.fields
    ]


def missing_columns(frame: DataFrame, required: Iterable[str]) -> List[str]:
    return sorted(set(required) - set(frame.columns))


def hadoop_path_status(spark: SparkSession, path: str) -> Dict[str, Any]:
    """Return provenance values exposed by Hadoop without inventing metadata."""
    try:
        jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = jvm_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        status = fs.getFileStatus(jvm_path)
        modified_ms = int(status.getModificationTime())
        modified = dt.datetime.fromtimestamp(
            modified_ms / 1000.0, tz=dt.timezone.utc
        ).isoformat()
        return {
            "source_modification_time_utc": modified,
            "source_path_length_bytes": int(status.getLen()),
        }
    except Exception:
        return {
            "source_modification_time_utc": "unavailable",
            "source_path_length_bytes": -1,
        }


def load_pseudo_sector(
    spark: SparkSession, config: Mapping[str, Any]
) -> Tuple[DataFrame, DataFrame, Dict[str, Any], List[Tuple[str, str, str, bool]]]:
    """Validate and return a unique configured-level pseudo lookup."""
    path = str(config["pseudo_sector_path"])
    level = str(config["pseudo_sector_label_level"]).strip()
    if not hadoop_path_exists(spark, path):
        raise M2ValidationError(
            "Pseudo-sector source is required for M2 V2 but does not exist at "
            f"{path}. Check `hdfs dfs -test -e {path}` and the "
            "pseudo_sector_path config key; run the upstream sector writer "
            "before retrying preflight."
        )
    try:
        raw = spark.read.parquet(path)
    except Exception as exc:
        raise M2ValidationError(
            "Pseudo-sector source exists but is not readable as Parquet at "
            f"{path}: {exc}. Inspect the path with Spark and rebuild the "
            "upstream asset before retrying."
        ) from exc

    validate_pseudo_schema(raw.columns, path)
    pseudo_schema = schema_rows(
        raw, "pseudo_sector", ("ticker", "pseudo_sector", "label_level")
    )
    sec_type = (
        F.trim(F.col("sec_type").cast("string"))
        if "sec_type" in raw.columns
        else F.lit(None).cast("string")
    )
    normalized = (
        raw.select(
            F.trim(F.col("ticker").cast("string")).alias("ticker"),
            F.trim(F.col("pseudo_sector").cast("string")).alias("pseudo_sector"),
            F.trim(F.col("label_level").cast("string")).alias("label_level"),
            sec_type.alias("sec_type"),
        )
        .withColumn(
            "ticker",
            F.when(F.col("ticker") == "", F.lit(None)).otherwise(F.col("ticker")),
        )
        .withColumn(
            "pseudo_sector",
            F.when(F.col("pseudo_sector") == "", F.lit(None)).otherwise(
                F.col("pseudo_sector")
            ),
        )
        .withColumn(
            "label_level",
            F.when(F.col("label_level") == "", F.lit(None)).otherwise(
                F.col("label_level")
            ),
        )
        .withColumn(
            "sec_type",
            F.when(F.col("sec_type") == "", F.lit(None)).otherwise(
                F.col("sec_type")
            ),
        )
        .cache()
    )
    source_rows = normalized.count()
    normalized_distinct_rows = normalized.distinct().count()
    aggregate = normalized.agg(
        F.countDistinct("ticker").alias("source_tickers"),
        F.countDistinct("pseudo_sector").alias("source_labels"),
        F.sum(F.when(F.col("ticker").isNull(), 1).otherwise(0)).alias(
            "blank_ticker_count"
        ),
        F.sum(F.when(F.col("pseudo_sector").isNull(), 1).otherwise(0)).alias(
            "blank_label_count"
        ),
        F.sum(F.when(F.col("label_level").isNull(), 1).otherwise(0)).alias(
            "blank_label_level_count"
        ),
    ).collect()[0]
    observed_levels = [
        row["label_level"]
        for row in normalized.filter(F.col("label_level").isNotNull())
        .select("label_level")
        .distinct()
        .orderBy("label_level")
        .collect()
    ]
    if level not in observed_levels:
        raise M2ValidationError(
            f"Configured pseudo-sector label level {level!r} is absent at {path}; "
            f"observed levels={observed_levels}. Inspect with Spark groupBy"
            "('label_level').count(), then rerun the upstream writer for the "
            "configured level. M2 will not fall back to another level."
        )

    label_conflicts = (
        normalized.filter(
            F.col("ticker").isNotNull() & F.col("pseudo_sector").isNotNull()
        )
        .groupBy("ticker")
        .agg(
            F.countDistinct("pseudo_sector").alias("value_count"),
            F.sort_array(F.collect_set("pseudo_sector")).alias("values"),
        )
        .filter(F.col("value_count") > 1)
        .cache()
    )
    level_conflicts = (
        normalized.filter(
            F.col("ticker").isNotNull() & F.col("label_level").isNotNull()
        )
        .groupBy("ticker")
        .agg(
            F.countDistinct("label_level").alias("value_count"),
            F.sort_array(F.collect_set("label_level")).alias("values"),
        )
        .filter(F.col("value_count") > 1)
        .cache()
    )
    label_conflict_count = label_conflicts.count()
    level_conflict_count = level_conflicts.count()
    if label_conflict_count or level_conflict_count:
        label_examples = [row.asDict() for row in label_conflicts.limit(20).collect()]
        level_examples = [row.asDict() for row in level_conflicts.limit(20).collect()]
        raise M2ValidationError(
            "Pseudo-sector source conflicts are blocking at "
            f"{path}: conflicting_label_tickers={label_conflict_count} "
            f"examples={label_examples}; conflicting_level_tickers="
            f"{level_conflict_count} examples={level_examples}. Resolve the "
            "upstream one-label/one-level-per-ticker contract and retry with a "
            "new run ID if any output exists."
        )

    configured = normalized.filter(
        (F.col("label_level") == F.lit(level))
        & F.col("ticker").isNotNull()
        & F.col("pseudo_sector").isNotNull()
    )
    configured_rows = configured.count()
    lookup = configured.dropDuplicates(
        ["ticker", "pseudo_sector", "label_level", "sec_type"]
    ).cache()
    configured_distinct_rows = lookup.count()
    lookup_tickers = lookup.select("ticker").distinct().count()
    if configured_distinct_rows != lookup_tickers:
        raise M2ValidationError(
            "Configured-level pseudo-sector source is not unique by ticker at "
            f"{path}: configured_distinct_rows={configured_distinct_rows}, "
            f"configured_tickers={lookup_tickers}. Inspect duplicate tickers and "
            "rebuild the source before retrying."
        )
    if lookup_tickers == 0:
        raise M2ValidationError(
            f"Pseudo-sector source {path} has zero usable rows for configured "
            f"label level {level!r}; rebuild the upstream source before retrying."
        )

    status = hadoop_path_status(spark, path)
    info: Dict[str, Any] = {
        "pseudo_sector_path": path,
        "configured_label_level": level,
        "observed_label_levels": observed_levels,
        "observed_label_levels_json": json.dumps(observed_levels),
        "source_rows": int(source_rows),
        "source_tickers": int(aggregate["source_tickers"] or 0),
        "source_labels": int(aggregate["source_labels"] or 0),
        "blank_ticker_count": int(aggregate["blank_ticker_count"] or 0),
        "blank_label_count": int(aggregate["blank_label_count"] or 0),
        "blank_label_level_count": int(
            aggregate["blank_label_level_count"] or 0
        ),
        "duplicate_identical_row_count": int(
            source_rows - normalized_distinct_rows
        ),
        "configured_level_rows": int(configured_rows),
        "configured_level_distinct_rows": int(configured_distinct_rows),
        "configured_level_tickers": int(lookup_tickers),
        "conflicting_ticker_count": int(label_conflict_count),
        "conflicting_level_ticker_count": int(level_conflict_count),
        "model_version_available": False,
        "prediction_confidence_available": False,
        "training_timestamp_available": False,
        "upstream_documented_model_metrics": UPSTREAM_MODEL_METRICS,
        "source_schema_json": json.dumps(
            {field.name: field.dataType.simpleString() for field in raw.schema.fields},
            sort_keys=True,
        ),
        **status,
    }
    contract = spark.createDataFrame([info])
    lookup = lookup.select(
        F.col("ticker").alias("_pseudo_join_ticker"),
        "pseudo_sector",
        "label_level",
        F.col("sec_type").alias("pseudo_sec_type"),
    )
    return lookup, contract, info, pseudo_schema


def normalize_sic(column: Any) -> Any:
    trimmed = F.trim(column.cast("string"))
    return (
        F.when(trimmed.isNull() | (trimmed == ""), F.lit(None))
        .when(F.upper(trimmed) == F.lit("UNKNOWN"), F.lit("UNKNOWN"))
        .otherwise(trimmed)
    )


def enrich_sic_description(
    spark: SparkSession,
    grain: DataFrame,
    config: Mapping[str, Any],
    issues: List[str],
) -> Tuple[DataFrame, Dict[str, Any], List[Tuple[str, str, str, bool]]]:
    sic_column = str(config["sic_description_column"])
    if sic_column in grain.columns:
        enriched = grain.withColumn(sic_column, normalize_sic(F.col(sic_column)))
        enriched = enriched.withColumn(
            sic_column,
            F.coalesce(F.col(sic_column), F.lit("UNKNOWN")),
        )
        return (
            enriched,
            {
                "mode": "grain",
                "conflicting_ticker_count": 0,
                "events_blocked_by_list_date": 0,
                "metadata_non_null_sic_count": None,
            },
            [],
        )

    metadata_path = str(config["metadata_path"])
    if not hadoop_path_exists(spark, metadata_path):
        raise M2ValidationError(
            "The grain has no sic_description and metadata_path does not exist: "
            f"{metadata_path}. Inspect metadata_path and the M1 reference upload."
        )
    try:
        metadata_raw = spark.read.json(metadata_path)
    except Exception as exc:
        raise M2ValidationError(
            f"metadata_path is not readable as JSON: {metadata_path}: {exc}"
        ) from exc

    metadata_schema = schema_rows(
        metadata_raw, "metadata", METADATA_REQUIRED_COLUMNS
    )
    missing = missing_columns(metadata_raw, METADATA_REQUIRED_COLUMNS)
    if missing:
        raise M2ValidationError(
            "Metadata schema is missing required fields "
            f"{missing}; found {sorted(metadata_raw.columns)}. Inspect "
            "ticker/gen_ticker_metadata.py and metadata_path."
        )

    metadata = (
        metadata_raw.select(
            F.col("ticker").cast("string").alias("ticker"),
            F.col("active").alias("active"),
            F.to_date("list_date").alias("list_date"),
            normalize_sic(F.col("sic_description")).alias("sic_description"),
        )
        .filter(F.col("ticker").isNotNull())
        .cache()
    )

    non_null_sic_count = metadata.filter(
        F.col("sic_description").isNotNull()
    ).count()
    if non_null_sic_count == 0:
        issues.append(
            "metadata_path exposes sic_description but contains no non-null values"
        )

    description_counts = (
        metadata.filter(F.col("sic_description").isNotNull())
        .groupBy("ticker")
        .agg(
            F.countDistinct("sic_description").alias("sic_value_count"),
            F.sort_array(F.collect_set("sic_description")).alias("sic_values"),
        )
        .cache()
    )
    conflicts = description_counts.filter(F.col("sic_value_count") > 1).cache()
    conflict_count = conflicts.count()
    if conflict_count:
        examples = [
            f"{row['ticker']}: {row['sic_values']}"
            for row in conflicts.orderBy("ticker").limit(50).collect()
        ]
        print("=== conflicting metadata SIC descriptions (up to 50) ===")
        for example in examples:
            print(example)
        issues.append(
            f"{conflict_count} ticker(s) have conflicting non-null "
            "sic_description values; resolve metadata conflicts upstream"
        )

    unambiguous = description_counts.filter(F.col("sic_value_count") == 1).select(
        "ticker"
    )
    candidates = (
        metadata.filter(F.col("sic_description").isNotNull())
        .join(unambiguous, "ticker", "inner")
        .groupBy("ticker", "sic_description")
        .agg(F.min("list_date").alias("sic_list_date"))
    )

    joined = grain.join(F.broadcast(candidates), "ticker", "left")
    blocked_condition = (
        F.col("sic_description").isNotNull()
        & F.col("sic_list_date").isNotNull()
        & (F.col("ex_date") < F.col("sic_list_date"))
    )
    blocked_count = joined.filter(blocked_condition).count()
    enriched = (
        joined.withColumn(
            "sic_description",
            F.when(blocked_condition, F.lit("UNKNOWN"))
            .otherwise(F.col("sic_description"))
            .cast("string"),
        )
        .withColumn(
            "sic_description",
            F.coalesce(F.col("sic_description"), F.lit("UNKNOWN")),
        )
        .drop("sic_list_date")
    )
    return (
        enriched,
        {
            "mode": "metadata",
            "conflicting_ticker_count": conflict_count,
            "events_blocked_by_list_date": blocked_count,
            "metadata_non_null_sic_count": non_null_sic_count,
        },
        metadata_schema,
    )
