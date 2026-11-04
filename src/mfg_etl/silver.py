"""Silver: validate, quarantine, deduplicate and enrich.

The transformation logic is written as plain DataFrame -> DataFrame functions, so the
same code is unit-tested on small batch DataFrames and run inside streaming queries.

Design notes
  * Every invalid row gets a reason code and goes to a quarantine Delta table;
    nothing is silently dropped.
  * Telemetry and production quarantine are separate tables because their schemas differ.
  * Deduplication uses dropDuplicatesWithinWatermark(event_id) with a 15-minute
    event-time watermark: state for an event_id is kept only while it can still
    reasonably arrive again, so streaming state does not grow forever.
  * Stream-static LEFT joins keep events whose machine/product is missing from master
    data. Their alert columns are NULL (unknown), not False, and master_data_found
    is False so they can be tracked.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from mfg_etl.config import Paths
from mfg_etl.schemas import VALID_STATUS_CODES

WATERMARK = "15 minutes"


def _to_timestamp(col_name: str) -> Column:
    # try_to_timestamp returns NULL for an unparsable value instead of failing the
    # whole batch when ANSI mode is on (the default on newer Databricks runtimes).
    return F.expr(f"try_to_timestamp({col_name})")


def _first_failing_rule(rules: list[tuple[Column, str]]) -> Column:
    expr = F.when(rules[0][0], F.lit(rules[0][1]))
    for condition, reason in rules[1:]:
        expr = expr.when(condition, F.lit(reason))
    return expr


def _negative_or_null(name: str) -> Column:
    return F.col(name).isNull() | (F.col(name) < 0)


# --------------------------------------------------------------------------- telemetry
def add_telemetry_quality(df: DataFrame) -> DataFrame:
    """Parse the timestamp and attach the first failed rule (NULL = row is valid)."""
    df = df.withColumn("event_timestamp", _to_timestamp("event_timestamp"))
    rules = [
        (F.col("event_id").isNull(), "missing event_id"),
        (F.col("machine_id").isNull(), "missing machine_id"),
        (F.col("plant_id").isNull(), "missing plant_id"),
        (F.col("event_timestamp").isNull(), "bad timestamp"),
        (_negative_or_null("temperature"), "bad temperature"),
        (_negative_or_null("vibration"), "bad vibration"),
        (_negative_or_null("pressure"), "bad pressure"),
        (F.col("status_code").isNull() | ~F.col("status_code").isin(*VALID_STATUS_CODES), "bad status"),
    ]
    return df.withColumn("quarantine_reason", _first_failing_rule(rules))


def enrich_telemetry(valid: DataFrame, machines: DataFrame, thresholds: DataFrame) -> DataFrame:
    """Join machine attributes and alarm limits, then derive alert flags."""
    # Select only the lookup attributes we need. machine_master also has plant_id;
    # taking it too would create a duplicate column (DELTA_DUPLICATE_COLUMNS_FOUND).
    m = machines.select("machine_id", "machine_name", "machine_type", "manufacturer")
    out = (valid
           .join(F.broadcast(m), "machine_id", "left")
           .join(F.broadcast(thresholds), "machine_type", "left")
           .withColumn("master_data_found", F.col("machine_name").isNotNull())
           .withColumn("temperature_alert", F.col("temperature") > F.col("max_temperature"))
           .withColumn("vibration_alert", F.col("vibration") > F.col("max_vibration"))
           .withColumn("pressure_alert", F.col("pressure") > F.col("max_pressure"))
           .withColumn("anomaly_flag",
                       F.col("temperature_alert") | F.col("vibration_alert") | F.col("pressure_alert")))
    assert len(out.columns) == len(set(out.columns)), f"duplicate columns: {out.columns}"
    return out


# --------------------------------------------------------------------------- production
def add_production_quality(df: DataFrame) -> DataFrame:
    df = df.withColumn("event_timestamp", _to_timestamp("event_timestamp"))
    rules = [
        (F.col("event_id").isNull(), "missing event_id"),
        (F.col("machine_id").isNull(), "missing machine_id"),
        (F.col("product_id").isNull(), "missing product_id"),
        (F.col("batch_id").isNull(), "missing batch_id"),
        (F.col("event_timestamp").isNull(), "bad timestamp"),
        (_negative_or_null("units_produced"), "bad units"),
        (_negative_or_null("defect_count"), "bad defects"),
        (F.col("defect_count") > F.col("units_produced"), "defects exceed units"),
    ]
    return df.withColumn("quarantine_reason", _first_failing_rule(rules))


def enrich_production(valid: DataFrame, products: DataFrame) -> DataFrame:
    out = (valid
           .join(F.broadcast(products), "product_id", "left")
           .withColumn("master_data_found", F.col("product_name").isNotNull())
           .withColumn("defect_rate",
                       F.when(F.col("units_produced") > 0,
                              F.col("defect_count") / F.col("units_produced")).otherwise(F.lit(0.0))))
    assert len(out.columns) == len(set(out.columns)), f"duplicate columns: {out.columns}"
    return out


# --------------------------------------------------------------------------- shared helpers
def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid = df.filter(F.col("quarantine_reason").isNotNull())
    valid = df.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    return valid, invalid


def deduplicate(df: DataFrame) -> DataFrame:
    return df.withWatermark("event_timestamp", WATERMARK).dropDuplicatesWithinWatermark(["event_id"])


def _append(df: DataFrame, target: str, checkpoint: str, available_now: bool) -> StreamingQuery:
    writer = (df.writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", checkpoint))
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start(target)


# --------------------------------------------------------------------------- streaming runners
def run_silver_telemetry(spark: SparkSession, paths: Paths, available_now: bool = True) -> list[StreamingQuery]:
    stream = add_telemetry_quality(spark.readStream.format("delta").load(paths.bronze_telemetry))
    valid, invalid = split_valid_invalid(stream)

    machines = spark.read.format("delta").load(paths.master("machine_master"))
    thresholds = spark.read.format("delta").load(paths.master("threshold_config"))
    clean = enrich_telemetry(deduplicate(valid), machines, thresholds)

    # Valid and quarantine outputs are two queries with independent checkpoints.
    queries = [
        _append(invalid, paths.quarantine_telemetry, paths.checkpoint("silver_t_quarantine"), available_now),
        _append(clean, paths.silver_telemetry, paths.checkpoint("silver_t"), available_now),
    ]
    if available_now:
        for q in queries:
            q.awaitTermination()
    return queries


def run_silver_production(spark: SparkSession, paths: Paths, available_now: bool = True) -> list[StreamingQuery]:
    stream = add_production_quality(spark.readStream.format("delta").load(paths.bronze_production))
    valid, invalid = split_valid_invalid(stream)

    products = spark.read.format("delta").load(paths.master("product_master"))
    clean = enrich_production(deduplicate(valid), products)

    queries = [
        _append(invalid, paths.quarantine_production, paths.checkpoint("silver_p_quarantine"), available_now),
        _append(clean, paths.silver_production, paths.checkpoint("silver_p"), available_now),
    ]
    if available_now:
        for q in queries:
            q.awaitTermination()
    return queries
