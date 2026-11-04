"""Gold: business KPIs, rebuilt as a bounded batch over the current Silver snapshot.

Overwriting Gold from Silver is deterministic and easy to reason about: re-running it
gives the same answer. Scheduled every few minutes, it is near-real-time reporting.
(A fully streaming Gold would need an output mode, watermark and MERGE/upsert strategy.)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_etl.config import Paths


def _pct(numerator: str, denominator: str):
    return F.when(F.col(denominator) > 0,
                  F.round(F.col(numerator) * 100.0 / F.col(denominator), 2)).otherwise(F.lit(0.0))


def hourly_production(prod: DataFrame) -> DataFrame:
    return (prod.groupBy("machine_id", F.window("event_timestamp", "1 hour").alias("hour"))
            .agg(F.sum("units_produced").alias("total_units"), F.sum("defect_count").alias("total_defects"))
            .withColumn("defect_pct", _pct("total_defects", "total_units"))
            .select("machine_id", F.col("hour.start").alias("hour_start"),
                    "total_units", "total_defects", "defect_pct"))


def hourly_anomalies(tele: DataFrame) -> DataFrame:
    return (tele.groupBy("machine_id", F.window("event_timestamp", "1 hour").alias("hour"))
            .agg(F.count("*").alias("readings"),
                 F.sum(F.when(F.col("anomaly_flag"), 1).otherwise(0)).alias("anomaly_count"),
                 F.round(F.avg("temperature"), 2).alias("avg_temperature"),
                 F.max("vibration").alias("max_vibration"))
            .select("machine_id", F.col("hour.start").alias("hour_start"),
                    "readings", "anomaly_count", "avg_temperature", "max_vibration"))


def batch_quality(prod: DataFrame) -> DataFrame:
    return (prod.groupBy("batch_id", "product_id", "machine_id")
            .agg(F.sum("units_produced").alias("units"), F.sum("defect_count").alias("defects"))
            .withColumn("defect_pct", _pct("defects", "units")))


def data_quality_summary(q_tele: DataFrame, q_prod: DataFrame) -> DataFrame:
    """How many records each rule rejected: lets the business see source-data health."""
    t = q_tele.groupBy("quarantine_reason").count().withColumn("source", F.lit("telemetry"))
    p = q_prod.groupBy("quarantine_reason").count().withColumn("source", F.lit("production"))
    return t.unionByName(p).select("source", "quarantine_reason", F.col("count").alias("rejected_records"))


def run_gold(spark: SparkSession, paths: Paths) -> dict[str, int]:
    read = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    prod, tele = read(paths.silver_production), read(paths.silver_telemetry)
    outputs = {
        "hourly_production": hourly_production(prod),
        "hourly_anomalies": hourly_anomalies(tele),
        "batch_quality": batch_quality(prod),
        "data_quality_summary": data_quality_summary(read(paths.quarantine_telemetry),
                                                     read(paths.quarantine_production)),
    }
    counts = {}
    for name, df in outputs.items():
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .save(paths.gold(name)))
        counts[name] = read(paths.gold(name)).count()
    return counts
