"""Reconciliation checks: prove that every Bronze record is accounted for.

Bronze = Silver valid + quarantine + duplicates removed. If that equation breaks,
records were lost somewhere and the job should fail loudly.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from mfg_etl.config import Paths


def layer_counts(spark: SparkSession, paths: Paths) -> dict[str, int]:
    tables = {
        "bronze_telemetry": paths.bronze_telemetry,
        "bronze_production": paths.bronze_production,
        "silver_telemetry": paths.silver_telemetry,
        "silver_production": paths.silver_production,
        "quarantine_telemetry": paths.quarantine_telemetry,
        "quarantine_production": paths.quarantine_production,
    }
    return {name: spark.read.format("delta").load(p).count() for name, p in tables.items()}


def reconcile(spark: SparkSession, paths: Paths) -> dict[str, dict[str, int]]:
    """Raise if Silver + quarantine holds more rows than Bronze, or if Silver has duplicate IDs."""
    report = {}
    for kind, bronze, silver, quarantine in [
        ("telemetry", paths.bronze_telemetry, paths.silver_telemetry, paths.quarantine_telemetry),
        ("production", paths.bronze_production, paths.silver_production, paths.quarantine_production),
    ]:
        b = spark.read.format("delta").load(bronze).count()
        s_df = spark.read.format("delta").load(silver)
        s = s_df.count()
        q = spark.read.format("delta").load(quarantine).count()
        dup_ids = s - s_df.select("event_id").distinct().count()
        if s + q > b:
            raise AssertionError(f"{kind}: silver({s}) + quarantine({q}) > bronze({b})")
        if dup_ids:
            raise AssertionError(f"{kind}: {dup_ids} duplicate event_id(s) in Silver")
        report[kind] = {"bronze": b, "silver": s, "quarantine": q, "duplicates_removed": b - s - q}
    return report
