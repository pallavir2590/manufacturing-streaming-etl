"""Static reference data (simulates ERP / asset-master extracts).

Master data is a lookup, not a stream. It is written once as Delta and joined to the
streams in Silver (stream-static join).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from mfg_etl.config import Paths

MACHINES = [
    ("M101", "Cutter-101", "CUTTER", "P1", "Bosch"),
    ("M102", "Press-102", "PRESS", "P1", "Siemens"),
    ("M201", "Welder-201", "WELDER", "P2", "GE"),
]
MACHINE_COLS = ["machine_id", "machine_name", "machine_type", "plant_id", "manufacturer"]

PRODUCTS = [
    ("PRD1", "Bolt", "Fasteners", 30),
    ("PRD2", "Nut", "Fasteners", 20),
    ("PRD3", "Panel", "Assemblies", 90),
]
PRODUCT_COLS = ["product_id", "product_name", "category", "standard_cycle_time_sec"]

# Alarm limits per machine type
THRESHOLDS = [
    ("CUTTER", 80.0, 1.5, 35.0),
    ("PRESS", 90.0, 2.0, 50.0),
    ("WELDER", 100.0, 2.5, 40.0),
]
THRESHOLD_COLS = ["machine_type", "max_temperature", "max_vibration", "max_pressure"]


def build_master_frames(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "machine_master": spark.createDataFrame(MACHINES, MACHINE_COLS),
        "product_master": spark.createDataFrame(PRODUCTS, PRODUCT_COLS),
        "threshold_config": spark.createDataFrame(THRESHOLDS, THRESHOLD_COLS),
    }


def write_master_data(spark: SparkSession, paths: Paths) -> dict[str, int]:
    """Overwrite the master tables. Safe to re-run (idempotent)."""
    counts = {}
    for name, df in build_master_frames(spark).items():
        df.write.format("delta").mode("overwrite").save(paths.master(name))
        counts[name] = df.count()
    return counts
