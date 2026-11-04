"""Simulated source systems.

landing/telemetry  -> IoT gateway / OPC-UA-to-cloud adapter output (JSON)
landing/production -> MES production event export (JSON)

The sample deliberately contains bad data so every quality rule is exercised:
  T004 / P004  duplicate event IDs             -> kept once in Silver
  T005 / P005  missing machine_id              -> quarantine
  T006         negative temperature            -> quarantine
  P006         defects greater than units      -> quarantine
  T002, T004   readings above machine limits   -> anomaly_flag = true

Each call appends NEW files, so running it twice doubles Bronze; Silver dedup
removes the repeated event IDs that arrive within the watermark horizon.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from mfg_etl.config import Paths
from mfg_etl.schemas import PRODUCTION_SCHEMA, TELEMETRY_SCHEMA

TELEMETRY_ROWS = [
    ("T001", "M101", "P1", "2026-03-10T10:00:00", 75.5, 1.2, 30.0, 12.5, "RUNNING"),
    ("T002", "M102", "P1", "2026-03-10T10:01:00", 95.0, 2.2, 48.0, 15.0, "RUNNING"),
    ("T004", "M101", "P1", "2026-03-10T10:03:00", 82.0, 1.6, 36.0, 13.2, "RUNNING"),
    ("T004", "M101", "P1", "2026-03-10T10:03:00", 82.0, 1.6, 36.0, 13.2, "RUNNING"),
    ("T005", None, "P1", "2026-03-10T10:04:00", 70.0, 1.0, 29.0, 11.0, "RUNNING"),
    ("T006", "M102", "P1", "2026-03-10T10:05:00", -5.0, 1.1, 31.0, 12.0, "DOWN"),
]

PRODUCTION_ROWS = [
    ("P001", "M101", "PRD1", "B001", "2026-03-10T10:00:30", 100, 2, "SHIFT_A", "OP01"),
    ("P002", "M102", "PRD2", "B002", "2026-03-10T10:01:30", 120, 5, "SHIFT_A", "OP02"),
    ("P004", "M101", "PRD1", "B004", "2026-03-10T10:03:30", 110, 15, "SHIFT_A", "OP01"),
    ("P004", "M101", "PRD1", "B004", "2026-03-10T10:03:30", 110, 15, "SHIFT_A", "OP01"),
    ("P005", None, "PRD2", "B005", "2026-03-10T10:04:30", 100, 3, "SHIFT_B", "OP04"),
    ("P006", "M102", "PRD2", "B006", "2026-03-10T10:05:30", 50, 60, "SHIFT_A", "OP02"),
]


def write_sample_files(spark: SparkSession, paths: Paths) -> None:
    """Drop one JSON file per source into landing (one micro-batch of 'arrivals')."""
    (spark.createDataFrame(TELEMETRY_ROWS, TELEMETRY_SCHEMA)
        .coalesce(1).write.mode("append").json(paths.landing_telemetry))
    (spark.createDataFrame(PRODUCTION_ROWS, PRODUCTION_SCHEMA)
        .coalesce(1).write.mode("append").json(paths.landing_production))
