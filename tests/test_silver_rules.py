"""Unit tests for the Silver quality rules and enrichment (small batch DataFrames)."""

from mfg_etl.master_data import build_master_frames
from mfg_etl.schemas import PRODUCTION_SCHEMA, TELEMETRY_SCHEMA
from mfg_etl.silver import (
    add_production_quality,
    add_telemetry_quality,
    enrich_production,
    enrich_telemetry,
    split_valid_invalid,
)
from mfg_etl.simulator import PRODUCTION_ROWS, TELEMETRY_ROWS

GOOD_T = ("T1", "M101", "P1", "2026-03-10T10:00:00", 70.0, 1.0, 30.0, 10.0, "RUNNING")


def _telemetry_reason(spark, **overrides):
    names = TELEMETRY_SCHEMA.fieldNames()
    row = dict(zip(names, GOOD_T, strict=True)) | overrides
    df = spark.createDataFrame([tuple(row[n] for n in names)], TELEMETRY_SCHEMA)
    return add_telemetry_quality(df).first()["quarantine_reason"]


def test_valid_telemetry_has_no_reason(spark):
    assert _telemetry_reason(spark) is None


def test_each_telemetry_rule(spark):
    assert _telemetry_reason(spark, machine_id=None) == "missing machine_id"
    assert _telemetry_reason(spark, event_timestamp="not-a-date") == "bad timestamp"
    assert _telemetry_reason(spark, temperature=-1.0) == "bad temperature"
    assert _telemetry_reason(spark, vibration=None) == "bad vibration"
    assert _telemetry_reason(spark, status_code="EXPLODED") == "bad status"


def test_sample_telemetry_split(spark):
    df = add_telemetry_quality(spark.createDataFrame(TELEMETRY_ROWS, TELEMETRY_SCHEMA))
    valid, invalid = split_valid_invalid(df)
    reasons = {r["event_id"]: r["quarantine_reason"] for r in invalid.collect()}
    assert reasons == {"T005": "missing machine_id", "T006": "bad temperature"}
    assert "quarantine_reason" not in valid.columns
    assert valid.count() == 4  # T004 still twice: dedup happens in the stream


def test_sample_production_split(spark):
    df = add_production_quality(spark.createDataFrame(PRODUCTION_ROWS, PRODUCTION_SCHEMA))
    _, invalid = split_valid_invalid(df)
    reasons = {r["event_id"]: r["quarantine_reason"] for r in invalid.collect()}
    assert reasons == {"P005": "missing machine_id", "P006": "defects exceed units"}


def test_enrichment_flags_anomalies_and_has_no_duplicate_columns(spark):
    master = build_master_frames(spark)
    df = add_telemetry_quality(spark.createDataFrame(TELEMETRY_ROWS, TELEMETRY_SCHEMA))
    valid, _ = split_valid_invalid(df)
    out = enrich_telemetry(valid.dropDuplicates(["event_id"]), master["machine_master"],
                           master["threshold_config"])
    assert len(out.columns) == len(set(out.columns))
    flags = {r["event_id"]: r["anomaly_flag"] for r in out.collect()}
    assert flags == {"T001": False, "T002": True, "T004": True}


def test_unknown_machine_gives_unknown_alert_not_false(spark):
    master = build_master_frames(spark)
    row = list(GOOD_T)
    row[1] = "M999"  # not in machine_master
    df = add_telemetry_quality(spark.createDataFrame([tuple(row)], TELEMETRY_SCHEMA))
    out = enrich_telemetry(df.drop("quarantine_reason"), master["machine_master"],
                           master["threshold_config"]).first()
    assert out["master_data_found"] is False
    assert out["anomaly_flag"] is None


def test_defect_rate(spark):
    master = build_master_frames(spark)
    df = add_production_quality(spark.createDataFrame(PRODUCTION_ROWS[:1], PRODUCTION_SCHEMA))
    out = enrich_production(df.drop("quarantine_reason"), master["product_master"]).first()
    assert out["defect_rate"] == 0.02
    assert out["product_name"] == "Bolt"
