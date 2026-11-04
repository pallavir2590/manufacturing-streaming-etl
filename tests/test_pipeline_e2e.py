"""End-to-end test: real streaming queries (availableNow) writing Delta in a temp folder."""

from pyspark.sql import functions as F

from mfg_etl.bronze import run_bronze
from mfg_etl.config import Paths
from mfg_etl.gold import run_gold
from mfg_etl.master_data import write_master_data
from mfg_etl.silver import run_silver_production, run_silver_telemetry
from mfg_etl.simulator import write_sample_files
from mfg_etl.validation import layer_counts, reconcile


def _run_once(spark, paths):
    write_sample_files(spark, paths)
    run_bronze(spark, paths)
    run_silver_telemetry(spark, paths)
    run_silver_production(spark, paths)
    return run_gold(spark, paths)


def test_full_pipeline_and_replay(spark, tmp_path):
    paths = Paths.for_local(str(tmp_path))
    write_master_data(spark, paths)

    gold = _run_once(spark, paths)
    assert layer_counts(spark, paths) == {
        "bronze_telemetry": 6, "bronze_production": 6,
        "silver_telemetry": 3, "silver_production": 3,
        "quarantine_telemetry": 2, "quarantine_production": 2,
    }
    assert reconcile(spark, paths)["telemetry"]["duplicates_removed"] == 1
    assert gold["batch_quality"] == 3

    batch = {r["batch_id"]: r["defect_pct"]
             for r in spark.read.format("delta").load(paths.gold("batch_quality")).collect()}
    assert batch == {"B001": 2.0, "B002": 4.17, "B004": 13.64}

    anomalies = (spark.read.format("delta").load(paths.gold("hourly_anomalies"))
                 .agg(F.sum("anomaly_count")).first()[0])
    assert anomalies == 2  # T002 (press over limits) and T004 (cutter over limits)

    # Replay: the same events arrive again. Bronze keeps everything (raw history),
    # Silver must NOT double count because the event IDs were already seen.
    _run_once(spark, paths)
    counts = layer_counts(spark, paths)
    assert counts["bronze_telemetry"] == 12
    assert counts["silver_telemetry"] == 3
    assert counts["silver_production"] == 3
    reconcile(spark, paths)  # raises if Silver has duplicate IDs
