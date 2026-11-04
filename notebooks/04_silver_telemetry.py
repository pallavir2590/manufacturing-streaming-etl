# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver telemetry: validate → quarantine → dedup → enrich
# MAGIC * Invalid rows get a `quarantine_reason` and go to `silver/quarantine/telemetry`.
# MAGIC * Valid rows are deduplicated on `event_id` within a 15-minute event-time watermark.
# MAGIC * Stream-static join with machine master + thresholds, then temperature/vibration/pressure alerts.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.silver import run_silver_telemetry

run_silver_telemetry(spark, paths)

# COMMAND ----------

display(spark.read.format("delta").load(paths.silver_telemetry))
display(spark.read.format("delta").load(paths.quarantine_telemetry))
