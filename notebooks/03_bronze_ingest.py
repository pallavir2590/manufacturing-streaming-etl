# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Bronze: streaming ingest with lineage
# MAGIC Structured Streaming reads new JSON files with an explicit schema and appends them to Delta,
# MAGIC adding `ingest_timestamp` and `source_file`. `availableNow` processes everything new, then stops.
# MAGIC Each query has its own checkpoint, so a restart continues exactly where it stopped.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.bronze import run_bronze

run_bronze(spark, paths)
print("bronze telemetry :", spark.read.format("delta").load(paths.bronze_telemetry).count())
print("bronze production:", spark.read.format("delta").load(paths.bronze_production).count())
