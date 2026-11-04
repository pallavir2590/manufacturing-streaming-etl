# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Simulate source files (IoT telemetry + MES production)
# MAGIC Writes one JSON file per source into `landing/`. The sample includes duplicates, missing IDs,
# MAGIC a negative temperature and defects > units, so every quality rule is exercised.
# MAGIC Running this again appends **new** files (more raw events); Silver removes the repeated IDs.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.simulator import write_sample_files

write_sample_files(spark, paths)
display(dbutils.fs.ls(paths.landing_telemetry))
