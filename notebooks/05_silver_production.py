# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Silver production: validate → quarantine → dedup → enrich
# MAGIC Rules: missing IDs, bad timestamp, negative units/defects, defects greater than units.
# MAGIC Valid events are joined to product master and get a `defect_rate`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.silver import run_silver_production

run_silver_production(spark, paths)

# COMMAND ----------

display(spark.read.format("delta").load(paths.silver_production))
display(spark.read.format("delta").load(paths.quarantine_production))
