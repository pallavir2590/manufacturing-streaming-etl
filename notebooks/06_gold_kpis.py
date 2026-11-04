# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Gold: business KPIs
# MAGIC Rebuilt from the current Silver snapshot (deterministic overwrite):
# MAGIC `hourly_production`, `hourly_anomalies`, `batch_quality`, `data_quality_summary`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.gold import run_gold

print(run_gold(spark, paths))

# COMMAND ----------

spark.read.format("delta").load(paths.gold("batch_quality")).createOrReplaceTempView("batch_quality")
display(spark.sql("SELECT * FROM batch_quality ORDER BY defect_pct DESC"))
