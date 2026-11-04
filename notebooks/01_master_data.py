# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Master data (ERP / asset-master simulation)
# MAGIC Static lookup tables written as Delta: machines, products, alarm thresholds. Idempotent overwrite.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.master_data import write_master_data

print(write_master_data(spark, paths))
