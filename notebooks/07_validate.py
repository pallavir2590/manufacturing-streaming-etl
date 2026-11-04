# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Validation and reconciliation
# MAGIC Fails the job if Silver + quarantine holds more rows than Bronze, or if Silver has duplicate IDs.
# MAGIC Expected on a clean first run: Bronze 6+6, quarantine 2+2, Silver 3+3.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from mfg_etl.validation import layer_counts, reconcile

print(layer_counts(spark, paths))
print(reconcile(spark, paths))
