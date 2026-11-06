# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: paths and storage access
# MAGIC Every other notebook runs this one with `%run ./00_setup`.
# MAGIC It reads job/widget parameters, makes the `src/` package importable, and builds `paths`.
# MAGIC
# MAGIC **Storage modes**
# MAGIC * `volume` (default): a Unity Catalog volume. Works on **Databricks Free Edition**, no Azure needed.
# MAGIC * `adls`: your own ADLS Gen2 account with `landing/bronze/silver/gold` containers. Access through a
# MAGIC   Unity Catalog external location (recommended), or a service principal whose secret is read from
# MAGIC   the `manufacturing` secret scope at runtime (`auth_mode = oauth`, classic compute only).

# COMMAND ----------

dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("catalog", "workspace", "UC catalog (volume mode)")
dbutils.widgets.text("schema", "default", "UC schema (volume mode)")
dbutils.widgets.text("storage_account", "", "ADLS storage account (adls mode)")
dbutils.widgets.dropdown("auth_mode", "unity_catalog", ["unity_catalog", "oauth"], "Auth (adls mode)")
dbutils.widgets.text("tenant_id", "", "Tenant ID (oauth only)")
dbutils.widgets.text("client_id", "", "Client ID (oauth only)")

# COMMAND ----------

import os
import sys

# Notebooks live in <repo>/notebooks; the package lives in <repo>/src
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from mfg_etl.config import Paths, configure_adls_oauth  # noqa: E402

if dbutils.widgets.get("storage_mode") == "volume":
    catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.manufacturing")
    paths = Paths.for_volume(f"/Volumes/{catalog}/{schema}/manufacturing")
else:
    storage_account = dbutils.widgets.get("storage_account")
    assert storage_account, "Set the storage_account widget / job parameter"
    if dbutils.widgets.get("auth_mode") == "oauth":
        configure_adls_oauth(spark, dbutils, storage_account,
                             tenant_id=dbutils.widgets.get("tenant_id"),
                             client_id=dbutils.widgets.get("client_id"))
    paths = Paths.for_adls(storage_account)

print("landing telemetry :", paths.landing_telemetry)
print("silver telemetry  :", paths.silver_telemetry)
