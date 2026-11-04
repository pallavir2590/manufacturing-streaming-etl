# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: paths and storage access
# MAGIC Every other notebook runs this one with `%run ./00_setup`.
# MAGIC It reads job/widget parameters, makes the `src/` package importable, and builds `paths`.
# MAGIC
# MAGIC **Access options (pick one):**
# MAGIC 1. **Unity Catalog external location** (recommended): nothing to configure here.
# MAGIC 2. **Service principal + secret scope**: set `auth_mode = oauth`. The client secret is read
# MAGIC    from the `manufacturing` secret scope at runtime and is never in code or Git.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "ADLS storage account")
dbutils.widgets.dropdown("auth_mode", "unity_catalog", ["unity_catalog", "oauth"], "Auth mode")
dbutils.widgets.text("tenant_id", "", "Tenant ID (oauth only)")
dbutils.widgets.text("client_id", "", "Client ID (oauth only)")

# COMMAND ----------

import os
import sys

# Notebooks live in <repo>/notebooks; the package lives in <repo>/src
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from mfg_etl.config import Paths, configure_adls_oauth  # noqa: E402

storage_account = dbutils.widgets.get("storage_account")
assert storage_account, "Set the storage_account widget / job parameter"

if dbutils.widgets.get("auth_mode") == "oauth":
    configure_adls_oauth(spark, dbutils, storage_account,
                         tenant_id=dbutils.widgets.get("tenant_id"),
                         client_id=dbutils.widgets.get("client_id"))

paths = Paths.for_adls(storage_account)
print("landing telemetry :", paths.landing_telemetry)
print("silver telemetry  :", paths.silver_telemetry)
