"""Local Spark session with Delta Lake, for laptops and CI. Not used on Databricks
(Databricks already provides `spark` with Delta built in)."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_local_spark(app_name: str = "mfg-etl-local") -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (SparkSession.builder.appName(app_name).master("local[2]")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
