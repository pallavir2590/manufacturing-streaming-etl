"""Bronze: land raw events as Delta with lineage. No business rules here.

Bronze keeps every record exactly as received (plus when and from which file it
arrived), so Silver can always be rebuilt by replaying Bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from mfg_etl.config import Paths
from mfg_etl.schemas import PRODUCTION_SCHEMA, TELEMETRY_SCHEMA


def read_landing(spark: SparkSession, source: str, schema: StructType) -> DataFrame:
    return (spark.readStream.schema(schema).json(source)
            .withColumn("ingest_timestamp", F.current_timestamp())
            .withColumn("source_file", F.col("_metadata.file_path")))


def start_bronze_stream(spark: SparkSession, source: str, schema: StructType, target: str,
                        checkpoint: str, available_now: bool = True) -> StreamingQuery:
    """availableNow=True processes every file not yet seen, then stops (cheap, job-friendly).
    available_now=False keeps the query running and picks up files as they land."""
    writer = (read_landing(spark, source, schema).writeStream.format("delta")
              .outputMode("append").option("checkpointLocation", checkpoint))
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start(target)


def run_bronze(spark: SparkSession, paths: Paths, available_now: bool = True) -> list[StreamingQuery]:
    queries = [
        start_bronze_stream(spark, paths.landing_telemetry, TELEMETRY_SCHEMA,
                            paths.bronze_telemetry, paths.checkpoint("bronze_t"), available_now),
        start_bronze_stream(spark, paths.landing_production, PRODUCTION_SCHEMA,
                            paths.bronze_production, paths.checkpoint("bronze_p"), available_now),
    ]
    if available_now:
        for q in queries:
            q.awaitTermination()
    return queries
