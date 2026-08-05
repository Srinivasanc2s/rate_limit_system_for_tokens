# Databricks notebook source
"""
Auto Loader picks up new NDJSON files the local exporter dropped in the Volume
and appends them to bronze. Safe to rerun - it only reads files it hasn't seen.

Volume paths are /Volumes/<catalog>/<schema>/<volume>/<path>, so the schema
and checkpoint directories must sit INSIDE the landing volume, not beside it.
"""

from pyspark.sql import functions as F

CATALOG = "rate_limit_system"
VOLUME = f"/Volumes/{CATALOG}/bronze/landing"
EVENT_TABLES = ["query_log", "request_log"]

for table in EVENT_TABLES:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME}/_schema/{table}")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/{table}/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_lane", F.lit("volume_autoloader"))
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", f"{VOLUME}/_ckpt/{table}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)   # process what's there, then stop
        .toTable(f"{CATALOG}.bronze.{table}_raw")
    )
    query.awaitTermination()

    row_count = spark.table(f"{CATALOG}.bronze.{table}_raw").count()
    print(f"{table}_raw now has {row_count} rows")
