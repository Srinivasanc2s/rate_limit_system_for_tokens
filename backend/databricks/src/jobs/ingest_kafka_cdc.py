# Databricks notebook source
"""
Lane B: reads Debezium CDC events from Confluent Cloud into a single raw
bronze table. The envelope is stored UNPARSED so a Debezium format change
can never break ingestion - all parsing happens downstream in the pipeline.
"""

from pyspark.sql import functions as F

CATALOG = "rate_limit_system"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/landing/_ckpt/cdc_raw"

bootstrap = dbutils.secrets.get(scope="rl", key="confluent_bootstrap")
api_key = dbutils.secrets.get(scope="rl", key="confluent_key")
api_secret = dbutils.secrets.get(scope="rl", key="confluent_secret")

# Databricks shades its Kafka classes, hence the kafkashaded. prefix
jaas = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    f'required username="{api_key}" password="{api_secret}";'
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribePattern", r"rl\.rate_limit\..*")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)

bronze = raw.select(
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp").alias("_kafka_ts"),
    F.col("key").cast("string").alias("_key"),
    F.col("value").cast("string").alias("_value"),
    F.current_timestamp().alias("_ingested_at"),
    F.lit("confluent_cdc").alias("_source_lane"),
)

query = (
    bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.cdc_raw")
)
query.awaitTermination()

print(f"cdc_raw now has {spark.table(f'{CATALOG}.bronze.cdc_raw').count()} events")
