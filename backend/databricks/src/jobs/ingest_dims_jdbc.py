# Databricks notebook source

"""
Runs ON Databricks (as a job task). Pulls the 3 small, mutable tables
(plans, users, users_usage) from MySQL over the ngrok tunnel and overwrites
their bronze snapshot tables. AUTO CDC FROM SNAPSHOT (in the pipeline) reads
these snapshots and derives the history itself.
"""


from pyspark.sql import functions as F

ngrok_host = dbutils.secrets.get(scope="rl", key="ngrok_host")
ngrok_port = dbutils.secrets.get(scope="rl", key="ngrok_port")
mysql_user = dbutils.secrets.get(scope="rl", key="mysql_user")
mysql_password = dbutils.secrets.get(scope="rl", key="mysql_password")

jdbc_options = {
    "url": f"jdbc:mysql://{ngrok_host}:{ngrok_port}/rate_limit",
    "user": mysql_user,
    "password": mysql_password,
    "driver": "org.mariadb.jdbc.Driver",
}

DIM_TABLES = ["plans", "users", "users_usage"]

for table in DIM_TABLES:
    df = spark.read.format("jdbc").options(**jdbc_options, dbtable=table).load()
    df = df.withColumn("_snapshot_at", F.current_timestamp())
    df.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"rate_limit_system.bronze.snap_{table}")
    print(f"snap_{table} refreshed: {df.count()} rows")
