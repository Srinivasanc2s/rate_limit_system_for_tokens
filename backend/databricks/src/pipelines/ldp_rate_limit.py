"""
Rate Limit Lakehouse - Lakeflow Declarative Pipeline.

  Lane A (JDBC snapshots)  -> bronze.snap_*      -> silver dimensions (SCD)
  Lane C (Auto Loader)     -> bronze.*_raw       -> silver event tables
                                                 -> gold aggregates

Target catalog/schema are set in resources/rate_limit_pipeline.yml,
so table names here are unqualified.
"""

import dlt
from pyspark.sql import functions as F

BRONZE = "rate_limit_system.bronze"

# _snapshot_at is stamped fresh on every Lane A run, so every row would look
# modified every time. Excluding it stops SCD2 from versioning the whole
# table on each pipeline update.
SNAPSHOT_META = ["_snapshot_at"]


# =============================================================
# SILVER - dimensions  (Lane A, snapshot-based CDC)
# =============================================================

dlt.create_streaming_table(
    "silver_plans",
    comment="Plan definitions with full history. Token limits change over time.",
)
dlt.create_auto_cdc_from_snapshot_flow(
    target="silver_plans",
    source=f"{BRONZE}.snap_plans",
    keys=["plan_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=SNAPSHOT_META,
)

dlt.create_streaming_table(
    "silver_users",
    comment="Users with full history. Tracks budget and plan migrations.",
)
dlt.create_auto_cdc_from_snapshot_flow(
    target="silver_users",
    source=f"{BRONZE}.snap_users",
    keys=["user_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=SNAPSHOT_META,
)

dlt.create_streaming_table(
    "silver_users_usage",
    comment="Current rolling token counters. History not retained (SCD1).",
)
dlt.create_auto_cdc_from_snapshot_flow(
    target="silver_users_usage",
    source=f"{BRONZE}.snap_users_usage",
    keys=["user_id"],
    stored_as_scd_type=1,
)


# =============================================================
# SILVER - events  (Lane C, Auto Loader)
# =============================================================

# Expectations are written to tolerate NULLs explicitly. A constraint that
# evaluates to NULL counts as a violation, so "tokens_used >= 0" alone would
# silently drop every rejected request - exactly the rows worth keeping.

@dlt.table(comment="Cleaned LLM query events, deduplicated on q_id.")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
@dlt.expect("non_negative_tokens", "tokens_used IS NULL OR tokens_used >= 0")
@dlt.expect("not_future_dated", "event_ts <= current_timestamp()")
def silver_query_events():
    return (
        spark.readStream.table(f"{BRONZE}.query_log_raw")
        .withColumn("event_ts", F.to_timestamp(F.col("time_stamp")))
        .withWatermark("event_ts", "1 hour")
        .dropDuplicatesWithinWatermark(["q_id"])
        .select(
            "q_id",
            "user_id",
            "plan_id",
            "event_ts",
            "query_status",
            F.col("tokens_used").cast("int").alias("tokens_used"),
            F.col("tokens_estimated").cast("int").alias("tokens_estimated"),
            "conversation_id",
            "message_type",
            "validation_result",
            F.col("execution_time_ms").cast("int").alias("execution_time_ms"),
            F.col("is_successful").cast("boolean").alias("is_successful"),
            "_ingested_at",
        )
    )


@dlt.table(comment="Cleaned HTTP request traces from the FastAPI middleware.")
@dlt.expect_or_drop("has_request_id", "request_id IS NOT NULL")
@dlt.expect("latency_sane",
            "execution_time_ms IS NULL OR execution_time_ms BETWEEN 0 AND 300000")
def silver_request_events():
    return (
        spark.readStream.table(f"{BRONZE}.request_log_raw")
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .withWatermark("event_ts", "1 hour")
        .dropDuplicatesWithinWatermark(["request_id"])
        .select(
            "log_id",
            "request_id",
            "user_id",
            "http_method",
            "endpoint",
            F.col("response_status_code").cast("int").alias("status_code"),
            (F.col("response_status_code") >= 400).alias("is_error"),
            F.col("execution_time_ms").cast("int").alias("execution_time_ms"),
            "client_ip",
            "event_ts",
            "_ingested_at",
        )
    )


# =============================================================
# GOLD - business aggregates
# =============================================================

@dlt.table(comment="Token consumption by user, plan and hour.")
def gold_token_usage_hourly():
    return (
        dlt.read("silver_query_events")
        .withColumn("usage_hour", F.date_trunc("hour", "event_ts"))
        .groupBy("usage_hour", "user_id", "plan_id")
        .agg(
            F.sum("tokens_used").alias("tokens_used"),
            F.count("*").alias("query_count"),
            F.sum(F.when(F.col("query_status") == "failed", 1).otherwise(0))
             .alias("failed_count"),
        )
    )


@dlt.table(comment="Latency percentiles and error rate per API endpoint.")
def gold_api_slo():
    return (
        dlt.read("silver_request_events")
        .groupBy(
            F.date_trunc("hour", "event_ts").alias("usage_hour"),
            "endpoint",
            "http_method",
        )
        .agg(
            F.count("*").alias("request_count"),
            F.expr("percentile_approx(execution_time_ms, 0.50)").alias("p50_ms"),
            F.expr("percentile_approx(execution_time_ms, 0.95)").alias("p95_ms"),
            F.expr("percentile_approx(execution_time_ms, 0.99)").alias("p99_ms"),
            F.round(
                100.0 * F.sum(F.col("is_error").cast("int")) / F.count("*"), 2
            ).alias("error_rate_pct"),
        )
    )


# =============================================================
# SILVER - Lane B  (log-based CDC via Debezium / Confluent)
# =============================================================
# These sit BESIDE the Lane A dimensions, they do not replace them:
#   silver_users      <- Lane A, inferred by diffing snapshots
#   silver_users_cdc  <- Lane B, captured from the MySQL binlog
# Same source table, two mechanisms, kept separate so they can be compared.

USERS_ENVELOPE = """
struct<
  before: struct<user_id:int, user_name:string, plan_id:int,
                 token_budget:int, remaining_budget:int, is_active:int>,
  after:  struct<user_id:int, user_name:string, plan_id:int,
                 token_budget:int, remaining_budget:int, is_active:int>,
  op: string,
  ts_ms: bigint
>
"""

PLANS_ENVELOPE = """
struct<
  before: struct<plan_id:int, plan_name:string, hourly_token_limit:int,
                 daily_token_limit:int, monthly_token_limit:int, is_active:int>,
  after:  struct<plan_id:int, plan_name:string, hourly_token_limit:int,
                 daily_token_limit:int, monthly_token_limit:int, is_active:int>,
  op: string,
  ts_ms: bigint
>
"""


def debezium_changes(topic: str, envelope: str, columns: list):
    """
    Pull one Debezium topic out of the shared raw table and flatten it.

    On a delete the `after` block is null and the row lives in `before`, so
    the two are coalesced - otherwise the key would be null and AUTO CDC
    could not work out which row to close off.
    """
    parsed = F.from_json(F.col("_value"), envelope)
    row = F.coalesce(parsed["after"], parsed["before"])

    fields = [row[c].alias(c) for c in columns]
    fields += [parsed["op"].alias("_op"), parsed["ts_ms"].alias("_ts_ms")]

    return (
        spark.readStream.table(f"{BRONZE}.cdc_raw")
        .filter(F.col("topic") == topic)
        .select(*fields)
    )


@dlt.view
def users_changes():
    return debezium_changes(
        "rl.rate_limit.users",
        USERS_ENVELOPE,
        ["user_id", "user_name", "plan_id",
         "token_budget", "remaining_budget", "is_active"],
    )


@dlt.view
def plans_changes():
    return debezium_changes(
        "rl.rate_limit.plans",
        PLANS_ENVELOPE,
        ["plan_id", "plan_name", "hourly_token_limit",
         "daily_token_limit", "monthly_token_limit", "is_active"],
    )


dlt.create_streaming_table(
    "silver_users_cdc",
    comment="User history from the MySQL binlog. Captures every intermediate change.",
)
dlt.create_auto_cdc_flow(
    target="silver_users_cdc",
    source="users_changes",
    keys=["user_id"],
    sequence_by=F.col("_ts_ms"),
    apply_as_deletes=F.expr("_op = 'd'"),
    except_column_list=["_op", "_ts_ms"],
    stored_as_scd_type=2,
)

dlt.create_streaming_table(
    "silver_plans_cdc",
    comment="Plan history from the MySQL binlog.",
)
dlt.create_auto_cdc_flow(
    target="silver_plans_cdc",
    source="plans_changes",
    keys=["plan_id"],
    sequence_by=F.col("_ts_ms"),
    apply_as_deletes=F.expr("_op = 'd'"),
    except_column_list=["_op", "_ts_ms"],
    stored_as_scd_type=2,
)
