# Build Log — Problems Faced & How They Were Solved

Every blocker hit while building the Databricks lakehouse for the rate-limit system,
with cause, origin, fix, and the lesson.

Ordered by category, not chronology, so it works as a lookup table. Each entry follows
the same shape: **Symptom → Cause → Origin → Fix → Lesson**.

---

## Quick index

| # | Problem | Category | Cost |
|---|---|---|---|
| 1 | Free Edition can't reach `localhost` MySQL | Architecture | Design constraint |
| 2 | Catalog renamed in one file but not others | Config | ~30 min |
| 3 | `spark_python_task` has no `dbutils` / `spark` | Config | Would have blocked first deploy |
| 4 | MySQL user in secrets ≠ user created | Credentials | Caught pre-deploy |
| 5 | `DATABRICKS_HOST` was a browser URL | Credentials | ~1 hour |
| 6 | Edited the wrong `.env` file | Credentials | ~30 min |
| 7 | Windows env vars are case-insensitive | Credentials | Root cause of #6 |
| 8 | PAT leaked in terminal output | Security | Token rotated |
| 9 | `UC_VOLUME_NOT_FOUND` on `_schema` | Storage paths | ~45 min |
| 10 | Volume never created in renamed catalog | Storage paths | ~20 min |
| 11 | Pipeline selected columns that didn't exist | Schema | ~1 hour |
| 12 | `rescue` mode never adds new columns | Schema | ~30 min |
| 13 | `is_active` was `1`, schema said `boolean` | Schema/types | Pre-empted |
| 14 | `_snapshot_at` would re-version every SCD2 row | Pipeline logic | Pre-empted |
| 15 | DLT expectations silently drop NULL rows | Pipeline logic | Pre-empted |
| 16 | Deleted all checkpoints, not just one | Streaming state | Duplicate risk |
| 17 | `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` | Streaming state | ~1 hour |
| 18 | ngrok died from DNS failure after laptop sleep | Networking | ~19 hours idle |
| 19 | ngrok address needed in two places | Networking | Recurring |
| 20 | PowerShell cmdlets in cmd.exe | Environment | ~5 min |
| 21 | `mysql.connector` not installed in Databricks | Wrong approach | ~15 min |
| 22 | Secrets always print as `[REDACTED]` | Expected behaviour | Confusion |
| 23 | `cdc_raw` didn't exist — file never executed | Process | ~20 min |
| 24 | Lakeflow Connect gateway `CloudStorageException` | Platform limit | Abandoned path |
| 25 | `query_log` double-write inflates token counts | Source data | **Outstanding** |

---

# A. Configuration & credentials

## 1. Databricks Free Edition cannot reach `localhost` MySQL

**Symptom** — No way to point Databricks at a database running on a laptop.

**Cause** — Free Edition runs serverless compute inside Databricks' own cloud. No VPN,
no classic clusters, no ingestion gateway, no private networking.

**Origin** — Fundamental to the product tier, not a misconfiguration.

**Fix** — Three ingestion lanes, each solving it differently:
- **Lane A** — ngrok TCP tunnel gives `localhost:3306` a public address; Spark JDBC pulls
- **Lane C** — a local Python exporter *pushes* NDJSON into a Unity Catalog Volume
- **Lane B** — Debezium tails the binlog and pushes to Confluent Cloud

**Lesson** — When the platform can't pull, make the data push. Two of the three lanes
invert the direction of travel, which also decouples them from source uptime.

---

## 2. Catalog renamed in one file but not the others

**Symptom** — `export_to_volume.py` wrote to `/Volumes/rate_limit_system/...` while
`ingest_dims_jdbc.py` and the pipeline YAML used `rate_limit`.

**Cause** — The catalog was renamed mid-build; the change was applied file by file.

**Origin** — Catalog name hardcoded as a string literal in five separate places.

**Fix** — Standardised on `rate_limit_system` everywhere, and pulled it into a
`CATALOG` constant at the top of each file.

**Lesson** — Two different names are legitimately in play here — the **MySQL database**
is `rate_limit`, the **Databricks catalog** is `rate_limit_system`. The JDBC URL uses
the MySQL name; everything else uses the catalog name. Keeping that distinction explicit
prevents a whole class of confusion.

---

## 3. `spark_python_task` has no `dbutils` or `spark`

**Symptom** — Scripts using bare `dbutils.secrets.get(...)` and `spark.read...` would
fail with `NameError` when run as a job task.

**Cause** — Those globals are injected by the **notebook** runtime. A
`spark_python_task` runs a plain Python file with no such injection, and on serverless
it additionally needs an `environments:` block.

**Origin** — The scripts were written notebook-style but wired into the job as
`spark_python_task`.

**Fix** — Added `# Databricks notebook source` as line 1 of each job script and changed
the YAML to `notebook_task` / `notebook_path`.

**Lesson** — That single comment tells Databricks to treat a `.py` file as a notebook.
It belongs only in `src/jobs/*.py`, **not** in DLT pipeline files — those are loaded as
libraries and get `spark` from the pipeline's own context.

---

## 4. MySQL user in secrets didn't match the user created

**Symptom** — Would have produced `Access denied for user`.

**Cause** — `steps.txt` created `'databricks'@'%'` with password `password123`, but the
`put-secret` command stored `root`.

**Origin** — Setup notes written before the secret commands, never reconciled.

**Fix** — `databricks secrets put-secret rl mysql_user --string-value "databricks"`

**Lesson** — Caught by reading the setup notes against the actual secret values before
deploying. Worth doing deliberately rather than discovering it in a job failure.

---

## 5. `DATABRICKS_HOST` was a browser URL

**Symptom**
```
databricks.sdk.errors.platform.NotFound: Not Found
Failed to fetch host metadata from
https://dbc-1ccea413-a8e4.cloud.databricks.com/browse/folders/687466180909506?o=3179011341927249/.well-known/databricks-config
```

**Cause** — The host was copied from the browser address bar, including the
`/browse/folders/...?o=...` path. The SDK then appended its API path to that, producing
a URL that doesn't exist.

**Origin** — Copy-paste from the UI rather than the workspace root.

**Fix** — Host must be the **workspace root only**, no path, no trailing slash:
```ini
DATABRICKS_HOST=https://dbc-1ccea413-a8e4.cloud.databricks.com
```
Better still — drop the credentials from `.env` entirely and use `WorkspaceClient()` with
no arguments, which reads `~/.databrickscfg`, the same config the CLI already uses.

**Lesson** — The warning line named the exact malformed URL. Reading the *first* error
rather than the final exception would have found this immediately.

---

## 6. Edited the wrong `.env` file

**Symptom** — After "fixing" `DATABRICKS_HOST`, the identical error reappeared unchanged.

**Cause** — Two `.env` files existed:
```
backend/.env                          <- FastAPI's
backend/databricks/exporter/.env      <- the exporter's  ← the one that mattered
```
`load_dotenv()` searches from the script's own directory, so it found the exporter's copy
and stopped. The edit went into the FastAPI one.

**Origin** — Two config files with the same name for two different programs.

**Fix** — Edited the correct file, and removed the duplicated Databricks keys from
`backend/.env`.

**Lesson** — The tell was that the error was **byte-for-byte identical** after the fix.
An unchanged error means your change never reached the running code — check *which* file
is being loaded before re-editing it.

---

## 7. Windows environment variables are case-insensitive

**Symptom** — The exporter's `.env` defined `Databricks_host` (mixed case), yet
`os.getenv("DATABRICKS_HOST")` returned its broken value.

**Cause** — Python normalises `os.environ` keys to uppercase on Windows. `Databricks_host`
becomes `DATABRICKS_HOST`. The lowercase spelling provided no isolation at all.

**Origin** — Assuming env var names are case-sensitive, as they are on Linux/macOS.

**Fix** — One canonical uppercase name per value. Also `load_dotenv(override=True)` if an
OS-level variable might shadow the file.

**Lesson** — `load_dotenv()` defaults to `override=False`, so a pre-existing OS variable
silently wins over the file. Two independent gotchas stacked here.

---

## 8. Personal access token leaked into terminal output

**Symptom** — A redaction pattern matching `TOKEN=` failed against `Databricks_Token=`,
printing the PAT in full.

**Cause** — Case-sensitive pattern, mixed-case variable name.

**Fix** — Revoked the token immediately (Settings → Developer → Access tokens), then
removed tokens from files entirely in favour of `~/.databrickscfg`.

**Lesson** — Assume any redaction can fail. The durable fix isn't a better regex — it's
not having the secret in a file that gets printed. `WorkspaceClient()` with no arguments
removed the need to store it at all.

---

# B. Unity Catalog & storage paths

## 9. `UC_VOLUME_NOT_FOUND` on `_schema`

**Symptom**
```
[UC_VOLUME_NOT_FOUND] Volume `rate_limit_system`.`bronze`.`_schema` does not exist.
```

**Cause** — Volume paths have **four fixed segments** before free-form directories:
```
/Volumes/<catalog>/<schema>/<volume>/<any/nested/path>
```
The code used `VOLUME = "/Volumes/rate_limit_system/bronze"` — catalog and schema only,
**missing the volume name**. So:

| Path | Interpreted as | Result |
|---|---|---|
| `/Volumes/rate_limit_system/bronze/landing/query_log/` | volume `landing`, dir `query_log` | ✅ |
| `/Volumes/rate_limit_system/bronze/_schema/query_log` | volume **`_schema`** | ❌ |

`_schema` and `_ckpt` were being read as *sibling volumes* to `landing`, not folders.

**Origin** — Treating a Volume path like an ordinary filesystem path.

**Fix** — Everything moved inside the `landing` volume:
```python
VOLUME = f"/Volumes/{CATALOG}/bronze/landing"
.option("cloudFiles.schemaLocation", f"{VOLUME}/_schema/{table}")
.option("checkpointLocation",        f"{VOLUME}/_ckpt/{table}")
.load(f"{VOLUME}/{table}/")
```

**Lesson** — The third segment is always the volume name. The exporter worked all along
because it happened to include `landing`; the autoloader didn't.

---

## 10. Volume never created in the renamed catalog

**Symptom** — `NotFound` on the Files API upload even after fixing the host.

**Cause** — `CREATE VOLUME` had been run against the *old* catalog name (`rate_limit`).
After renaming to `rate_limit_system`, the volume didn't exist there.

**Origin** — Tables auto-create on `saveAsTable`. **Volumes do not.** So Lane A kept
working after the rename while Lane C broke.

**Fix**
```sql
SHOW VOLUMES IN rate_limit_system.bronze;
CREATE VOLUME IF NOT EXISTS rate_limit_system.bronze.landing;
```

**Lesson** — After renaming a catalog, re-run every `CREATE` statement, not just the ones
that error immediately. Auto-creating objects mask the ones that don't.

---

# C. Schema evolution & data types

## 11. Pipeline selected columns that didn't exist in bronze

**Symptom**
```
Failed to resolve flow: 'rate_limit_system.silver.silver_query_events'
Failed to resolve flow due to upstream failure: 'gold_token_usage_hourly'
```
while `silver_request_events` resolved fine.

**Cause** — `silver_query_events` selected 12 columns; bronze only had the 6 original
`query_log` columns. The 9 columns added by the `ALTER TABLE` section of
`Rate_limit_sql2.sql` had never been applied to MySQL.

**Origin** — Re-running the SQL file from the top dropped and recreated `query_log` with
its original definition; the `ALTER` block further down was never executed.

**Fix** — Two parts:
1. Ran the `ALTER TABLE query_log ADD COLUMN (...)` block in MySQL
2. Added a helper so a missing column can never break analysis again:

```python
def with_missing_as_null(df, columns: dict):
    """Add absent columns as typed NULLs so the flow stays resolvable."""
    for name, dtype in columns.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
    return df
```

**Lesson** — **The asymmetry was the diagnosis.** Both silver tables read bronze the same
way; one resolved and one didn't. Whenever two near-identical things behave differently,
the difference between them *is* the bug. The first hypothesis (empty schema) was wrong;
reading the actual bronze schema proved it was a column-set mismatch.

---

## 12. `schemaEvolutionMode = "rescue"` never adds new columns

**Symptom** — After the `ALTER`, new fields still didn't appear in bronze.

**Cause** — In `rescue` mode Auto Loader deliberately **never** widens the schema.
Unrecognised fields are packed into `_rescued_data` as JSON. The inferred schema is also
cached in `_schema/`, so it won't re-infer on its own.

**Origin** — `rescue` was chosen for stability, which is the wrong trade-off while the
source schema is actively changing.

**Fix**
```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```
plus clearing the cached state (drop the bronze table, delete `_schema/<table>/` and
`_ckpt/<table>/`).

| Mode | On a new column |
|---|---|
| `rescue` | → `_rescued_data`. Stream never fails, column never appears |
| `addNewColumns` | Stream **fails once**, records the schema, picks it up next run |

**Lesson** — `addNewColumns` pairs well with `with_missing_as_null` from #11: bronze
widens automatically, silver keeps a stable schema throughout. One failed run per schema
change is the price, and it's worth paying.

---

## 13. `is_active` arrived as `1`, schema declared `boolean`

**Symptom** — Would have produced NULLs across **every** column of the parsed struct.

**Cause** — Debezium emitted MySQL `BOOLEAN` (`TINYINT(1)`) as integer `1`, not `true`.
In `from_json`, a single type mismatch nulls the *entire struct*, not just that field.

**Origin** — Guessing the wire format instead of inspecting it.

**Fix** — Inspected a real message first:
```sql
SELECT _value FROM rate_limit_system.bronze.cdc_raw
WHERE topic = 'rl.rate_limit.users' LIMIT 1;
```
Saw `"is_active": 1`, then declared `is_active:int` in both envelope schemas.

**Lesson** — **Look at the data before writing the schema.** This failure mode is
especially nasty because it produces no error — just silent NULLs everywhere, which look
like a connectivity problem rather than a type problem.

---

# D. Pipeline logic

## 14. `_snapshot_at` would have re-versioned every SCD2 row

**Symptom** — Every row in `silver_plans` / `silver_users` would get a new SCD2 version
on every single pipeline run.

**Cause** — Lane A stamps `_snapshot_at = current_timestamp()` on each snapshot. AUTO CDC
FROM SNAPSHOT compares full rows, so every row differs every time.

**Origin** — A useful debugging column accidentally participating in change detection.

**Fix**
```python
track_history_except_column_list=["_snapshot_at"]
```

**Lesson** — Metadata columns must be excluded from change detection. Caught by reasoning
about the mechanism before running it — this one would have been slow to diagnose,
since it produces plausible-looking output that's quietly wrong.

---

## 15. DLT expectations silently drop rows where the condition is NULL

**Symptom** — `@dlt.expect_or_drop("non_negative_tokens", "tokens_used >= 0")` would have
dropped every rejected request.

**Cause** — A constraint evaluating to NULL counts as a **violation**. Rejected requests
have NULL `tokens_used`, so `NULL >= 0` → NULL → dropped.

**Origin** — SQL three-valued logic. Easy to forget when writing constraints.

**Fix** — Guard NULLs explicitly, and use `expect` (warn) rather than `expect_or_drop`
where the row is still wanted:
```python
@dlt.expect("non_negative_tokens", "tokens_used IS NULL OR tokens_used >= 0")
@dlt.expect("latency_sane",
            "execution_time_ms IS NULL OR execution_time_ms BETWEEN 0 AND 300000")
```

**Lesson** — The dropped rows would have been precisely the rate-limit rejections — the
most analytically interesting records in the whole dataset. Reserve `expect_or_drop` for
rows that are genuinely unusable (null primary key), and warn on everything else.

---

## 16. `silver_users_usage` was missing from the merged pipeline

**Symptom** — The combined Lane A + C pipeline file was a *regression* on the
Lane-A-only version.

**Cause** — Manual merge of two files; one table definition was dropped.

**Fix** — Rebuilt the file as a proper superset, then deleted the superseded
`ldp_lane_a.py`.

**Lesson** — When merging two files that define overlapping objects, diff the object
lists rather than eyeballing. Also: never reference both files in a pipeline's
`libraries:` — duplicate dataset definitions fail the pipeline outright.

---

# E. Streaming state & checkpoints

## 17. Deleted all checkpoints instead of one table's

**Symptom** — Deleting the whole `_schema/` and `_ckpt/` trees, while only dropping
`query_log_raw`, put `request_log_raw` at risk of 797 rows instead of 406.

**Cause** — Auto Loader lost its memory of the 391-row file and would re-read it, while
the target table still held those rows. Append + re-read = duplicates.

**Origin** — Treating checkpoint and table as independent things.

**Fix** — Verified the count afterwards (`406` — clean, because the table had also been
recreated). Otherwise: drop the table, delete `_ckpt/<table>/` and `_schema/<table>/`
together, re-run.

**Lesson** — **A checkpoint and its target table are one unit. Reset both, or neither.**
This rule recurred three times during the build.

---

## 18. `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`

**Symptom**
```
The streaming query was reading from an unexpected Delta table
(id = 'b4247b4d-...'). It used to read from another Delta table
(id = 'bd02585f-...') according to checkpoint.
```
`silver_request_events` failed 3 times and the pipeline aborted.

**Cause** — Dropping and recreating `bronze.request_log_raw` gave it a **new table UUID**.
DLT's internal checkpoint still referenced the old one, so the Delta streaming source
refused to proceed — it can't tell whether the new table is the same data or something
unrelated.

**Origin** — The same rule as #17, one layer up: DLT keeps its own checkpoints, separate
from Auto Loader's.

**Fix** — Pipelines UI → dropdown beside **Start** → **Full refresh selected tables** →
tick only `silver_request_events` and `gold_api_slo`.

> ⚠️ **Never "Full refresh all"** — it rebuilds `silver_plans` / `silver_users` from
> scratch and destroys the SCD2 history that took real work to accumulate.

**Lesson** — Two things made this diagnosable:
1. `silver_query_events` succeeded because it had **never run**, so had no stale
   checkpoint. Only the previously-successful flow broke.
2. The error message states the fix explicitly — *"delete your streaming query checkpoint
   to restart from scratch"*. In DLT, "delete the checkpoint" means **full refresh**;
   you never touch it by hand.

---

# F. Networking & environment

## 19. ngrok died from DNS failure after laptop sleep

**Symptom**
```
dial tcp: lookup connect.us.ngrok-agent.com: no such host
```
repeated for ~19 hours, interleaved with `heartbeat timeout` and
`connection forcibly closed`.

**Cause** — `no such host` is a **DNS resolution failure**, not an ngrok or auth problem.
The laptop slept, the network dropped, DNS stopped resolving, and the agent's reconnect
loop spun forever.

**Origin** — Two compounding issues:
1. Laptop sleep/wake cycles (visible in the timestamps: 16:45 → 21:24 → 00:18 → 09:06)
2. The original script printed the address **once at startup** and never again — so even
   a successful reconnect on a new address would have gone unnoticed

**Fix**
```cmd
taskkill /IM ngrok.exe /F
nslookup connect.us.ngrok-agent.com
ipconfig /flushdns
```
Then rewrote `ngrok_manager.py` to poll `ngrok.get_tunnels()`, detect address changes,
push them straight into the Databricks secret scope, and print the values for Confluent.

**Lesson** — Distinguish the failure classes: `no such host` = DNS; `connection refused` =
service down; `access denied` = credentials. Each points somewhere completely different.

---

## 20. The ngrok address is needed in two places

**Symptom** — Fixing the Databricks secrets alone left the CDC connector broken.

**Cause** — Free-tier ngrok rotates its address on every restart, and that address lives in:
| Consumer | Where |
|---|---|
| Lane A — JDBC | Databricks secrets `rl/ngrok_host`, `rl/ngrok_port` |
| Lane B — Debezium | Confluent connector config |

**Fix** — `ngrok_manager.py` now syncs the Databricks secrets automatically and prints the
values to paste into Confluent (which has no API in this setup).

**Lesson** — Every duplicated piece of configuration is a future outage. Automating the
half that *can* be automated cut the manual work in two, and — more valuably — makes the
rotation visible instead of surfacing as a mysterious job failure hours later.

---

## 21. PowerShell cmdlets don't work in cmd.exe

**Symptom**
```
'Get-Process' is not recognized as an internal or external command
'Resolve-DnsName' is not recognized as an internal or external command
```

**Cause** — `Verb-Noun` cmdlets are PowerShell-only. `ipconfig` worked because it's a
real executable.

**Fix**

| PowerShell | cmd.exe |
|---|---|
| `Get-Process ngrok \| Stop-Process -Force` | `taskkill /IM ngrok.exe /F` |
| `Resolve-DnsName <host>` | `nslookup <host>` |
| `Remove-Item Env:\VAR` | `set VAR=` |

**Lesson** — `databricks`, `python`, `ipconfig`, `nslookup` are real executables and work
in both. Only cmdlets need PowerShell.

---

## 22. `mysql.connector` isn't available in Databricks

**Symptom** — `ModuleNotFoundError: No module named 'mysql'` in a notebook.

**Cause** — Databricks doesn't ship `mysql-connector-python`.

**Origin** — Reaching for the same library the FastAPI backend uses.

**Fix** — Use **Spark JDBC** instead, which is what `ingest_dims_jdbc.py` already does:
```python
spark.read.format("jdbc").options(**jdbc_options, dbtable="users").load()
```

**Lesson** — The Python connector pulls rows through a single driver process; Spark JDBC
distributes and integrates with the rest of the platform. Even with `%pip install`, the
Python connector is the wrong tool inside Databricks.

---

## 23. Secrets always print as `[REDACTED]`

**Symptom** — `print(NGROK_HOST)` showed `[REDACTED]`, which looked like the secret hadn't
been written.

**Cause** — Databricks automatically masks any value obtained from `dbutils.secrets.get()`
when it appears in notebook output. **This is a security feature, not a bug.**

**Fix** — Verify without printing:
```cmd
databricks secrets list-secrets rl        :: shows last_updated_timestamp
```
```python
print("length:", len(host), "| ends with ngrok.io:", host.endswith("ngrok.io"))
```
Best of all — just *use* the secret in a JDBC read. A successful connection proves it.

**Lesson** — You can't verify a secret by reading it back. Verify by exercising it.

---

## 24. `cdc_raw` didn't exist — the file had never been executed

**Symptom** — `TABLE_OR_VIEW_NOT_FOUND` querying `bronze.cdc_raw`.

**Cause** — `ingest_kafka_cdc.py` had been written to disk but never run. Writing a file
locally does nothing until either a job task or a manual notebook run executes it.

**Fix** — Pasted the body into a notebook and ran it once; later wired it in as a job task.

**Lesson** — A file in the repo is not a deployed artefact. `bundle deploy` uploads it;
only a run creates tables.

---

# G. Platform limitations

## 25. Lakeflow Connect ingestion gateway failed

**Symptom**
```
The Ingestion Gateway has encountered an internal error and cannot recover.
com.databricks.ingestion.cdc.uc.volume.client.exception.CloudStorageException:
Error while perform put object operation. response: null
bucket name: dbstorage-prod-sn6mz
key: .../volumes/.../backup/manager/manager_backup_*.sqlite.md5
```

**Cause** — The gateway got **past** the MySQL connection — it provisioned, initialised,
and started extracting. It then failed writing its own internal SQLite state file into
Databricks-managed Unity Catalog storage. `response: null` means the S3 PUT never
received an HTTP response at all — a credential-vending or permission failure inside the
managed environment.

The `arcionshaded.s3.software.amazon.awssdk` frames confirm it: Arcion is the CDC engine
behind Lakeflow Connect. The failure is entirely inside Databricks' managed stack.

**Origin** — Free Edition. Not a configuration problem, and **not a quota** — a quota
failure rejects creation up front with an explicit message, whereas this one provisioned
compute, booted, ran, and then failed on storage.

**Correction to an earlier assumption:** the original plan stated these connectors were
structurally unavailable because the gateway requires classic compute. That was too
strong — the UI does expose it and the gateway *does* start. It fails at runtime instead.
Same outcome, different mechanism.

**Why merging it into the existing DLT pipeline isn't possible** — they're different
pipeline *types*:

| Name | Type |
|---|---|
| `ldp_rate_limit` | Pipeline - **ETL** (declarative, defined by your Python file) |
| `Ingestion_thru_connector_CDC` | Pipeline - **Ingestion** (wizard-configured, no source file) |

There is no `dlt` API for a Lakeflow Connect source. They can only be *chained* — ingestion
lands bronze, DLT reads bronze — which still requires the ingestion pipeline to work.

**Fix** — Deleted the pipeline and connection. Log-based CDC was already working via
Debezium → Confluent → Structured Streaming → AUTO CDC.

**Lesson** — Worth recording as a project finding rather than a failure:

> *Evaluated Lakeflow Connect managed MySQL CDC on Databricks Free Edition. Connection and
> pipeline created successfully and the gateway provisioned, but it failed at runtime
> writing internal state to managed Unity Catalog storage. Concluded the managed connector
> path is not viable on this tier; implemented log-based CDC directly via Debezium →
> Confluent Cloud → Structured Streaming → AUTO CDC.*

The hand-built path also teaches more — the raw envelope is visible, the schema is chosen
deliberately, and the CDC flow is written explicitly rather than hidden behind a wizard.

---

# H. Source data quality

## 26. `query_log` writes two rows per query — **OUTSTANDING**

**Symptom** — Every `/query` call produces two rows with identical `time_stamp`,
`user_id` and `tokens_used`. One sparse, one enriched:

| q_id | time_stamp | tokens_used | conversation_id | user_message |
|---|---|---|---|---|
| 2 | 09:45:39 | 130 | null | null |
| 3 | 09:45:39 | 130 | `03860ae5...` | "what is a token" |

**Cause** — Two independent inserts per request:
1. `/query` calls `sp_record_usage` — [main.py:330](backend/main.py#L330)
2. `sp_record_usage` internally calls `sp_log_query` — [Rate_limit_sql2.sql:570](Rate_limit_sql2.sql#L570) → **INSERT #1**
3. `/query` then calls `save_to_query_log` — [main.py:345](backend/main.py#L345) → **INSERT #2**

**Impact** — `gold_token_usage_hourly` sums `tokens_used`, so **every query is counted
twice**. Utilisation percentages, budget burndown and rate-limit analysis are all
inflated 2×.

**Proposed fix (source-side, preferred)** — the schema already has the right mechanism.
`message_type ENUM('llm_query','validation_check','budget_check','procedure_exec')` exists
to distinguish writers, but `sp_log_query` doesn't set it, so both rows default to
`'llm_query'`. Stamp `message_type = 'procedure_exec'` in `sp_log_query`, then filter in
silver:
```python
.filter("message_type = 'llm_query'")
```

**Stopgap (no MySQL change)** — filter on the marker only the app-level insert sets:
```python
.filter("conversation_id IS NOT NULL")
```

**Lesson** — The pipeline is faithfully aggregating what the source gave it. Data quality
issues upstream don't announce themselves as errors — they show up as numbers that are
merely *wrong*. Fix it at the source before Lane B ingests the same duplication.

---

# Cross-cutting principles

Six rules that would have prevented most of the above:

### 1. A checkpoint and its target table are one unit
Reset both, or neither. Applies at every layer — Auto Loader's `_ckpt/` with its bronze
table, DLT's internal checkpoint with its silver table. This caused #17 and #18.

### 2. Asymmetry is the diagnosis
When two near-identical things behave differently, the difference between them *is* the
bug. `silver_request_events` worked while `silver_query_events` failed (#11);
`silver_query_events` worked while `silver_request_events` failed (#18). Both times the
asymmetry pointed straight at the cause.

### 3. Look at the data before writing the schema
One `SELECT _value ... LIMIT 1` prevented an entire debugging session over
`is_active` (#13). Type mismatches in `from_json` produce silent NULLs, not errors —
the worst kind of failure.

### 4. An unchanged error means your change didn't land
Byte-for-byte identical output after a "fix" means the code never saw it. Check *which*
file, *which* environment, *which* deployment (#5, #6).

### 5. Verify by exercising, not by inspecting
You cannot print a secret (#23). You cannot confirm a connection by reading config.
Run the thing and see if it works.

### 6. Smoke test the risky dependency first
The five-minute Confluent batch read before building any of Lane B meant that when
things later broke, connectivity was already ruled out. The cheapest test is the one that
eliminates a whole category of cause.

---

# Outstanding

| # | Item | Priority |
|---|---|---|
| 26 | `query_log` double-write inflating token counts 2× | **High** — corrupts all gold metrics |
| — | Unity Catalog column masks on `user_message`, `llm_response`, `client_ip` | Medium |
| — | AI/BI dashboard + Genie space over the gold layer | Medium |
| — | ngrok reserved address, or self-managed Debezium to drop the tunnel dependency | Low |
