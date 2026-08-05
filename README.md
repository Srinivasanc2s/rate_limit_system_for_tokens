# Token Rate Limit System — OLTP + Lakehouse

A token-based **rate limiting system** for LLM APIs, built as a system-design exercise,
then extended into a full **Databricks lakehouse** with three independent ingestion lanes.

The rate-limit logic lives entirely in **MySQL stored procedures**; a thin **FastAPI**
backend orchestrates the request flow and a single-page **HTML UI** drives it. Everything
that database produces is then ingested into Databricks three different ways — batch,
file-streaming, and log-based CDC — and modelled through a medallion architecture.

---

## What it does

Every user belongs to a **plan** (hourly / daily / monthly token limits) and has a
personal **token budget** used for overflow. Each request to the LLM passes through a
strict three-step gate:

```
1. sp_validate_request   →  Is the user allowed? (checks limits, refreshes time windows)
2. LLM call (simulated)  →  Produce a response + token count
3. sp_record_usage       →  Deduct tokens under a row-level lock (FOR UPDATE)
```

The `SELECT ... FOR UPDATE` inside `sp_record_usage` **serializes concurrent requests
for the same user** at the database level, so a user can only have one in-flight
request at a time — no application-level locking needed.

---

## Architecture at a glance

```
                        MySQL  (laptop)
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
    LANE A               LANE C               LANE B
   JDBC pull        exporter + files      binlog + Kafka
  (dimensions)         (events)          (all tables, CDC)
        │                    │                    │
        ▼                    ▼                    ▼
   bronze.snap_*      bronze.*_raw          bronze.cdc_raw
        │                    │                    │
        └────────────────────┴────────────────────┘
                             ▼
              Lakeflow Declarative Pipeline
              bronze → silver → gold
                             ▼
              Lakeflow Job  •  Unity Catalog  •  AI/BI
```

| Lane | Mechanism | Tables it owns | CDC style |
|---|---|---|---|
| **A** | Spark JDBC over ngrok | `plans`, `users`, `users_usage` | Snapshot diffing |
| **C** | Python exporter → UC Volume → Auto Loader | `query_log`, `request_log` | Query-based (watermark) |
| **B** | Debezium → Confluent Cloud → Structured Streaming | all five | **Log-based (binlog)** |

Lanes A and C are complementary — disjoint table sets, no overlap. Lane B deliberately
re-ingests the same data by a different mechanism into separate tables, so the two
approaches can be compared directly.

Full reasoning in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Tech stack

| Layer | Technology |
|---|---|
| Database | MySQL 8 (stored procedures, binlog CDC) |
| Backend | FastAPI (Python) |
| Frontend | Plain HTML + vanilla JS (no build) |
| Tunnel | ngrok (TCP) |
| Streaming | Confluent Cloud (Kafka) + Debezium |
| Lakehouse | Databricks Free Edition — Unity Catalog, Lakeflow Declarative Pipelines, Lakeflow Jobs |
| Deployment | Databricks Asset Bundles (DAB) |

---

## Project structure

```
.
├── Rate_limit_sql2.sql            Schema + 22 stored procedures + test cases
├── README.md                      This file
├── ARCHITECTURE.md                Lane design, concepts, medallion model
├── TROUBLESHOOTING.md             Every problem hit during the build, and its fix
│
├── backend/
│   ├── main.py                    FastAPI app (endpoints, logging middleware)
│   ├── index.html                 Single-page UI
│   ├── ngrok_manager.py           Opens the tunnel, syncs Databricks secrets
│   ├── requirements.txt
│   ├── .env                       MySQL credentials (gitignored)
│   │
│   └── databricks/                ── the lakehouse ──
│       ├── databricks.yml         DAB root: bundle name, targets
│       ├── resources/
│       │   ├── rate_limit_job.yml       Lakeflow Job: 3 ingest tasks + pipeline
│       │   └── rate_limit_pipeline.yml  Declarative pipeline definition
│       ├── src/
│       │   ├── jobs/
│       │   │   ├── ingest_dims_jdbc.py        LANE A → bronze.snap_*
│       │   │   ├── ingest_events_autoloader.py LANE C → bronze.*_raw
│       │   │   └── ingest_kafka_cdc.py         LANE B → bronze.cdc_raw
│       │   └── pipelines/
│       │       └── ldp_rate_limit.py     bronze → silver → gold
│       └── exporter/
│           ├── export_to_volume.py   LANE C laptop half: rows → NDJSON → Volume
│           ├── requirements.txt
│           ├── .env                  MySQL credentials (gitignored)
│           └── .export_state.json    Watermarks (gitignored, auto-created)
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| MySQL | 8.0+ | Binlog defaults are already correct for CDC |
| Python | 3.11+ | |
| Databricks CLI | latest | `winget install Databricks.DatabricksCLI` |
| ngrok account | free tier | Address rotates on restart — see Part 3 |
| Databricks workspace | Free Edition | Serverless only |
| Confluent Cloud account | free credits | Only needed for Lane B |

> ⚠️ **Naming that trips people up:** the **MySQL database** is called `rate_limit`.
> The **Databricks catalog** is called `rate_limit_system`. The JDBC URL uses the MySQL
> name; everything else uses the catalog name. Keep them straight.

---

# PART 1 — MySQL

### 1.1 Find your connection details

```sql
SHOW VARIABLES LIKE 'port';        -- usually 3306
SELECT USER(), @@hostname;
```

Host is `localhost`. The **root password** is whatever you set during the MySQL installer
— there's no way to recover it, so reset it via MySQL Workbench or reinstall if lost.

Confirm you can connect:
```cmd
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql" -u root -p
```

### 1.2 Load the schema and procedures

```cmd
mysql -u root -p < Rate_limit_sql2.sql
```

### 1.3 Run the ALTER section — don't skip this

`Rate_limit_sql2.sql` drops and recreates `query_log` at the top, but the extra chat
columns are added by an `ALTER` block further down. Running the file top-to-bottom is
fine; running only the first half leaves you with 6 columns instead of 15 and the
pipeline will fail to resolve.

```sql
USE rate_limit;

ALTER TABLE query_log ADD COLUMN (
    conversation_id VARCHAR(100),
    user_message VARCHAR(1000),
    llm_response VARCHAR(3000),
    message_type ENUM('llm_query','validation_check','budget_check','procedure_exec')
                 DEFAULT 'llm_query',
    allow_budget_override BOOLEAN DEFAULT FALSE,
    validation_result VARCHAR(100),
    tokens_estimated INT,
    execution_time_ms INT,
    is_successful BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_chat_history ON query_log(user_id, conversation_id, time_stamp);

DESCRIBE query_log;   -- expect 15 columns
```

### 1.4 Create the Databricks user (Lane A)

Databricks connects as its own user, not root:

```sql
CREATE USER 'databricks'@'%' IDENTIFIED BY 'password123';
GRANT ALL PRIVILEGES ON rate_limit.* TO 'databricks'@'%';
FLUSH PRIVILEGES;
```

`'%'` allows connections from any host — required, because the connection arrives via the
ngrok tunnel rather than from `localhost`.

Verify:
```cmd
mysql -u databricks -p
```

### 1.5 Enable binlog CDC (Lane B only)

MySQL 8 ships with all four settings correct, so this is normally just verification:

```sql
SHOW VARIABLES LIKE 'log_bin';           -- ON
SHOW VARIABLES LIKE 'binlog_format';     -- ROW
SHOW VARIABLES LIKE 'binlog_row_image';  -- FULL
SHOW VARIABLES LIKE 'server_id';         -- non-zero
```

If any are wrong, edit `C:\ProgramData\MySQL\MySQL Server 8.0\my.ini` under `[mysqld]`:

```ini
server-id=1
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
binlog_expire_logs_seconds=604800
```

Restart as Administrator:
```cmd
net stop MySQL80
net start MySQL80
```

### 1.6 Create the Debezium capture user (Lane B only)

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY 'Debezium#2026';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';

-- MySQL 8 defaults to caching_sha2_password, which some connectors can't negotiate
ALTER USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'Debezium#2026';

FLUSH PRIVILEGES;
```

Why each grant: `SELECT` reads rows during the initial snapshot; `RELOAD` takes a brief
global lock for a consistent start point; **`REPLICATION SLAVE` is the one that matters**
— it permits streaming the binlog; `REPLICATION CLIENT` reads binlog position metadata.

---

# PART 2 — Backend

```cmd
cd backend
python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```

Create `backend/.env`:
```ini
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=rate_limit
NGROK_AUTHTOKEN=your_ngrok_token
```

Run it:
```cmd
uvicorn main:app --reload
```
Open **http://localhost:8000**, send a few `/query` requests so there's data to ingest.

---

# PART 3 — ngrok

Databricks Free Edition runs in Databricks' cloud and **cannot reach `localhost`**.
The tunnel gives MySQL a public address.

### 3.1 Get your authtoken

Sign up at [ngrok.com](https://ngrok.com) → **Your Authtoken** → copy it into
`backend/.env` as `NGROK_AUTHTOKEN`.

### 3.2 Start the tunnel

```cmd
cd backend
python ngrok_manager.py
```

Output:
```
MySQL tunnel is now 4.tcp.ngrok.io:29384
  databricks secrets updated: rl/ngrok_host, rl/ngrok_port
  paste into the Confluent connector settings:
     Database hostname : 4.tcp.ngrok.io
     Database port     : 29384
```

**Leave this terminal open.** The script polls for address changes and pushes them
straight into the Databricks secret scope.

> ⚠️ **Free-tier ngrok rotates its address on every restart**, and that address lives in
> **two** places — the Databricks secret scope (Lane A) and the Confluent connector config
> (Lane B). The script syncs the first automatically; the second is a manual paste.
>
> If your laptop sleeps, the tunnel usually dies with
> `lookup connect.us.ngrok-agent.com: no such host` (a DNS failure). Kill it and restart:
> ```cmd
> taskkill /IM ngrok.exe /F
> ipconfig /flushdns
> python ngrok_manager.py
> ```

---

# PART 4 — Databricks

### 4.1 Install the CLI

```cmd
winget install Databricks.DatabricksCLI
```
Close and reopen your terminal, then:
```cmd
databricks -v
```

### 4.2 Get your workspace host

The **workspace root only** — no path, no trailing slash:
```
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

> ⚠️ Do **not** copy the full browser URL. Anything after `.com` (like
> `/browse/folders/123?o=456`) breaks every SDK call with a confusing `NotFound`.

### 4.3 Create a personal access token

Databricks UI → avatar (top right) → **Settings → Developer → Access tokens → Manage →
Generate new token**. Copy it immediately — it's shown once.

### 4.4 Configure and verify

```cmd
databricks configure --token
```
Enter the host from 4.2 and the token from 4.3. Then:
```cmd
databricks current-user me
```

If that prints your user info, auth works. **If it errors, stop — nothing downstream will
work.** The CLI writes `~/.databrickscfg`, which every script here reads via
`WorkspaceClient()` with no arguments — so credentials never live in a file in this repo.

### 4.5 Create Unity Catalog objects (SQL Editor)

Databricks UI → **SQL Editor** → run:

```sql
CREATE CATALOG IF NOT EXISTS rate_limit_system;

CREATE SCHEMA IF NOT EXISTS rate_limit_system.bronze
  COMMENT 'Raw ingested data. No transformations.';
CREATE SCHEMA IF NOT EXISTS rate_limit_system.silver
  COMMENT 'Cleaned, typed, deduplicated, conformed.';
CREATE SCHEMA IF NOT EXISTS rate_limit_system.gold
  COMMENT 'Business aggregates for dashboards.';

CREATE VOLUME IF NOT EXISTS rate_limit_system.bronze.landing
  COMMENT 'Landing zone for NDJSON exported from MySQL.';
```

Verify in **Catalog Explorer**: `rate_limit_system` → `bronze` → **Volumes** → `landing`.

> ⚠️ Tables auto-create on write. **Volumes do not.** If you ever rename the catalog,
> re-run the `CREATE VOLUME` statement or Lane C will fail with `NotFound`.

### 4.6 Create the secret scope

```cmd
databricks secrets create-scope rl

databricks secrets put-secret rl mysql_user     --string-value "databricks"
databricks secrets put-secret rl mysql_password --string-value "password123"
databricks secrets put-secret rl ngrok_host     --string-value "4.tcp.ngrok.io"
databricks secrets put-secret rl ngrok_port     --string-value "29384"
```

(`ngrok_manager.py` keeps the last two current from then on.)

For Lane B, add three more after Part 5:
```cmd
databricks secrets put-secret rl confluent_bootstrap --string-value "pkc-xxxxx.us-east-2.aws.confluent.cloud:9092"
databricks secrets put-secret rl confluent_key       --string-value "<api-key>"
databricks secrets put-secret rl confluent_secret    --string-value "<api-secret>"
```

Verify:
```cmd
databricks secrets list-secrets rl
```

> Secret **values** are never shown back — not by the CLI, and not by `print()` in a
> notebook (Databricks masks them as `[REDACTED]` by design). Verify a secret by *using*
> it, not by reading it.

---

# PART 5 — Confluent Cloud (Lane B only)

### 5.1 Create a cluster

confluent.cloud → **Environments** → default → **Create cluster**

| Setting | Value |
|---|---|
| Type | **Basic** |
| Cloud / Region | **Match your Databricks workspace** (e.g. AWS `us-east-2`) |
| Name | `rate-limit` |

Matching the region keeps latency low and avoids cross-region transfer charges. Your
workspace's cloud and region appear in any pipeline event log under `origin`.

### 5.2 API key and bootstrap server

- Cluster → **API Keys** → **Create key** → scoped to this cluster. Copy **both** parts.
- Cluster → **Cluster settings** → copy the **Bootstrap server**
  (`pkc-xxxxx.us-east-2.aws.confluent.cloud:9092`)

Store all three in the secret scope (see 4.6).

### 5.3 Smoke test before building anything

Five minutes here saves hours. Create a topic `smoke-test` (1 partition), use the UI's
**Produce** button to send `{"hello":"world"}`, then in a Databricks notebook:

```python
bootstrap = dbutils.secrets.get("rl", "confluent_bootstrap")
api_key = dbutils.secrets.get("rl", "confluent_key")
api_secret = dbutils.secrets.get("rl", "confluent_secret")

jaas = ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="{api_key}" password="{api_secret}";')

df = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", "smoke-test")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .option("startingOffsets", "earliest").load())

df.selectExpr("CAST(value AS STRING)").show(truncate=False)
```

> ⚠️ Note the **`kafkashaded.`** prefix on the login module. Databricks shades its Kafka
> classes; the plain `org.apache.kafka...` path fails with a confusing `ClassNotFound`.

**If you see your message, the rest of Lane B is mechanical.** If not, fix it here.

### 5.4 Create the Debezium connector

Confluent → **Connectors** → **MySQL CDC Source V2 (Debezium)** → Add.

| Field | Value |
|---|---|
| Kafka credentials | the API key from 5.2 |
| Database hostname / port | your **current** ngrok host and port |
| Database user / password | `debezium` / `Debezium#2026` |
| Database server ID | `184054` |
| **Topic prefix** | `rl` |
| Databases included | `rate_limit` |
| Tables included | `rate_limit.plans,rate_limit.users,rate_limit.users_usage,rate_limit.query_log,rate_limit.request_log` |
| **Snapshot mode** | `initial` |
| Output record value format | **JSON** |
| **After-state only** | **false** ← critical |
| Tasks | 1 |

Two settings decide whether this works at all:

- **`snapshot.mode = initial`** — full backfill of existing rows (as `op: "r"`) *then*
  live changes, from one connector.
- **After-state only = false** — if left on, Confluent strips the envelope and sends only
  the `after` row. You'd lose `op` and `ts_ms`, and AUTO CDC would have nothing to
  sequence with or detect deletes from. **This one toggle breaks the whole lane.**

Wait for `Provisioning → Running`, then check **Topics** — five topics should appear with
messages. Table names must be fully qualified (`rate_limit.users`, not `users`) or the
topics stay empty.

---

# PART 6 — Deploy the lakehouse

```cmd
cd backend\databricks
databricks bundle validate      :: checks YAML without deploying
databricks bundle deploy -t dev
```

This uploads every file and creates the job and pipeline. Check **Jobs & Pipelines** —
you should see `[dev <you>] rate_limit_ingestion_job` (paused) and
`[dev <you>] ldp_rate_limit`.

The job schedule ships as `PAUSED` on purpose. Run it by hand until it's proven, then
flip `pause_status: UNPAUSED` in `rate_limit_job.yml` and redeploy.

---

# PART 7 — Running each lane

## Lane A — JDBC batch (dimensions)

Pulls `plans`, `users`, `users_usage` over the tunnel and **overwrites** bronze snapshot
tables. `AUTO CDC FROM SNAPSHOT` then diffs successive snapshots to build SCD Type 2
history — so bronze is a scratch buffer and **silver holds the history**.

**Requires:** ngrok running, secrets current.

```cmd
databricks bundle run rate_limit_ingestion_job -t dev
```

**Verify:**
```sql
SELECT * FROM rate_limit_system.bronze.snap_plans;
SELECT * FROM rate_limit_system.silver.silver_plans;   -- has __START_AT / __END_AT
```

**Prove SCD2 works:**
```sql
-- MySQL
UPDATE plans SET hourly_token_limit = 5000 WHERE plan_id = 1;
```
Re-run the job, then expect two rows for `plan_id = 1` — the old limit closed off, the new
one open.

## Lane C — Files + Auto Loader (events)

Two halves with **two separate watermarks**:

| Hop | Who tracks what's new |
|---|---|
| MySQL rows → NDJSON files | your exporter, via `.export_state.json` |
| files → Delta table | Auto Loader, via its checkpoint |

**Setup once:**
```cmd
cd backend\databricks\exporter
pip install -r requirements.txt
```

Create `backend/databricks/exporter/.env` — **MySQL only**, no Databricks credentials
(the SDK reads `~/.databrickscfg`):
```ini
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=rate_limit
```

**Run:**
```cmd
python export_to_volume.py
```
```
[15:25:23] export starting
  query_log: 4 rows -> /Volumes/rate_limit_system/bronze/landing/query_log/query_log_000000000002_000000000005.json
  request_log: 15 rows -> /Volumes/.../request_log_000000000392_000000000406.json
```

> The exporter must run **before** the first Auto Loader run — it creates the landing
> directories on first upload, and Auto Loader fails on a non-existent path.

Then the job picks the files up. **Verify:**
```sql
SELECT COUNT(*) FROM rate_limit_system.bronze.query_log_raw;
SELECT * FROM rate_limit_system.silver.silver_query_events LIMIT 10;
```

**Prove incrementality** — the real test. Note the count, send exactly 5 more `/query`
requests, re-export, re-run the job. The count must rise by **exactly 5** — not double,
not reprocess the old files.

## Lane B — Debezium CDC (log-based)

**Requires:** Parts 5.1–5.4 complete, connector `Running`.

```cmd
databricks bundle run rate_limit_ingestion_job -t dev
```

**Verify the envelope parsed** — do this first, it's the usual failure point:
```sql
SELECT user_id, user_name, remaining_budget, is_active, __START_AT, __END_AT
FROM rate_limit_system.silver.silver_users_cdc ORDER BY user_id;
```

> ⚠️ **If rows exist but every column is NULL**, the envelope schema doesn't match the
> wire format. Inspect a real message and compare types field by field:
> ```sql
> SELECT _value FROM rate_limit_system.bronze.cdc_raw
> WHERE topic = 'rl.rate_limit.users' LIMIT 1;
> ```
> MySQL `BOOLEAN` is `TINYINT(1)` and arrives as `1`, not `true` — so the schema declares
> `is_active:int`. One wrong type nulls the **entire struct**, not just that field.

**The test the whole lane exists for.** Three rapid updates in MySQL:
```sql
UPDATE users SET remaining_budget = 4000 WHERE user_id = 2;
UPDATE users SET remaining_budget = 3000 WHERE user_id = 2;
UPDATE users SET remaining_budget = 2000 WHERE user_id = 2;
```
Re-run the job, then compare both lanes:
```sql
SELECT 'lane_a (snapshot)' AS lane, remaining_budget,
       __START_AT AS valid_from, __END_AT AS valid_to
FROM rate_limit_system.silver.silver_users WHERE user_id = 2
UNION ALL
SELECT 'lane_b (binlog)', remaining_budget,
       timestamp_millis(__START_AT), timestamp_millis(__END_AT)
FROM rate_limit_system.silver.silver_users_cdc WHERE user_id = 2
ORDER BY lane, valid_from;
```

| Lane | Result |
|---|---|
| **A** — snapshot diffing | one new version: `2000`. The 4000 and 3000 never existed to it |
| **B** — binlog capture | three versions: `4000`, `3000`, `2000` |

Same table, same window, two mechanisms — one sees history the other structurally cannot.
That single result set is the point of the whole project.

*(Lane B's `__START_AT` is epoch milliseconds because `sequence_by` uses Debezium's
`ts_ms`; Lane A's is a timestamp. Hence `timestamp_millis()` in the comparison.)*

---

# PART 8 — Day-to-day

Coming back to the project after a break:

```cmd
:: 1. MySQL service running?  (services.msc → MySQL80)

:: 2. Start the tunnel — leave open
cd backend
python ngrok_manager.py

:: 3. If the address changed, update the Confluent connector by hand and restart it

:: 4. Start the backend — leave open
uvicorn main:app --reload

:: 5. Generate some traffic at http://localhost:8000

:: 6. Export Lane C files
cd databricks\exporter
python export_to_volume.py

:: 7. Run everything
cd ..
databricks bundle run rate_limit_ingestion_job -t dev
```

**After changing any code**, `databricks bundle deploy -t dev` before running.

---

## Database design

| Table | Purpose |
|---|---|
| `plans` | Plan definitions and their hourly/daily/monthly limits |
| `users` | Users, their plan, and token budget |
| `users_usage` | Running token counters + rolling window start times |
| `query_log` | Business log: queries, chat messages, token usage |
| `request_log` | Technical log: every HTTP request + backend trace |

### Stored procedures (22)

- **Plans:** `sp_create_plan`, `sp_update_plan`, `sp_soft_delete_plan`, `sp_delete_plan`, `sp_migrate_users_to_plan`, `sp_get_all_plan`, `sp_get_plan_details`
- **Users:** `sp_create_user`, `sp_update_user`, `sp_add_budget`, `sp_delete_user`, `sp_get_user_details`, `sp_list_users`, `sp_get_remaining_limits`
- **Usage:** `sp_refresh_usage`, `sp_get_current_usage`, `sp_get_all_usage`
- **Validation & logging:** `sp_validate_request`, `sp_record_usage`, `sp_log_query`, `sp_get_query_history`

---

## Lakehouse tables

**Bronze** — raw, one table per source per lane

| Table | Lane |
|---|---|
| `snap_plans`, `snap_users`, `snap_users_usage` | A |
| `query_log_raw`, `request_log_raw` | C |
| `cdc_raw` (all five topics, unparsed envelope) | B |

**Silver** — cleaned and conformed; this is where the lanes merge

| Table | Type |
|---|---|
| `silver_plans`, `silver_users` | SCD2 from snapshots (Lane A) |
| `silver_users_usage` | SCD1 |
| `silver_query_events`, `silver_request_events` | deduped, typed, quality-checked (Lane C) |
| `silver_plans_cdc`, `silver_users_cdc` | SCD2 from binlog (Lane B) |

**Gold** — `gold_token_usage_hourly`, `gold_api_slo`

---

## The UI

| Tab | What it does |
|---|---|
| **Chat** | Pick a user, send prompts; each reply shows status + tokens used |
| **Procedures** | Run any of the 22 stored procedures with a form; MySQL-style output |
| **User Dashboard** | Live limits, usage counters, and query history for a user |
| **Admin Dashboard** | Every HTTP request with status, latency, and expandable backend trace |

---

## API endpoints (selected)

| Method | Path | Description |
|---|---|---|
| POST | `/query` | Full validate → LLM → record flow |
| GET | `/users` | List users |
| POST | `/users` | Create user |
| GET | `/users/{id}/limits` | Remaining limits |
| GET | `/users/{id}/chat-history` | Chat history |
| POST | `/admin/call-procedure` | Run any whitelisted procedure |
| GET | `/admin/request-logs` | All backend request traces |

---

## Known issues

**`query_log` records two rows per query.** `/query` calls `sp_record_usage`, which
internally calls `sp_log_query` (insert #1), and then calls `save_to_query_log`
(insert #2). Both rows carry the same `tokens_used`, so **`gold_token_usage_hourly`
double-counts every query.**

Fix at the source using the enum the schema already has — stamp
`message_type = 'procedure_exec'` inside `sp_log_query`, then filter
`message_type = 'llm_query'` in silver.

**Lakeflow Connect managed MySQL CDC does not work on Free Edition.** The connection and
pipeline create successfully and the ingestion gateway provisions, but it fails at runtime
writing internal state to managed Unity Catalog storage (`CloudStorageException`,
`response: null`). This is why Lane B is built by hand with Debezium. Details in
[TROUBLESHOOTING.md](TROUBLESHOOTING.md#25-lakeflow-connect-ingestion-gateway-failed).

---

## Operational rules worth remembering

**A checkpoint and its target table are one unit — reset both, or neither.** Applies to
Auto Loader's `_ckpt/` with its bronze table, and to DLT's internal checkpoint with its
silver table. Dropping a bronze table gives it a new UUID and breaks any stream reading
it (`DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`); the fix is **Full refresh selected
tables** on just the affected ones.

> ⚠️ Never **Full refresh all** — it rebuilds `silver_plans` / `silver_users` from
> scratch and destroys accumulated SCD2 history.

**Volume paths have four fixed segments:** `/Volumes/<catalog>/<schema>/<volume>/<path>`.
The third segment is the volume name, not a folder — which is why `_schema/` and `_ckpt/`
live *inside* `landing`.

---

## Notes

- Logging is done in the backend (middleware + helpers), so **no stored procedure was
  modified** to add tracing — the procedures stay focused on business logic.
- The LLM call is **simulated** (token count estimated from prompt length). Swap the
  Step-2 block in `main.py` for a real Anthropic/OpenAI call to go live.
- All ingestion runs on `trigger(availableNow=True)` rather than continuously. Free
  Edition has usage caps and auto-terminating compute, so continuous streams aren't
  viable — this gives identical streaming semantics on a scheduled cadence.

---

## Further reading

| Document | Contents |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Lane design, the three kinds of CDC, medallion model, concept glossary |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | All 26 problems hit during the build, with cause and fix |
