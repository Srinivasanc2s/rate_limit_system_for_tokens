# Rate Limit Lakehouse — Architecture & Concepts

A reference for the Databricks side of the token rate-limit project.
Explains **where you stand**, **what each lane does**, **which tables go where**, and
**why the design is what it is**.

---

## Part 1 — Where you stand right now

### What you built

You exposed a MySQL instance running on your laptop through an ngrok TCP tunnel, and read
from it inside a Databricks notebook using Spark's JDBC data source. It worked.

### What that method is actually called

There are **three separate things** stacked here, and it helps to name them separately
because people conflate them:

| Layer | What it is | What it is NOT |
|---|---|---|
| **ngrok** | A TCP tunnel / reverse proxy. Gives your `localhost:3306` a temporary public address | Not an ingestion tool. It moves bytes, nothing else |
| **JDBC** | Java Database Connectivity — the standard API for talking to relational databases | Not Databricks-specific. Same thing your FastAPI app does, different language |
| **Spark JDBC data source** | Spark's built-in reader that turns a SQL query into a DataFrame | Not a "connector product". It's a library, not a service |

So the honest name for what you did is:

> **Query-based batch ingestion over a JDBC connection, tunnelled via ngrok.**

Nobody says that out loud. In practice people call it **"a JDBC pull"** or
**"a JDBC batch read."**

### What it is equivalent to, in other tools

If you've seen these elsewhere, they're the same idea:

| Tool | Their name for it |
|---|---|
| AWS Glue | JDBC Connection |
| Azure Data Factory | Copy Activity + Linked Service |
| Hadoop (legacy) | Sqoop import |
| Fivetran / Airbyte | JDBC / database source connector |
| **Databricks, governed version** | **Lakehouse Federation** (`CREATE CONNECTION` + foreign catalog) |

That last row matters. Lakehouse Federation is the *same JDBC underneath*, but registered
in Unity Catalog so you get governance, lineage, and permissions. If it's available in
Free Edition, it's a strict upgrade over the raw JDBC read. Worth testing.

### What is JDBC ingestion generally FOR?

The realistic use cases, in order of how common they are:

1. **Loading dimension / reference tables** — small, slowly-changing, cheap to re-read whole
2. **One-time migrations and backfills** — move history once, then switch to CDC
3. **Ad-hoc exploration** — "let me just look at what's in the prod DB"
4. **Writing results back** to an operational database

What it's **not** good for: continuously ingesting high-volume event tables. Every read is
a `SELECT` against your live OLTP database, and it's batch-only — there's no
`spark.readStream.format("jdbc")`. That constraint is the whole reason Lanes B and C exist.

### Your current status

```
[x] MySQL schema + stored procedures        (Rate_limit_sql2.sql)
[x] FastAPI backend + HTML UI               (backend/)
[x] ngrok tunnel, JDBC read verified        <- you are here
[x] Code pasted through Phase 4             (databricks/)
[ ] Bundle deployed                         (Phase 5)
[ ] Data flowing end to end                 (Phase 6-7)
[ ] Lane B (Kafka/Debezium)
[ ] Governance: column masks, tags
[ ] AI/BI dashboard + Genie
```

---

## Part 2 — The mental model (read this before anything else)

### The lanes are three **mechanisms**, not three copies of the data

This is the question you asked, and it's the right question. The answer:

> **Lane A and Lane C are complementary — they own different tables and never overlap.**
> **Lane B is a deliberate parallel alternative — it re-ingests the same data by a
> different mechanism, into separate tables.**

In a real production system you would choose **A + C**, *or* **B**. Not both.
You are building both **on purpose**, because the point of this project is to have
hands-on experience with each mechanism and be able to explain the tradeoff.

Nothing collides, because each lane writes to **its own bronze tables**.

### Table ownership — the definitive matrix

| MySQL table | Rows | Nature | Owned by | Why that lane |
|---|---|---|---|---|
| `plans` | ~5 | Dimension, mutable | **Lane A** | Tiny. Full snapshot is cheaper than any incremental logic |
| `users` | ~100s | Dimension, mutable | **Lane A** | Small. Budget/plan changes need SCD2 history |
| `users_usage` | 1/user | Counter, mutable | **Lane A** | Small. Only current state matters |
| `query_log` | High, growing | Event, append-only | **Lane C** | Large. Re-reading whole table every run would be wasteful |
| `request_log` | High, growing | Event, append-only | **Lane C** | Same. Plus JSON columns that need schema-drift tolerance |
| *(all five)* | — | — | **Lane B** | Alternative path. Lands in its own `cdc_raw` table |

### Why split A and C that way

The split is not arbitrary — it follows one rule:

> **Small + mutable → snapshot it. Large + append-only → stream it incrementally.**

- Snapshotting `plans` (5 rows) every 15 min costs nothing, and gives you deletes for free.
- Snapshotting `query_log` (growing forever) every 15 min would re-transfer the entire
  table every single run. Absurd.
- Incrementally reading `plans` with a watermark would miss updates entirely, because
  updates don't change the primary key.

Each table gets the mechanism that fits its shape.

---

## Part 3 — The three kinds of CDC (the conceptual core)

"CDC" means three genuinely different things. You are implementing all three, which is
why this project is worth doing.

| | **Query-based CDC** | **Snapshot-based CDC** | **Log-based CDC** |
|---|---|---|---|
| How it detects change | `WHERE id > watermark` | Diff two consecutive full copies | Read the DB transaction log (binlog) |
| Used in | **Lane C** (exporter) | **Lane A** (dims) | **Lane B** (Debezium) |
| Databricks feature | your own watermark file | `AUTO CDC FROM SNAPSHOT` | `AUTO CDC` (`APPLY CHANGES`) |
| Sees INSERTs | Yes | Yes | Yes |
| Sees UPDATEs | **No** — key doesn't change | Yes (net result only) | Yes (every one) |
| Sees DELETEs | **No** | Yes | Yes |
| Sees intermediate states | **No** | **No** | **Yes** |
| Load on source DB | `SELECT` per poll | Full `SELECT` per poll | Near zero |
| Setup complexity | Trivial | Low | High |

### The concrete consequence for your data

`users_usage.hourly_token` increments on **every single API request**.

- Poll every 15 min with **snapshot CDC** → 200 requests collapse into **one** number.
  The 199 intermediate values are gone forever.
- **Log-based CDC** captures all 200.

That is the entire argument for Lane B. Not "streaming is cooler" — it's that snapshot
diffing structurally cannot see history that happened between polls.

---

## Part 4 — Lane A: JDBC batch (DONE, working)

### Flow

```
  MySQL on your laptop
  localhost:3306
  tables: plans, users, users_usage
        │
        ▼
  ngrok TCP tunnel
  4.tcp.ngrok.io:18342  ──►  forwards to localhost:3306
        │                     (public address, rotates on restart)
        ▼
  Databricks serverless job task
  FILE: databricks/src/jobs/ingest_dims_jdbc.py
        │  reads secrets: rl/ngrok_host, rl/ngrok_port,
        │                 rl/mysql_user, rl/mysql_password
        │  spark.read.format("jdbc")
        │  .write.mode("overwrite")     <-- full replace, every run
        ▼
  BRONZE
  rate_limit.bronze.snap_plans
  rate_limit.bronze.snap_users
  rate_limit.bronze.snap_users_usage
        │
        ▼
  Lakeflow Declarative Pipeline
  FILE: databricks/src/pipelines/ldp_rate_limit.py
        │  dlt.create_auto_cdc_from_snapshot_flow(...)
        │  <-- compares this run's snapshot to last run's, derives changes
        ▼
  SILVER
  rate_limit.silver.silver_plans        (SCD Type 2)
  rate_limit.silver.silver_users        (SCD Type 2)
  rate_limit.silver.silver_users_usage  (SCD Type 1)
```

### Key concept: why `mode("overwrite")` is correct here

It looks wrong — you're throwing away data every run. You're not.

`AUTO CDC FROM SNAPSHOT` wants the **current state** of the table each run. It keeps its
own memory of what the previous snapshot looked like, diffs them, and writes the derived
history into the silver SCD2 table. Bronze is a scratch buffer; **silver holds the history.**

### Assumptions Lane A makes

1. ngrok is running **at the moment the job fires**. If not, the task fails.
2. The ngrok address in secrets is current. Free-tier ngrok rotates on every restart.
3. The dimension tables stay small. If `users` grew to millions, full snapshots would stop
   being viable and you'd move it to Lane B.
4. MySQL is reachable from Databricks' serverless egress.

### Known fragility

This is the least robust lane, by design — it's the one that proved connectivity fastest.
Every run depends on your laptop being awake, MySQL running, and ngrok holding the same
address. That's acceptable for dimensions (a missed run just means slightly stale plan
data) and unacceptable for events (a missed run would lose data permanently) — which is
exactly why events went to Lane C.

---

## Part 5 — Lane C: Files + Auto Loader (NEXT)

### Flow

```
  MySQL on your laptop
  localhost:3306
  tables: query_log, request_log
        │
        │  NOTE: this half runs ON YOUR LAPTOP.
        │  No ngrok needed - it connects to localhost directly.
        ▼
  FILE: databricks/exporter/export_to_volume.py
        │  reads .export_state.json  -> last seen q_id / log_id
        │  SELECT * FROM query_log WHERE q_id > <watermark> LIMIT 10000
        │  serialises rows to NDJSON (one JSON object per line)
        │  uploads over HTTPS via Databricks Files API
        │  writes new watermark back to .export_state.json
        ▼
  UNITY CATALOG VOLUME  (cloud storage, inside Databricks)
  /Volumes/rate_limit/bronze/landing/query_log/query_log_000000000001_000000000018.json
  /Volumes/rate_limit/bronze/landing/request_log/request_log_...json
        │
        │  <-- files sit here durably. Databricks reads them on ITS schedule.
        │      Your laptop can be closed. This is the decoupling.
        ▼
  Databricks serverless job task
  FILE: databricks/src/jobs/ingest_events_autoloader.py
        │  spark.readStream.format("cloudFiles")   <-- Structured Streaming
        │  checkpoint at /Volumes/rate_limit/bronze/_ckpt/<table>
        │  trigger(availableNow=True)  -> process what exists, then stop
        ▼
  BRONZE
  rate_limit.bronze.query_log_raw
  rate_limit.bronze.request_log_raw
        │
        ▼
  Lakeflow Declarative Pipeline
  FILE: databricks/src/pipelines/ldp_rate_limit.py
        │  @dlt.table + @dlt.expect_or_drop + dropDuplicatesWithinWatermark
        ▼
  SILVER
  rate_limit.silver.silver_query_events
  rate_limit.silver.silver_request_events
```

### Key concept: Lane C has TWO watermarks, not one

This trips people up. Auto Loader does not make incrementality automatic end-to-end —
it makes the *second hop* automatic.

| Hop | Who tracks "what's new" | Where it's stored |
|---|---|---|
| MySQL rows → files | **your exporter script** | `.export_state.json` on your laptop |
| files → Delta table | **Auto Loader** | RocksDB checkpoint in the Volume |

You still write watermark logic. What you get for free is everything after the file lands.

### Key concept: Auto Loader ingests FILES, never rows

Auto Loader has no JDBC anything. It watches a directory and asks one question:
*which files here have I not processed yet?*

The files don't appear by magic — **your exporter manufactures them.**

Its value is not scale (you'll have dozens of files, not millions). Its value is:

| | Plain `spark.read.json()` | Auto Loader |
|---|---|---|
| 500 files present, 3 new | Reads all 500 | Reads 3 |
| Tracking what's processed | You write it | Automatic checkpoint |
| Job crashes mid-run | Unknown state, likely duplicates | Resumes exactly where it stopped |
| New column appears in the JSON | Silently dropped or job fails | Lands in `_rescued_data` |

### Key concept: this IS streaming

`spark.readStream.format("cloudFiles")` is genuine Structured Streaming — real
checkpoints, real exactly-once, real incremental processing. It is not "batch pretending".

`trigger(availableNow=True)` means: process every file that exists right now, then stop.
Run it again and it picks up only what arrived since. **Streaming semantics on a batch
schedule** — which is what most production teams actually run, and which protects your
Free Edition quota from a 24/7 stream.

### Assumptions Lane C makes

1. `q_id` and `log_id` are monotonically increasing and never reused. (They're
   `AUTO_INCREMENT` primary keys, so yes.)
2. Rows are never updated or deleted after insert. True for both event tables.
3. Filenames are deterministic (`{table}_{first_id}_{last_id}.json`) — so a crashed run
   that retries overwrites the same file instead of creating a duplicate batch.
4. `BATCH_SIZE = 10_000` per run per table. Backfilling more means running it repeatedly.

---

## Part 6 — Lane B: Kafka + Debezium (LATER)

### Flow

```
  MySQL binlog  (the transaction log MySQL already writes for replication)
        │
        │  requires: log_bin=ON, binlog_format=ROW, binlog_row_image=FULL
        │  requires: a user with REPLICATION SLAVE, REPLICATION CLIENT
        ▼
  Debezium MySQL connector
  EITHER: self-managed in Docker on your laptop  (free, robust, no ngrok)
  OR:     Confluent fully-managed connector via ngrok  (fast, burns credits)
  FILES: databricks/cdc/docker-compose.yml
         databricks/cdc/debezium-mysql.json
        │  emits one message per row change:
        │  { "before": {...}, "after": {...}, "op": "c|u|d|r", "ts_ms": ... }
        ▼
  CONFLUENT CLOUD topics
  rl.rate_limit.plans
  rl.rate_limit.users
  rl.rate_limit.users_usage
  rl.rate_limit.query_log
  rl.rate_limit.request_log
        │
        │  <-- durable buffer. Retains events. Databricks consumes when it wants.
        ▼
  Databricks serverless job task
  FILE: databricks/src/jobs/ingest_kafka_cdc.py
        │  spark.readStream.format("kafka")
        │  trigger(availableNow=True)
        ▼
  BRONZE
  rate_limit.bronze.cdc_raw      <-- ONE table, all topics, unparsed envelope
        │
        ▼
  Lakeflow Declarative Pipeline
        │  parse the Debezium envelope -> extract op / ts_ms / after.*
        │  dlt.create_auto_cdc_flow(...)
        ▼
  SILVER
  rate_limit.silver.silver_users_cdc     <-- note the _cdc suffix
  rate_limit.silver.silver_plans_cdc         these do NOT collide with Lane A's
```

### Key concept: `op` codes

| `op` | Meaning |
|---|---|
| `r` | **Read** — emitted during the initial snapshot/backfill |
| `c` | **Create** — INSERT |
| `u` | **Update** — UPDATE |
| `d` | **Delete** — DELETE |

`snapshot.mode: initial` gives you a full backfill (all `r`) and then live changes
(`c`/`u`/`d`) from one connector. Backfill and streaming, one config.

### Why bronze is ONE raw table

`cdc_raw` holds the unparsed JSON envelope for all five topics. Parsing happens in silver.

Reason: if Debezium's schema changes or you add a table, ingestion never breaks — you're
just storing bytes. All the fragile parsing logic lives downstream where a failure is
recoverable by editing the pipeline, not by losing data.

### Assumptions Lane B makes

1. MySQL binlog is enabled with `ROW` format and `FULL` row image.
2. Confluent Cloud is reachable from Databricks serverless on port 9092 — **verify this
   first with a 10-line smoke test before building anything else.**
3. Docker Desktop is installed and running (self-managed path).
4. Connect's internal topics (`connect-configs`, `connect-offsets`, `connect-status`) are
   created manually in Confluent — auto-create is disabled there. This is the #1 setup failure.

---

## Part 7 — How the three lanes converge

```
                        MySQL  (laptop)
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
    LANE A               LANE C               LANE B
    JDBC pull        exporter + files      binlog + Kafka
   (dims only)        (events only)         (all tables)
        │                    │                    │
        ▼                    ▼                    ▼
   bronze.snap_*      bronze.*_raw          bronze.cdc_raw
        │                    │                    │
        └────────────────────┴────────────────────┘
                             │
                             ▼
             ┌───────────────────────────────┐
             │  LAKEFLOW DECLARATIVE PIPELINE│
             │  ldp_rate_limit.py            │
             └───────────────┬───────────────┘
                             ▼
    SILVER   silver_query_events   silver_plans (SCD2)
             silver_request_events silver_users (SCD2)
             silver_request_spans  silver_users_usage (SCD1)
                             │
                             ▼
    GOLD     gold_token_usage_hourly   gold_api_slo
             gold_plan_utilization     gold_budget_burndown
             gold_rate_limit_violations
                             │
                             ▼
             AI/BI Dashboard  +  Genie space
```

### Medallion responsibilities

| Layer | Rule | What lives here |
|---|---|---|
| **Bronze** | Raw. Never transform. One table per source-per-lane | `snap_*`, `*_raw`, `cdc_raw` |
| **Silver** | Clean, type, dedupe, conform. **This is where the lanes merge** | `silver_*` |
| **Gold** | Business aggregates. Recomputed in full, cheap at your volume | `gold_*` |

The single most important line: **bronze is per-lane, silver is shared.** Bronze is where
the ingestion mechanism is still visible. Silver is where it stops mattering — downstream
consumers don't know or care whether a row arrived via JDBC, a file, or Kafka.

Every bronze table carries `_ingested_at` and `_source_lane` so you can always trace
provenance if something looks wrong.

### SCD Type 2 — why it matters for THIS project

`silver_plans` is SCD Type 2, meaning it keeps `__START_AT` / `__END_AT` for every version
of every plan.

That is what lets a gold table answer:

> *"Was this request over the limit **as the limit stood at that moment**?"*

If you only kept current state and someone raised the hourly limit from 1000 to 5000 last
week, every historical violation would retroactively look compliant. SCD2 prevents that.

---

## Part 8 — File map

```
SQL/
├── Rate_limit_sql2.sql                 MySQL schema + stored procedures
├── backend/
│   ├── main.py                         FastAPI. LoggingMiddleware -> request_log
│   └── index.html                      demo UI
├── plan.md                             the original design doc
├── ARCHITECTURE.md                     this file
└── databricks/
    ├── databricks.yml                  DAB root: bundle name, targets
    ├── resources/
    │   ├── rate_limit_job.yml          Lakeflow Job: 3 tasks + schedule
    │   └── rate_limit_pipeline.yml     LDP pipeline definition
    ├── src/
    │   ├── jobs/
    │   │   ├── ingest_dims_jdbc.py         LANE A  -> bronze.snap_*
    │   │   ├── ingest_events_autoloader.py LANE C  -> bronze.*_raw
    │   │   └── ingest_kafka_cdc.py         LANE B  -> bronze.cdc_raw   (later)
    │   └── pipelines/
    │       └── ldp_rate_limit.py       bronze -> silver -> gold
    ├── exporter/
    │   ├── export_to_volume.py         LANE C laptop half: rows -> NDJSON -> Volume
    │   ├── .env                         creds (gitignored)
    │   └── .export_state.json           watermarks (gitignored, auto-created)
    └── cdc/                             LANE B (later)
        ├── docker-compose.yml
        └── debezium-mysql.json
```

### What runs where

| File | Runs on | Triggered by |
|---|---|---|
| `export_to_volume.py` | **Your laptop** | You, manually (or a local scheduler) |
| `ingest_dims_jdbc.py` | Databricks serverless | Lakeflow Job task |
| `ingest_events_autoloader.py` | Databricks serverless | Lakeflow Job task |
| `ldp_rate_limit.py` | Databricks serverless | Lakeflow Job pipeline task |

The exporter is the only piece that runs outside Databricks. That's deliberate — it's the
only piece that needs to reach `localhost`.

---

## Part 9 — Orchestration: the job DAG

```
  ┌──────────────────────┐     ┌────────────────────────────┐
  │  ingest_dims_jdbc    │     │ ingest_events_autoloader   │
  │  (Lane A)            │     │ (Lane C)                   │
  └──────────┬───────────┘     └─────────────┬──────────────┘
             │                                │
             └───────────────┬────────────────┘
                             ▼
                  ┌─────────────────────┐
                  │  run_ldp_pipeline   │
                  │  bronze -> silver   │
                  │        -> gold      │
                  └─────────────────────┘
```

The two ingestion tasks run **in parallel** (no dependency between them — different
tables, different mechanisms). The pipeline waits for both, because silver reads from
bronze tables that both tasks populate.

Defined in `resources/rate_limit_job.yml` via `depends_on`. Schedule: every 15 min,
starts `PAUSED` so you can run it by hand until it's proven.

---

## Part 10 — Concept glossary

| Term | Meaning in this project |
|---|---|
| **JDBC** | Standard API for connecting to relational databases. Lane A's transport |
| **ngrok** | TCP tunnel giving `localhost:3306` a temporary public address |
| **Watermark (high-water mark)** | The last-seen ID, so the next read starts after it. `.export_state.json` |
| **Watermark (streaming)** | Different thing! `withWatermark("event_ts", "1 hour")` — how long to wait for late data before dropping state |
| **Checkpoint** | Auto Loader / Kafka's own record of progress. Lives in the Volume |
| **NDJSON** | Newline-delimited JSON. One object per line. What the exporter writes |
| **Auto Loader (`cloudFiles`)** | Structured Streaming source that incrementally discovers new files |
| **`trigger(availableNow=True)`** | Process everything available, then stop. Not continuous |
| **Volume** | Unity Catalog-governed cloud storage you can read/write as file paths |
| **Medallion** | Bronze (raw) → Silver (clean) → Gold (aggregate) |
| **SCD Type 1** | Overwrite. Only current state kept |
| **SCD Type 2** | Keep every version with valid-from/valid-to timestamps |
| **`__START_AT` / `__END_AT`** | The SCD2 validity columns AUTO CDC generates |
| **CDC** | Change Data Capture. Three flavours — see Part 3 |
| **Debezium envelope** | `{before, after, op, ts_ms, source}` — one message per row change |
| **LDP / DLT** | Lakeflow Declarative Pipelines. Formerly Delta Live Tables |
| **Expectation** | `@dlt.expect_or_drop(...)` — declarative data quality rule |
| **`AUTO CDC`** | Consumes a change feed, applies SCD1/SCD2. Was `APPLY CHANGES INTO` |
| **`AUTO CDC FROM SNAPSHOT`** | Diffs consecutive snapshots, derives changes. No CDC source needed |
| **DAB** | Databricks Asset Bundle. Infrastructure-as-code: YAML → jobs, pipelines |
| **Secret scope** | Encrypted key-value store. `dbutils.secrets.get("rl", "mysql_password")` |
| **`_rescued_data`** | Auto Loader column holding fields that didn't match the known schema |
| **Lakehouse Federation** | UC-governed JDBC. Query MySQL as a foreign catalog |

---

## Part 11 — Requirement traceability

Mapping your original learning goals to where each is actually demonstrated:

| Requirement | Where | Status |
|---|---|---|
| Batch ingestion | Lane A — JDBC snapshot reads | Done |
| Streaming ingestion | Lane C (Auto Loader is Structured Streaming) | Next |
| Streaming ingestion, event-driven | Lane B — Kafka source | Later |
| CDC | 3 flavours: snapshot (A), query-based (C), log-based (B) | Partial |
| Data processing / declarative pipelines | `ldp_rate_limit.py` — `@dlt.table`, expectations | Next |
| Data quality | `@dlt.expect_or_drop` / `expect` / `expect_or_fail` | Next |
| SCD Type 2 | `silver_plans`, `silver_users` | Next |
| Orchestration | `rate_limit_job.yml` — DAG, `depends_on`, retries, alerts | Next |
| Lakeflow Jobs | same | Next |
| Unity Catalog governance | catalog/schema/volume, secret scope, lineage | Partial |
| Column masking / PII | `user_message`, `llm_response`, `client_ip` | Later |
| Dashboards | gold tables → AI/BI + Genie | Later |
| CI/CD deployment | DAB (`databricks.yml` + `resources/`) | Next |

---

## Part 12 — Assumptions and constraints (global)

### Databricks Free Edition

1. **Serverless only.** No classic clusters, no init scripts, no cluster-scoped JARs.
2. **No VPN or ingestion gateway.** Cannot reach `localhost`. Everything either gets
   pushed out, or the source becomes publicly reachable.
3. **Lakeflow Connect managed DB connectors: unavailable.** They need a gateway on
   classic compute plus a Premium tier.
4. **Usage caps + auto-terminating compute.** Never run continuous streams. Always
   `trigger(availableNow=True)` on a schedule.
5. **Unverified — test before relying on:** row filters / column masks, Lakehouse
   Federation foreign catalogs, `system.*` tables, Kafka source on serverless.

### This project

1. MySQL runs on your laptop. Any lane that pulls needs the laptop awake.
2. ngrok free tier rotates its address on restart. Stored in secrets so it's a one-command
   fix, no redeploy.
3. Event tables are strictly append-only. If that ever changes, Lane C's watermark logic
   breaks silently and you'd need Lane B.
4. Data volumes are small. Gold tables are recomputed in full each run — correct and cheap
   here, would need incrementalisation at real scale.

---

## Part 13 — Immediate next steps

1. `databricks bundle validate` — catch YAML errors before deploying
2. `databricks bundle deploy -t dev`
3. Generate data: hit `/query` ~20 times
4. `python export_to_volume.py` — confirm files land in the Volume
5. `databricks bundle run rate_limit_ingestion_job -t dev`
6. Verify each layer bronze → silver → gold
7. **The real test:** more API calls → re-export → re-run. Row counts should increase by
   exactly the number of new rows, with no double-counting. That proves incrementality
   across both watermarks.

Only after step 7 passes: add the remaining gold tables, then Lane B.
