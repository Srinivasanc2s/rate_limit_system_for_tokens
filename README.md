# Token Rate Limit System

A token-based **rate limiting system** for LLM APIs, built as a system-design exercise.
The rate-limit logic lives entirely in **MySQL stored procedures**; a thin **FastAPI**
backend orchestrates the request flow and a single-page **HTML UI** drives it.

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

## Tech stack

| Layer     | Technology                          |
|-----------|-------------------------------------|
| Database  | MySQL (stored procedures)           |
| Backend   | FastAPI (Python)                    |
| Frontend  | Plain HTML + vanilla JS (no build)  |

---

## Project structure

```
.
├── Rate_limit_sql2.sql      # Schema + all 22 stored procedures + test cases
├── backend/
│   ├── main.py              # FastAPI app (endpoints, logging middleware)
│   ├── index.html           # Single-page UI (Chat / Procedures / Dashboards)
│   ├── requirements.txt     # Python dependencies
│   └── .env.example         # Copy to .env and fill in your DB credentials
└── README.md
```

---

## Database design

| Table         | Purpose                                                   |
|---------------|-----------------------------------------------------------|
| `plans`       | Plan definitions and their hourly/daily/monthly limits    |
| `users`       | Users, their plan, and token budget                       |
| `users_usage` | Running token counters + rolling window start times       |
| `query_log`   | Business log: queries, chat messages, token usage         |
| `request_log` | Technical log: every HTTP request + backend trace         |

### Stored procedures (22)

- **Plans:** `sp_create_plan`, `sp_update_plan`, `sp_soft_delete_plan`, `sp_delete_plan`, `sp_migrate_users_to_plan`, `sp_get_all_plan`, `sp_get_plan_details`
- **Users:** `sp_create_user`, `sp_update_user`, `sp_add_budget`, `sp_delete_user`, `sp_get_user_details`, `sp_list_users`, `sp_get_remaining_limits`
- **Usage:** `sp_refresh_usage`, `sp_get_current_usage`, `sp_get_all_usage`
- **Validation & logging:** `sp_validate_request`, `sp_record_usage`, `sp_log_query`, `sp_get_query_history`

---

## Setup

### 1. Database
Load the schema and procedures into MySQL:
```bash
mysql -u root -p < Rate_limit_sql2.sql
```

### 2. Backend
```bash
cd backend
python -m venv .venv
.venv\Scripts\activate         # Windows
# source .venv/bin/activate    # macOS / Linux

pip install -r requirements.txt

cp .env.example .env           # then edit .env with your MySQL password
```

`.env`:
```
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=rate_limit
```

### 3. Run
```bash
uvicorn main:app --reload
```
Open **http://localhost:8000**

---

## The UI

| Tab                | What it does                                                        |
|--------------------|--------------------------------------------------------------------|
| **Chat**           | Pick a user, send prompts; each reply shows status + tokens used   |
| **Procedures**     | Run any of the 22 stored procedures with a form; MySQL-style output |
| **User Dashboard** | Live limits, usage counters, and query history for a user          |
| **Admin Dashboard**| Every HTTP request with status, latency, and expandable backend trace |

---

## API endpoints (selected)

| Method | Path                          | Description                          |
|--------|-------------------------------|--------------------------------------|
| POST   | `/query`                      | Full validate → LLM → record flow    |
| GET    | `/users`                      | List users                           |
| POST   | `/users`                      | Create user                          |
| GET    | `/users/{id}/limits`          | Remaining limits                     |
| GET    | `/users/{id}/chat-history`    | Chat history                         |
| POST   | `/admin/call-procedure`       | Run any whitelisted procedure        |
| GET    | `/admin/request-logs`         | All backend request traces           |

---

## Notes

- Logging is done in the backend (middleware + helpers), so **no stored procedure was
  modified** to add tracing — the procedures stay focused on business logic.
- The LLM call is **simulated** (token count estimated from prompt length). Swap the
  Step-2 block in `main.py` for a real Anthropic/OpenAI call to go live.
