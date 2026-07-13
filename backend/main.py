import os
import random
import uuid
import json

from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
#from fastapi.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import mysql.connector
load_dotenv()
# ---------------------------------------------------------------------------
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import time

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = generate_request_id()
        start_time = time.time()
        
        # Store in request state (accessible in endpoint handlers)
        request.state.request_id = request_id
        request.state.backend_trace = []
        request.state.user_id = None
        
        # Extract client IP
        client_ip = request.client.host if request.client else "unknown"
        
        # Capture request body for logging
        request_body = None
        if request.method in ["POST", "PUT", "DELETE"]:
            try:
                request_body = await request.body()
                request.state.request_body = request_body
            except:
                pass
        
        # Call the actual handler
        response = await call_next(request)
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Extract status and message from response
        status_code = response.status_code
        status_message = "OK" if status_code < 400 else "Error"
        
        # Log to database (async, so don't block response)
        try:
            log_request_to_db(
                request_id=request_id,
                user_id=request.state.user_id,
                http_method=request.method,
                endpoint=request.url.path,
                request_params=request.query_params._dict if request.query_params else None,
                response_status_code=status_code,
                response_message=status_message,
                backend_trace=request.state.backend_trace,
                execution_time_ms=execution_time_ms,
                client_ip=client_ip
            )
        except:
            pass  # Don't fail the response if logging fails
        
        return response



# ---------------------------------------------------------------------------

app = FastAPI(title="Token Rate Limit API")
app.add_middleware(LoggingMiddleware)
app.add_middleware(CORSMiddleware, 
                   allow_origins=["*"], 
                   allow_methods=["*"], 
                   allow_headers=["*"])


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rate_limit"),
        autocommit=False,
    )


def drain(cur):
    """Consume all result sets left open after 
    callproc (required before SELECT)."""
    for result in cur.stored_results():
        result.fetchall()


def get_rows(cur):
    """Return all rows from the last stored procedure's 
    result set."""
    rows = []
    for result in cur.stored_results():
        rows = result.fetchall()
    return rows


def call_proc_with_out(cur, proc_name, params):
    """Call procedure with OUT param by using user variables.
    Returns (rows, out_value) tuple.
    """
    var_name = "@_out_result"
    param_str = ", ".join(["%s"] * len(params)) + f", {var_name}"
    cur.execute(f"CALL {proc_name}({param_str})", params)
    drain(cur)
    cur.execute(f"SELECT {var_name} AS val")
    result = cur.fetchone()
    out_val = result["val"] if result else None
    return out_val


# ---------------------------------------------------------------------------
# LOGGING HELPERS
# ---------------------------------------------------------------------------

def generate_request_id() -> str:
    """Generate a short unique trace ID for correlating a request's logs."""
    return str(uuid.uuid4())[:12]


def log_request_to_db(
    request_id: str,
    user_id: Optional[int],
    http_method: str,
    endpoint: str,
    request_params: Optional[Dict[str, Any]],
    response_status_code: int,
    response_message: str,
    backend_trace: List[Dict[str, Any]],
    execution_time_ms: int,
    client_ip: str = "unknown",
):
    """Insert one row into request_log (technical trace of an HTTP request)."""
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute(
            """
            INSERT INTO request_log (
                request_id, user_id, http_method, endpoint,
                request_params, response_status_code, response_message,
                backend_trace, execution_time_ms, client_ip
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                request_id,
                user_id,
                http_method,
                endpoint,
                json.dumps(request_params) if request_params else None,
                response_status_code,
                response_message,
                json.dumps(backend_trace) if backend_trace else None,
                execution_time_ms,
                client_ip,
            ),
        )
        db.commit()
    except Exception as e:
        # Logging must never break the request itself.
        print(f"[LOGGING ERROR] request_log: {e}")
    finally:
        db.close()


def save_to_query_log(
    user_id: int,
    plan_id: Optional[int],
    query_status: str,
    tokens_used: int,
    conversation_id: str,
    user_message: str,
    llm_response: str,
    message_type: str = "llm_query",
    allow_budget_override: bool = False,
    validation_result: Optional[str] = None,
    execution_time_ms: int = 0,
):
    """Save the full chat/query record into the extended query_log columns."""
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute(
            """
            INSERT INTO query_log (
                user_id, plan_id, query_status, tokens_used,
                conversation_id, user_message, llm_response, message_type,
                allow_budget_override, validation_result, execution_time_ms,
                is_successful, time_stamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                user_id,
                plan_id,
                query_status,
                tokens_used,
                conversation_id,
                user_message,
                llm_response,
                message_type,
                allow_budget_override,
                validation_result,
                execution_time_ms,
                query_status == "success",
            ),
        )
        db.commit()
    except Exception as e:
        print(f"[LOGGING ERROR] query_log: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    user_id: int
    prompt: str
    allow_budget: bool = False

class PlanCreate(BaseModel):
    plan_name: str
    hourly_limit: int
    daily_limit: int
    monthly_limit: int

class PlanUpdate(BaseModel):
    hourly_limit: int
    daily_limit: int
    monthly_limit: int

class UserCreate(BaseModel):
    user_name: str
    plan_id: int
    budget: int

class UserUpdate(BaseModel):
    user_name: str
    plan_id: int
    budget: int

class BudgetAdd(BaseModel):
    tokens: int

class MigratePlan(BaseModel):
    old_plan_id: int
    new_plan_id: int

class ProcCall(BaseModel):
    proc_name: str
    params: List[Any] = []
    has_out: bool = False   # True if last arg is an OUT parameter


# ---------------------------------------------------------------------------
# CORE: LLM query flow
# The gate that every request must pass through:
#   1. sp_validate_request  — is this user allowed to send a request?
#   2. Simulate LLM call    — get a response and a token count
#   3. sp_record_usage      — deduct tokens under a row-level lock
#
# The FOR UPDATE inside sp_record_usage serializes concurrent requests
# for the same user at the DB level — no per-user mutex needed here.
# ---------------------------------------------------------------------------

@app.post("/query")
def send_query(req: QueryRequest, request: Request):
    db = get_db()
    start_time = time.time()
    conversation_id = str(uuid.uuid4())[:20]
    
    def trace(message: str, level: str = "INFO"):
        """Add message to backend trace."""
        request.state.backend_trace.append({
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message
        })
    
    try:
        request.state.user_id = req.user_id  # Set for logging middleware
        cur = db.cursor(dictionary=True)
        
        trace(f"[START] User {req.user_id} sending query (allow_budget={req.allow_budget})")
        trace(f"Prompt: {req.prompt[:100]}...")
        
        # --- Step 1: Validation ---
        trace("Calling sp_validate_request...")
        try:
            validate_msg = call_proc_with_out(cur, "sp_validate_request", (req.user_id, req.allow_budget))
            trace(f"Validation result: {validate_msg}")
        except mysql.connector.Error as e:
            trace(f"ERROR in validate_request: {str(e)}", level="ERROR")
            raise HTTPException(400, f"DB error in validate_request: {str(e)}")

        if not validate_msg:
            trace("ERROR: User not found or inactive", level="ERROR")
            raise HTTPException(404, f"User {req.user_id} not found, inactive, or no plan assigned")

        if validate_msg not in ("ALLOWED", "ALLOWED_USING_BUDGET"):
            trace(f"Request blocked: {validate_msg}", level="WARN")
            return {"status": "blocked", "validate": validate_msg}

        # --- Step 2: LLM Simulation ---
        trace("Simulating LLM call...")
        tokens_used = max(50, len(req.prompt.split()) * 10 + random.randint(10, 200))
        llm_response = f"[Simulated LLM] You said: \"{req.prompt[:80]}\" — used {tokens_used} tokens."
        trace(f"LLM simulation complete: {tokens_used} tokens")

        # --- Step 3: Record Usage ---
        trace("Calling sp_record_usage (with row lock)...")
        try:
            record_msg = call_proc_with_out(cur, "sp_record_usage", (req.user_id, tokens_used, req.allow_budget))
            trace(f"Record usage result: {record_msg}")
        except mysql.connector.Error as e:
            trace(f"ERROR in record_usage: {str(e)}", level="ERROR")
            raise HTTPException(400, f"DB error in record_usage: {str(e)}")

        # --- Save to query_log ---
        trace("Saving to query_log table...")
        execution_time = int((time.time() - start_time) * 1000)
        
        # Get plan_id for logging
        cur.execute("SELECT plan_id FROM users WHERE user_id = %s", (req.user_id,))
        user_row = cur.fetchone()
        plan_id = user_row["plan_id"] if user_row else None
        
        save_to_query_log(
            user_id=req.user_id,
            plan_id=plan_id,
            query_status="success" if record_msg in ("SUCCESS", "SUCCESS_USING_OVERFLOW") else "failed",
            tokens_used=tokens_used,
            conversation_id=conversation_id,
            user_message=req.prompt,
            llm_response=llm_response,
            message_type="llm_query",
            allow_budget_override=req.allow_budget,
            validation_result=validate_msg,
            execution_time_ms=execution_time
        )
        
        trace(f"[END] Request completed successfully in {execution_time}ms")

        if record_msg in ("SUCCESS", "SUCCESS_USING_OVERFLOW"):
            return {
                "status": "success",
                "validate": validate_msg,
                "record": record_msg,
                "tokens_used": tokens_used,
                "response": llm_response,
                "conversation_id": conversation_id,
            }
        else:
            return {
                "status": "blocked",
                "validate": validate_msg,
                "record": record_msg,
                "conversation_id": conversation_id,
            }

    except HTTPException:
        raise
    except Exception as e:
        trace(f"UNEXPECTED ERROR: {str(e)}", level="ERROR")
        raise HTTPException(500, str(e))
    finally:
        db.close()



# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

@app.get("/plans")
def list_plans():
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_get_all_plan")
        return get_rows(cur)
    finally:
        db.close()


@app.post("/plans", status_code=201)
def create_plan(req: PlanCreate):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_create_plan", (req.plan_name, req.hourly_limit, req.daily_limit, req.monthly_limit))
        db.commit()
        return {"message": "Plan created"}
    except mysql.connector.Error as e:
        raise HTTPException(400, str(e))
    finally:
        db.close()


@app.get("/plans/{plan_id}")
def get_plan(plan_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_get_plan_details", (plan_id,))
        result = get_rows(cur)
        if not result:
            raise HTTPException(404, "Plan not found")
        return result[0]
    finally:
        db.close()


@app.put("/plans/{plan_id}")
def update_plan(plan_id: int, req: PlanUpdate):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        msg = call_proc_with_out(cur, "sp_update_plan", (plan_id, req.hourly_limit, req.daily_limit, req.monthly_limit))
        db.commit()
        return {"message": msg}
    finally:
        db.close()


@app.delete("/plans/{plan_id}")
def deactivate_plan(plan_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_soft_delete_plan", (plan_id,))
        db.commit()
        return {"message": "Plan deactivated"}
    finally:
        db.close()


@app.post("/plans/migrate")
def migrate_users(req: MigratePlan):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_migrate_users_to_plan", (req.old_plan_id, req.new_plan_id))
        db.commit()
        return {"message": f"Users migrated from plan {req.old_plan_id} to {req.new_plan_id}"}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@app.get("/users")
def list_users():
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_list_users")
        return get_rows(cur)
    finally:
        db.close()


@app.post("/users", status_code=201)
def create_user(req: UserCreate):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_create_user", (req.user_name, req.plan_id, req.budget))
        db.commit()
        return {"message": "User created"}
    except mysql.connector.Error as e:
        raise HTTPException(400, str(e))
    finally:
        db.close()


@app.get("/users/{user_id}")
def get_user(user_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_get_user_details", (user_id,))
        result = get_rows(cur)
        if not result:
            raise HTTPException(404, "User not found")
        return result[0]
    finally:
        db.close()


@app.put("/users/{user_id}")
def update_user(user_id: int, req: UserUpdate):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_update_user", (user_id, req.user_name, req.plan_id, req.budget))
        db.commit()
        return {"message": "User updated"}
    finally:
        db.close()


@app.delete("/users/{user_id}")
def deactivate_user(user_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_delete_user", (user_id,))
        db.commit()
        return {"message": "User deactivated"}
    finally:
        db.close()


@app.post("/users/{user_id}/budget")
def add_budget(user_id: int, req: BudgetAdd):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_add_budget", (user_id, req.tokens))
        db.commit()
        return {"message": f"Added {req.tokens} tokens to budget"}
    finally:
        db.close()


@app.get("/users/{user_id}/limits")
def get_limits(user_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_get_remaining_limits", (user_id,))
        result = get_rows(cur)
        if not result:
            raise HTTPException(404, "User not found or inactive")
        return result[0]
    finally:
        db.close()


@app.get("/users/{user_id}/usage")
def get_usage(user_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_get_current_usage", (user_id,))
        result = get_rows(cur)
        if not result:
            raise HTTPException(404, "User not found")
        return result[0]
    finally:
        db.close()


@app.get("/users/{user_id}/history")
def get_history(user_id: int):
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.callproc("sp_get_query_history", (user_id,))
        return get_rows(cur)
    finally:
        db.close()



# adding chat history endpoint
@app.get("/users/{user_id}/chat-history")
def get_chat_history(user_id: int):
    """Fetch all chat messages for a user (from query_log where message_type='llm_query')."""
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("""
            SELECT 
                q_id,
                conversation_id,
                user_message,
                llm_response,
                tokens_used,
                validation_result,
                time_stamp,
                query_status
            FROM query_log
            WHERE user_id = %s 
            AND message_type = 'llm_query'
            ORDER BY time_stamp DESC
            LIMIT 100
        """, (user_id,))
        return cur.fetchall()
    finally:
        db.close()

@app.get("/admin/request-logs")
def get_request_logs(limit: Optional[int] = None, user_id: Optional[int] = None):
    """View backend request logs. If limit is omitted, returns ALL logs."""
    db = get_db()
    try:
        cur = db.cursor(dictionary=True)
        sql = "SELECT * FROM request_log"
        params = []
        if user_id:
            sql += " WHERE user_id = %s"
            params.append(user_id)
        sql += " ORDER BY timestamp DESC"
        if limit:                       # only add LIMIT when a value is given
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Generic procedure runner (Tab 2: "Procedure Executor")
# Whitelisted so only our 22 known procedures can be called.
# ---------------------------------------------------------------------------

ALLOWED_PROCS = {
    # Plans
    "sp_create_plan", "sp_update_plan", "sp_soft_delete_plan", "sp_delete_plan",
    "sp_migrate_users_to_plan", "sp_get_all_plan", "sp_get_plan_details",
    # Users
    "sp_create_user", "sp_update_user", "sp_add_budget", "sp_delete_user",
    "sp_get_user_details", "sp_list_users", "sp_get_remaining_limits",
    # Usage
    "sp_refresh_usage", "sp_get_current_usage", "sp_get_all_usage",
    # Validation & logging
    "sp_validate_request", "sp_record_usage", "sp_log_query", "sp_get_query_history",
}


@app.post("/admin/call-procedure")
def call_procedure(req: ProcCall):
    """Run any whitelisted procedure and return result rows or the OUT value.
    Mirrors what you'd see running CALL sp_xxx(...) in MySQL Workbench.
    """
    if req.proc_name not in ALLOWED_PROCS:
        raise HTTPException(400, f"Procedure '{req.proc_name}' is not allowed")

    db = get_db()
    try:
        cur = db.cursor(dictionary=True)

        if req.has_out:
            # Last parameter is OUT — call_proc_with_out appends the OUT var.
            out_val = call_proc_with_out(cur, req.proc_name, req.params)
            db.commit()
            return {"type": "out", "out_value": out_val}

        # Normal call: may return result rows (SELECT procs) or nothing (action procs).
        cur.callproc(req.proc_name, req.params)
        rows = get_rows(cur)
        db.commit()
        if rows:
            return {"type": "rows", "rows": rows, "row_count": len(rows)}
        return {"type": "action", "message": "Query OK — procedure executed."}

    except mysql.connector.Error as e:
        raise HTTPException(400, str(e))
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Serve the UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def serve_ui():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, encoding="utf-8") as f:
        return f.read()
