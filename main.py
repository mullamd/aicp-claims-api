from fastapi import FastAPI, HTTPException
from typing import Optional
import os, time, psycopg2, psycopg2.extras

app = FastAPI(title="AICP Claims API", version="1.0.0")

# ---------- Config ----------
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))

# ---------- DB helpers ----------
def get_connection(connect_timeout=3):
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        dbname=REDSHIFT_DB,
        port=REDSHIFT_PORT,
        connect_timeout=connect_timeout,
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

def get_connection_with_retry(max_attempts=8, base=0.5):
    last = None
    for i in range(max_attempts):
        try:
            return get_connection(connect_timeout=3)
        except Exception as e:
            last = e
            time.sleep(min(8, base * (2 ** i)))  # exponential backoff
    raise last

# ---------- Health endpoints ----------
@app.get("/health")
def health():
    # Liveness only – NEVER touch the DB here
    return {"ok": True}

@app.get("/ready")
def ready():
    # Readiness – verify DB connectivity, but don't crash app if it fails
    try:
        with get_connection(connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"ready": True}
    except Exception as e:
        # Keep container alive; caller can see readiness=false
        return {"ready": False, "reason": str(e)}

# ---------- Sample endpoints ----------
@app.get("/v1/claims/{claim_id}")
def get_claim(claim_id: str):
    """
    Example read path. If Redshift is paused, return 503 (don’t kill container).
    """
    try:
        with get_connection_with_retry() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT *
                    FROM aicp_insurance.claims_processed
                    WHERE claim_id = %s
                    LIMIT 1
                """, (claim_id,))
                row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Claim not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")

@app.get("/v1/claims/status/{claim_status}")
def list_claims_by_status(claim_status: str, days: Optional[int] = None, limit: int = 50):
    try:
        with get_connection_with_retry() as conn:
            with conn.cursor() as cur:
                if days:
                    cur.execute("""
                        SELECT claim_id, claim_status, inserted_at
                        FROM aicp_insurance.claims_processed
                        WHERE claim_status = %s
                          AND inserted_at >= DATEADD(day, -%s, GETDATE())
                        ORDER BY inserted_at DESC
                        LIMIT %s
                    """, (claim_status, days, limit))
                else:
                    cur.execute("""
                        SELECT claim_id, claim_status, inserted_at
                        FROM aicp_insurance.claims_processed
                        WHERE claim_status = %s
                        ORDER BY inserted_at DESC
                        LIMIT %s
                    """, (claim_status, limit))
                rows = cur.fetchall()
        return rows
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")
