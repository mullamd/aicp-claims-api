from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import os
import psycopg2
import psycopg2.extras

app = FastAPI(title="AICP Claims API", version="1.0.0")

# -----------------------------
# Redshift connection helper
# -----------------------------
def get_connection():
    """
    Creates a new psycopg2 connection to Redshift using env vars.
    Required env vars:
      REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_DB, REDSHIFT_PORT
    """
    host = os.getenv("REDSHIFT_HOST")
    user = os.getenv("REDSHIFT_USER")
    pwd  = os.getenv("REDSHIFT_PASSWORD")
    db   = os.getenv("REDSHIFT_DB", "dev")
    port = int(os.getenv("REDSHIFT_PORT", "5439"))

    if not all([host, user, pwd, db, port]):
        raise RuntimeError("Missing Redshift connection environment variables")

    # Tip: statement timeout prevents long-hanging queries if desired
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=pwd,
        dbname=db,
        port=port,
        sslmode="require",
        connect_timeout=10,
        application_name="aicp-claims-api",
    )
    # Optional: shorten runaway queries (uncomment if you want)
    # with conn.cursor() as c:
    #     c.execute("SET statement_timeout = 5000")  # 5 seconds
    return conn

# -----------------------------
# Health Check
# -----------------------------
@app.get("/health")
def health():
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return {"status": "ok", "db": "ok"}
    except Exception as e:
        # If you want ECS to mark task unhealthy on DB errors, uncomment next line:
        # raise HTTPException(status_code=503, detail=f"DB error: {e}")
        return {"status": "ok", "db": f"error: {e}"}

# -----------------------------
# GET /v1/claims/{claim_id}
# status + AI details
# -----------------------------
@app.get("/v1/claims/{claim_id}")
def get_claim_status_and_ai(claim_id: str):
    """
    Returns status + AI fields for a single claim.
    Expects table: aicp_insurance.claims_processed
      columns: claim_id, claim_status, fraud_prediction, fraud_score, fraud_explanation, inserted_at
    """
    sql = """
      SELECT
          claim_id,
          claim_status,
          fraud_prediction,
          fraud_score,
          fraud_explanation
      FROM aicp_insurance.claims_processed
      WHERE claim_id = %s
      ORDER BY inserted_at DESC
      LIMIT 1;
    """
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(sql, (claim_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")

                return {
                    "claim_id": row["claim_id"],
                    "status": row["claim_status"],
                    "ai": {
                        "fraud_prediction": row["fraud_prediction"],
                        "fraud_score": float(row["fraud_score"]) if row["fraud_score"] is not None else None,
                        "fraud_explanation": row["fraud_explanation"],
                    }
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

# -----------------------------
# GET /v1/claims/status/{claim_status}?days=&limit=
# list IDs by status (robust to "-", "_", spaces, case)
# -----------------------------
@app.get("/v1/claims/status/{claim_status}")
def list_claim_ids_by_status(
    claim_status: str,
    days: Optional[int] = Query(default=7, ge=1, le=90, description="Lookback window in days"),
    limit: Optional[int] = Query(default=50, ge=1, le=500, description="Max number of IDs to return")
):
    """
    Returns a list of claim_ids with the given status in the last `days` days,
    ordered by most recent.
    Expects column 'inserted_at' in aicp_insurance.claims_processed.
    Matching is normalized on both sides (remove non-alphanumerics, lowercase).
    """
    sql = """
      SELECT claim_id
      FROM aicp_insurance.claims_processed
      WHERE LOWER(REGEXP_REPLACE(claim_status, '[^a-z0-9]+', '')) =
            LOWER(REGEXP_REPLACE(%s,          '[^a-z0-9]+', ''))
        AND inserted_at >= DATEADD(day, -%s, GETDATE())
      ORDER BY inserted_at DESC
      LIMIT %s;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (claim_status, days, limit))
                rows = cur.fetchall()
                claim_ids = [r[0] for r in rows]
                return {
                    "status": claim_status,
                    "days": days,
                    "limit": limit,
                    "count": len(claim_ids),
                    "claim_ids": claim_ids
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
