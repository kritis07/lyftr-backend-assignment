import os
import hmac
import hashlib
from typing import Optional

from fastapi import FastAPI, Request, Header, HTTPException, Query

from app.models import MessageIn
from app.storage import (
    get_db,
    insert_message,
    list_messages,
    get_stats,
)

app = FastAPI(title="Lyftr Backend Assignment")


# -------------------------
# Health checks
# -------------------------

@app.get("/health/live")
def health_live():
    return {"status": "live"}


@app.get("/health/ready")
def health_ready():
    if not os.getenv("WEBHOOK_SECRET"):
        raise HTTPException(status_code=503, detail="WEBHOOK_SECRET missing")

    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
    except Exception:
        raise HTTPException(status_code=503, detail="Database not ready")

    return {"status": "ready"}


# -------------------------
# Helpers
# -------------------------

def compute_signature(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


# -------------------------
# Webhook
# -------------------------

@app.post("/webhook")
async def webhook(
    request: Request,
    x_signature: str = Header(None, alias="X-Signature"),
):
    secret = os.getenv("WEBHOOK_SECRET")
    if not secret:
        raise HTTPException(status_code=503, detail="WEBHOOK_SECRET missing")

    raw_body = await request.body()
    expected = compute_signature(secret, raw_body)

    if not x_signature or not hmac.compare_digest(expected, x_signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = MessageIn.model_validate_json(raw_body)

    with get_db() as conn:
        created = insert_message(
            conn,
            {
                "message_id": payload.message_id,
                "from_": payload.from_,
                "to": payload.to,
                "ts": payload.ts,
                "text": payload.text,
            },
        )

    return {"status": "ok", "duplicate": not created}


# -------------------------
# Messages
# -------------------------

@app.get("/messages")
def get_messages(
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    since: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    with get_db() as conn:
        total, messages = list_messages(
            conn,
            from_=from_,
            to=to,
            since=since,
            q=q,
            limit=limit,
            offset=offset,
        )

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "messages": messages,
    }


# -------------------------
# Stats
# -------------------------

@app.get("/stats")
def stats():
    with get_db() as conn:
        return get_stats(conn)
