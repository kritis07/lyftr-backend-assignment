import os
import sqlite3
from contextlib import contextmanager

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")


def _get_db_path() -> str:
    # Expected format: sqlite:////data/app.db
    return DATABASE_URL.replace("sqlite:////", "/", 1)


DB_PATH = _get_db_path()


@contextmanager
def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Create table if not exists
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            message_id TEXT PRIMARY KEY,
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            ts TEXT NOT NULL,
            text TEXT NOT NULL
        )
        """
    )
    conn.commit()

    try:
        yield conn
    finally:
        conn.close()


def insert_message(conn, message: dict) -> bool:
    """
    Inserts message into DB.
    Returns True if inserted, False if duplicate message_id.
    """
    try:
        conn.execute(
            """
            INSERT INTO messages (message_id, sender, receiver, ts, text)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message["message_id"],
                message["from_"],
                message["to"],
                message["ts"],
                message["text"],
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def list_messages(
    conn,
    from_=None,
    to=None,
    since=None,
    q=None,
    limit=50,
    offset=0,
):
    conditions = []
    params = []

    if from_:
        conditions.append("sender = ?")
        params.append(from_)
    if to:
        conditions.append("receiver = ?")
        params.append(to)
    if since:
        conditions.append("ts >= ?")
        params.append(since)
    if q:
        conditions.append("text LIKE ?")
        params.append(f"%{q}%")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    total = conn.execute(
        f"SELECT COUNT(*) FROM messages {where_clause}",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""
        SELECT message_id, sender, receiver, ts, text
        FROM messages
        {where_clause}
        ORDER BY ts DESC
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()

    messages = [
        {
            "message_id": row["message_id"],
            "from": row["sender"],
            "to": row["receiver"],
            "ts": row["ts"],
            "text": row["text"],
        }
        for row in rows
    ]

    return total, messages


def get_stats(conn):
    total_messages = conn.execute(
        "SELECT COUNT(*) FROM messages"
    ).fetchone()[0]

    senders_count = conn.execute(
        "SELECT COUNT(DISTINCT sender) FROM messages"
    ).fetchone()[0]

    rows = conn.execute(
        """
        SELECT sender, COUNT(*) as cnt
        FROM messages
        GROUP BY sender
        """
    ).fetchall()

    messages_per_sender = {row["sender"]: row["cnt"] for row in rows}

    first_ts = conn.execute(
        "SELECT MIN(ts) FROM messages"
    ).fetchone()[0]

    last_ts = conn.execute(
        "SELECT MAX(ts) FROM messages"
    ).fetchone()[0]

    return {
        "total_messages": total_messages,
        "senders_count": senders_count,
        "messages_per_sender": messages_per_sender,
        "first_message_ts": first_ts,
        "last_message_ts": last_ts,
    }
