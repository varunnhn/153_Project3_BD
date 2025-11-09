# storage.py  (REPLACE your old storage.py with this version)
import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "frontend.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        job_id TEXT PRIMARY KEY,
        orig_path TEXT,
        status TEXT,
        nx INTEGER,
        ny INTEGER,
        orig_width INTEGER,
        orig_height INTEGER,
        created_at TEXT,
        completed_at TEXT,
        result_path TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tiles(
        tile_id TEXT PRIMARY KEY,
        job_id TEXT,
        row_idx INTEGER,
        col_idx INTEGER,
        status TEXT,
        result_path TEXT,
        processed_at TEXT,
        FOREIGN KEY(job_id) REFERENCES jobs(job_id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS workers(
        worker_id TEXT PRIMARY KEY,
        last_heartbeat TEXT,
        status TEXT
    );
    """)
    conn.commit()
    conn.close()

def create_job(job_id, orig_path, nx, ny, orig_w, orig_h):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO jobs(job_id, orig_path, status, nx, ny, orig_width, orig_height, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (job_id, orig_path, 'submitted', nx, ny, orig_w, orig_h, datetime.utcnow().replace(tzinfo=timezone.utc).isoformat())
    )
    # insert tiles rows with status 'pending'
    for r in range(ny):
        for c in range(nx):
            tile_id = f"{job_id}_{r}_{c}"
            cur.execute(
                "INSERT INTO tiles(tile_id, job_id, row_idx, col_idx, status) VALUES (?, ?, ?, ?, ?)",
                (tile_id, job_id, r, c, 'pending')
            )
    conn.commit()
    conn.close()

def get_job(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def get_job_progress(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
    job = cur.fetchone()
    if not job:
        conn.close()
        return None
    cur.execute("SELECT COUNT(*) as total FROM tiles WHERE job_id = ?", (job_id,))
    total = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) as done FROM tiles WHERE job_id = ? AND status = 'done'", (job_id,))
    done = cur.fetchone()["done"]
    conn.close()
    return {
        "job": dict(job),
        "total_tiles": total,
        "done_tiles": done
    }

def mark_tile_result(tile_id, result_path):
    """Mark tile as done and set result path."""
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    cur.execute("UPDATE tiles SET status = 'done', result_path = ?, processed_at = ? WHERE tile_id = ?", (result_path, now, tile_id))
    # check job completeness
    cur.execute("SELECT job_id FROM tiles WHERE tile_id = ?", (tile_id,))
    row = cur.fetchone()
    job_id = row["job_id"] if row else None
    if job_id:
        cur.execute("SELECT COUNT(*) as pending FROM tiles WHERE job_id = ? AND status != 'done'", (job_id,))
        pending = cur.fetchone()["pending"]
        if pending == 0:
            cur.execute("UPDATE jobs SET status = 'completed', completed_at = ? WHERE job_id = ?", (now, job_id))
    conn.commit()
    conn.close()

def set_job_result_path(job_id, result_path):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET result_path = ? WHERE job_id = ?", (result_path, job_id))
    conn.commit()
    conn.close()

def get_tile_info(tile_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tiles WHERE tile_id = ?", (tile_id,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None

def get_pending_tiles(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT tile_id, row_idx, col_idx FROM tiles WHERE job_id = ? AND status != 'done'", (job_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_all_tiles(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tiles WHERE job_id = ? ORDER BY row_idx, col_idx", (job_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def mark_tiles_done(job_id, n):
    """mark up to n pending tiles as done (simulate). Returns number actually marked."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT tile_id FROM tiles WHERE job_id = ? AND status != 'done' LIMIT ?", (job_id, n))
    rows = cur.fetchall()
    if not rows:
        conn.close()
        return 0
    count = 0
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    for r in rows:
        cur.execute("UPDATE tiles SET status = 'done', processed_at = ? WHERE tile_id = ?", (now, r["tile_id"]))
        count += 1
    # if all done mark job completed
    cur.execute("SELECT COUNT(*) as pending FROM tiles WHERE job_id = ? AND status != 'done'", (job_id,))
    pending = cur.fetchone()["pending"]
    if pending == 0:
        cur.execute("UPDATE jobs SET status = 'completed', completed_at = ? WHERE job_id = ?", (now, job_id))
    conn.commit()
    conn.close()
    return count

def get_active_workers_count(threshold_seconds=30):
    from datetime import datetime, timedelta
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT last_heartbeat FROM workers")
    rows = cur.fetchall()
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    alive = 0
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["last_heartbeat"])
            if (now - ts).total_seconds() <= threshold_seconds:
                alive += 1
        except Exception:
            pass
    conn.close()
    return alive

def simulate_worker_heartbeat(worker_id):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    cur.execute("INSERT OR REPLACE INTO workers(worker_id, last_heartbeat, status) VALUES (?, ?, ?)", (worker_id, now, 'alive'))
    conn.commit()
    conn.close()

def update_worker_heartbeat(worker_id, iso_ts):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO workers(worker_id, last_heartbeat, status) VALUES (?, ?, ?)", (worker_id, iso_ts, 'alive'))
    conn.commit()
    conn.close()

