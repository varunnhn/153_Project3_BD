import sqlite3

DATABASE = 'jobs.db'

print("Connecting to database...")
conn = sqlite3.connect(DATABASE, timeout=10.0, check_same_thread=False)
cursor = conn.cursor()

print("Creating 'jobs' table...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT,
    original_filename TEXT,
    original_height INTEGER,
    original_width INTEGER,
    total_tiles INTEGER,
    processed_count INTEGER,
    result_image TEXT,
    error TEXT
);
""")

print("Creating 'processed_tiles' table...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS processed_tiles (
    tile_id TEXT PRIMARY KEY,
    job_id TEXT,
    processed_data BLOB,
    coord_x INTEGER,
    coord_y INTEGER,
    FOREIGN KEY (job_id) REFERENCES jobs (job_id)
);
""")

print("Creating 'workers' table...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    status TEXT,
    last_seen REAL
);
""")

conn.commit()
conn.close()

print("Database 'jobs.db' initialized successfully.")
