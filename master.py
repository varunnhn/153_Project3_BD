# master.py
import os
import math
import base64
import json
import threading
import time
from uuid import uuid4
from PIL import Image
from kafka_client import make_producer, make_consumer, produce_task
import storage

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")   # same as frontend
RESULTS_DIR = os.path.join(BASE_DIR, "results")
TILE_SIZE = 512

TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEAT_TOPIC = "heartbeats"
BOOTSTRAP = "localhost:9092"  # change if needed

storage.init_db()

def split_and_publish(job_id, bootstrap=BOOTSTRAP):
    """Split original image into tiles and publish tasks to Kafka (base64 tile)."""
    job = storage.get_job(job_id)
    if not job:
        raise ValueError("job not found: " + job_id)
    orig_path = job["orig_path"]
    tile_w = TILE_SIZE
    tile_h = TILE_SIZE
    img = Image.open(orig_path).convert("RGBA")  # preserve alpha if present
    w, h = img.size
    nx = math.ceil(w / tile_w)
    ny = math.ceil(h / tile_h)

    # prepare job folder
    job_folder = os.path.join(UPLOAD_DIR, job_id)
    os.makedirs(job_folder, exist_ok=True)
    tiles_out_dir = os.path.join(job_folder, "tiles")
    os.makedirs(tiles_out_dir, exist_ok=True)

    producer = make_producer(bootstrap)

    # create padded canvas to simplify reconstruction
    padded_w = nx * tile_w
    padded_h = ny * tile_h
    canvas = Image.new("RGBA", (padded_w, padded_h), (0,0,0,0))
    canvas.paste(img, (0, 0))

    # for each tile, crop and publish
    for r in range(ny):
        for c in range(nx):
            left = c * tile_w
            upper = r * tile_h
            right = left + tile_w
            lower = upper + tile_h
            tile = canvas.crop((left, upper, right, lower))
            tile_bytes = to_png_bytes(tile)
            tile_b64 = base64.b64encode(tile_bytes).decode('utf-8')
            tile_id = f"{job_id}_{r}_{c}"
            # store tile file (for debugging/demo)
            tile_path = os.path.join(tiles_out_dir, f"tile_{r}_{c}.png")
            with open(tile_path, "wb") as tf:
                tf.write(tile_bytes)
            msg = {
                "job_id": job_id,
                "tile_id": tile_id,
                "row": r,
                "col": c,
                "nx": nx,
                "ny": ny,
                "orig_width": w,
                "orig_height": h,
                "tile_width": tile_w,
                "tile_height": tile_h,
                "tile_b64": tile_b64,
                "processing": "grayscale"  # example instruction
            }
            # publish to Kafka (key by job_id -> helps partitioning by job)
            produce_task(producer, TASKS_TOPIC, key=job_id, message=msg)
            time.sleep(0.01)  # small throttle
    producer.flush()
    print(f"[master] Published {nx*ny} tiles for job {job_id} to topic {TASKS_TOPIC}")

def to_png_bytes(pil_img):
    from io import BytesIO
    buf = BytesIO()
    pil_img.convert("RGBA").save(buf, format="PNG")
    return buf.getvalue()

def save_result_tile(job_id, tile_id, tile_bytes_b64):
    """Save processed tile to results folder and update DB."""
    job_folder = os.path.join(UPLOAD_DIR, job_id)
    results_dir = os.path.join(job_folder, "results")
    os.makedirs(results_dir, exist_ok=True)
    tile_bytes = base64.b64decode(tile_bytes_b64)
    # tile_id is jobid_r_c
    _, r, c = tile_id.rsplit("_", 2)
    filename = f"tile_{r}_{c}.png"
    out_path = os.path.join(results_dir, filename)
    with open(out_path, "wb") as f:
        f.write(tile_bytes)
    # update DB
    storage.mark_tile_result(tile_id, out_path)
    print(f"[master] Saved result tile {tile_id} -> {out_path}")

def results_consumer_loop(bootstrap=BOOTSTRAP, group_id="master-results"):
    consumer = make_consumer(RESULTS_TOPIC, group_id=group_id, bootstrap_servers=bootstrap)
    print("[master] started results consumer")
    for msg in consumer:
        try:
            payload = msg.value
            tile_id = payload.get("tile_id")
            job_id = payload.get("job_id")
            tile_b64 = payload.get("tile_b64")
            status = payload.get("status", "done")
            if status != "done":
                continue
            save_result_tile(job_id, tile_id, tile_b64)
            # optionally: check if job completed and reconstruct
            prog = storage.get_job_progress(job_id)
            if prog and prog["done_tiles"] == prog["total_tiles"]:
                print(f"[master] All tiles done for {job_id}. Reconstructing...")
                reconstruct_job(job_id)
        except Exception as e:
            print("[master] error processing result message:", e)

def heartbeat_consumer_loop(bootstrap=BOOTSTRAP, group_id="master-heartbeat"):
    consumer = make_consumer(HEARTBEAT_TOPIC, group_id=group_id, bootstrap_servers=bootstrap)
    print("[master] started heartbeat consumer")
    for msg in consumer:
        try:
            payload = msg.value
            worker_id = payload.get("worker_id")
            ts = payload.get("timestamp")
            iso_ts = payload.get("iso_ts") if payload.get("iso_ts") else datetime_iso_now()
            storage.update_worker_heartbeat(worker_id, iso_ts)
        except Exception as e:
            print("[master] heartbeat error:", e)

def reconstruct_job(job_id):
    """Read processed tiles and reconstruct final image; crop padding to original size."""
    job = storage.get_job(job_id)
    if not job:
        print("[master] job missing:", job_id)
        return
    nx = job["nx"]
    ny = job["ny"]
    tile_w = TILE_SIZE
    tile_h = TILE_SIZE
    orig_w = job["orig_width"]
    orig_h = job["orig_height"]

    job_folder = os.path.join(UPLOAD_DIR, job_id)
    results_dir = os.path.join(job_folder, "results")
    if not os.path.isdir(results_dir):
        print("[master] no results dir for job", job_id)
        return

    canvas = Image.new("RGBA", (nx * tile_w, ny * tile_h), (0,0,0,0))
    for r in range(ny):
        for c in range(nx):
            tile_path = os.path.join(results_dir, f"tile_{r}_{c}.png")
            if not os.path.exists(tile_path):
                print("[master] missing processed tile", tile_path)
                return
            t = Image.open(tile_path).convert("RGBA")
            canvas.paste(t, (c * tile_w, r * tile_h))

    # crop to original
    final = canvas.crop((0, 0, orig_w, orig_h)).convert("RGB")
    out_dir = os.path.join(results_dir, "final")
    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"{job_id}_final.png")
    final.save(final_path)
    storage.set_job_result_path(job_id, final_path)
    print(f"[master] reconstructed final image: {final_path}")

def datetime_iso_now():
    from datetime import datetime, timezone
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def start_consumers(bootstrap=BOOTSTRAP):
    t1 = threading.Thread(target=results_consumer_loop, args=(bootstrap,), daemon=True)
    t2 = threading.Thread(target=heartbeat_consumer_loop, args=(bootstrap,), daemon=True)
    t1.start(); t2.start()
    print("[master] consumers started (results, heartbeat)")

if __name__ == "__main__":
    # lightweight CLI to publish tiles for a job and start consumers
    import sys
    if len(sys.argv) < 2:
        print("Usage: python master.py <job_id> [start_consumers]")
        sys.exit(1)
    job_id = sys.argv[1]
    start_consumers()
    split_and_publish(job_id)
    print("[master] done publishing; consumers running. press Ctrl-C to exit.")
    while True:
        time.sleep(1)

