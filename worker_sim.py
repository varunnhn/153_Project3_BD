# worker_sim.py
import os
import base64
import json
import time
from kafka_client import make_consumer, make_producer, produce_task
from PIL import Image, ImageOps
from io import BytesIO
import threading
from datetime import datetime, timezone
import uuid

TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEAT_TOPIC = "heartbeats"
BOOTSTRAP = "localhost:9092"

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"

def process_tile_bytes(tile_bytes, processing="grayscale"):
    img = Image.open(BytesIO(tile_bytes)).convert("RGB")
    if processing == "grayscale":
        img = ImageOps.grayscale(img).convert("RGB")
    # add a small border or annotation if you want to see difference - optional
    # for now just return processed bytes
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

def send_heartbeat_loop(producer, interval=5):
    while True:
        payload = {
            "worker_id": WORKER_ID,
            "timestamp": int(time.time()),
            "iso_ts": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
            "status": "alive"
        }
        produce_task(producer, HEARTBEAT_TOPIC, key=WORKER_ID, message=payload)
        time.sleep(interval)

def tasks_loop():
    consumer = make_consumer(TASKS_TOPIC, group_id=f"worker-group-{WORKER_ID}", bootstrap_servers=BOOTSTRAP)
    producer = make_producer(BOOTSTRAP)
    print(f"[worker {WORKER_ID}] started, waiting for tasks...")
    # start heartbeat thread
    hb_thread = threading.Thread(target=send_heartbeat_loop, args=(producer,), daemon=True)
    hb_thread.start()

    for msg in consumer:
        try:
            payload = msg.value
            job_id = payload.get("job_id")
            tile_id = payload.get("tile_id")
            tile_b64 = payload.get("tile_b64")
            processing = payload.get("processing", "grayscale")
            tile_bytes = base64.b64decode(tile_b64)
            processed_bytes = process_tile_bytes(tile_bytes, processing=processing)
            processed_b64 = base64.b64encode(processed_bytes).decode('utf-8')
            result_msg = {
                "job_id": job_id,
                "tile_id": tile_id,
                "tile_b64": processed_b64,
                "status": "done"
            }
            produce_task(producer, RESULTS_TOPIC, key=job_id, message=result_msg)
            print(f"[worker {WORKER_ID}] processed {tile_id} -> published result")
        except Exception as e:
            print("[worker] error processing task:", e)

if __name__ == "__main__":
    tasks_loop()

