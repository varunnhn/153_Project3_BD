import flask
from flask import Flask, request, render_template,  jsonify, send_from_directory, g
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
import cv2  # Using OpenCV
import numpy as np
import uuid
import threading
import time
import os
import json
import sqlite3

# --- 1. Configuration ---

DATABASE = 'jobs.db'

# Kafka Broker Address (Node #2)
KAFKA_BROKER = '192.168.192.59:9092'  # --- UPDATE THIS ---

# Kafka Topics
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'

# File directories for upload and results
UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

# Image tiling configuration
MIN_IMG_SIZE = 1024
TILE_SIZE = 512

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESULT_FOLDER'] = RESULT_FOLDER

# Kafka Producer instance
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'message.max.bytes': 10485760  # 10MB
}
kafka_producer = Producer(producer_config)

# --- 2. Database Setup ---

def get_db():
    """Get a per-request database connection."""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row  # This lets us access columns by name
    return db

@app.teardown_appcontext
def close_connection(exception):
    """Close the database connection at the end of the request."""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def db_execute(query, args=(), commit=False):
    """Helper to run a query in a thread-safe way."""
    # Each thread must have its own connection.
    conn = sqlite3.connect(DATABASE, timeout=600.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query, args)
    results = cursor.fetchall()
    if commit:
        conn.commit()
    conn.close()
    return results

# --- 3. Master Node: Image Tiling & Task Publishing ---
def process_image_task(job_id, image_path, original_filename):
    """
    Master Node logic:
    1. Loads and splits image into tiles.
    2. Publishes each tile as a task to Kafka.
    """
    try:
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError("Could not read image")

        height, width, _ = img.shape
        db_execute(
            "UPDATE jobs SET original_height = ?, original_width = ? WHERE job_id = ?",
            (height, width, job_id),
            commit=True
        )

        tiles = []
        for y in range(0, height, TILE_SIZE):
            for x in range(0, width, TILE_SIZE):
                y1, y2 = y, min(y + TILE_SIZE, height)
                x1, x2 = x, min(x + TILE_SIZE, width)
                tile = img[y1:y2, x1:x2]

                _, buffer = cv2.imencode('.jpg', tile)
                tile_bytes = buffer.tobytes()

                tile_id = f"{job_id}_{x}_{y}"
                tiles.append(tile_id)

                task_message = {
                    'job_id': job_id,
                    'tile_id': tile_id,
                    'tile_data': tile_bytes,
                    'coordinates': {'x': x, 'y': y}
                }
                
                kafka_producer.produce(
                    TASK_TOPIC,
                    key=job_id.encode('utf-8'),
                    value=json.dumps(task_message, cls=CustomEncoder).encode('utf-8')
                )

        kafka_producer.flush()

        db_execute(
            "UPDATE jobs SET status = 'Processing', total_tiles = ? WHERE job_id = ?",
            (len(tiles), job_id),
            commit=True
        )
        print(f"Job {job_id}: Dispatched {len(tiles)} tiles to Kafka.")

    except Exception as e:
        print(f"Error processing job {job_id}: {e}")
        db_execute(
            "UPDATE jobs SET status = 'Failed', error = ? WHERE job_id = ?",
            (str(e), job_id),
            commit=True
        )

# Custom JSON Encoder to handle bytes
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode('latin-1')
        return json.JSONEncoder.default(self, obj)

# Custom JSON Decoder
def custom_decoder(obj):
    if 'tile_data' in obj:
        obj['tile_data'] = obj['tile_data'].encode('latin-1')
    if 'processed_tile_data' in obj:
        obj['processed_tile_data'] = obj['processed_tile_data'].encode('latin-1')
    return obj

# --- 4. Master Node: Result Collection & Reconstruction ---
def result_consumer_thread():
    """
    Subscribes to the 'results' topic in a background thread.
    Collects processed tiles and triggers reconstruction.
    """
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'master-result-collectors',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([RESULT_TOPIC])

    print("Result consumer thread started...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Result consumer error: {msg.error()}")
            continue

        try:
            result_data = json.loads(msg.value().decode('utf-8'), object_hook=custom_decoder)
            job_id = result_data['job_id']
            tile_id = result_data['tile_id']
            coords = result_data['coordinates']
            
            # Check if job exists
            job_info = db_execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            if not job_info:
                print(f"Received tile for unknown job: {job_id}")
                continue
            
            job_info = job_info[0] # Get the first row
            
            # Store the processed tile
            db_execute(
            "INSERT OR REPLACE INTO processed_tiles (tile_id, job_id, processed_data, coord_x, coord_y) VALUES (?, ?, ?, ?, ?)",
                (tile_id, job_id, result_data['processed_tile_data'], coords['x'], coords['y']),
                commit=True
            )

            # Get new count
            count_result = db_execute("SELECT COUNT(*) FROM processed_tiles WHERE job_id = ?", (job_id,))
            processed_count = count_result[0][0]
            
            # Update progress
            status_msg = f'Processing ({processed_count} / {job_info["total_tiles"]})'
            db_execute(
                "UPDATE jobs SET processed_count = ?, status = ? WHERE job_id = ?",
                (processed_count, status_msg, job_id),
                commit=True
            )

            print(f"Job {job_id}: Received processed tile {tile_id} ({processed_count}/{job_info['total_tiles']})")

            # Check if all tiles are processed
            if processed_count == job_info['total_tiles']:
                print(f"Job {job_id}: All tiles received. Starting reconstruction.")
                reconstruct_image(job_id)

        except Exception as e:
            print(f"Error processing result message: {e}")

def reconstruct_image(job_id):
    """
    Reconstructs the final image from all its processed tiles.
    """
    try:
        job_info = db_execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))[0]
        tiles_info = db_execute("SELECT * FROM processed_tiles WHERE job_id = ?", (job_id,))

        original_height = job_info['original_height']
        original_width = job_info['original_width']

        # Create a blank canvas for the final image
        final_image = np.zeros((original_height, original_width, 3), dtype=np.uint8)

        for tile_row in tiles_info:
            # Decode the processed tile image data
            tile_data_bytes = tile_row['processed_data']
            nparr = np.frombuffer(tile_data_bytes, np.uint8)
            processed_tile_img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            # Get coordinates to place the tile
            x = tile_row['coord_x']
            y = tile_row['coord_y']
            h, w, _ = processed_tile_img.shape

            # Place the tile onto the canvas
            final_image[y:y+h, x:x+w] = processed_tile_img

        # Save the final reconstructed image
        output_filename = f"{job_id}_{job_info['original_filename']}"
        output_path = os.path.join(app.config['RESULT_FOLDER'], output_filename)
        cv2.imwrite(output_path, final_image)

        # Update job status to 'Complete'
        db_execute(
            "UPDATE jobs SET status = 'Complete', result_image = ? WHERE job_id = ?",
            (output_filename, job_id),
            commit=True
        )
        print(f"Job {job_id}: Reconstruction complete. Saved to {output_path}")

        # Clean up processed tiles from DB
        db_execute("DELETE FROM processed_tiles WHERE job_id = ?", (job_id,), commit=True)

    except Exception as e:
        print(f"Error during reconstruction for job {job_id}: {e}")
        db_execute(
            "UPDATE jobs SET status = 'Reconstruction Failed', error = ? WHERE job_id = ?",
            (str(e), job_id),
            commit=True
        )


# --- 5. Master Node: Worker Heartbeat Monitoring ---
def heartbeat_consumer_thread():
    """
    Subscribes to the 'heartbeats' topic to monitor worker health.
    """
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'master-heartbeat-monitor',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([HEARTBEAT_TOPIC])

    print("Heartbeat consumer thread started...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            # Check for stale workers
            stale_time = time.time() - 30  # 30 seconds ago
            db_execute("DELETE FROM workers WHERE last_seen < ?", (stale_time,), commit=True)
            continue

        if msg.error():
            print(f"Heartbeat consumer error: {msg.error()}")
            continue

        try:
            hb_data = json.loads(msg.value().decode('utf-8'))
            worker_id = hb_data['worker_id']
            
            # Add or update worker in DB
            db_execute(
                "INSERT OR REPLACE INTO workers (worker_id, status, last_seen) VALUES (?, ?, ?)",
                (worker_id, hb_data['status'], time.time()),
                commit=True
            )
            # print(f"Received heartbeat from {worker_id}") # Can be noisy
        except Exception as e:
            print(f"Error processing heartbeat: {e}")


# --- 6. Client UI: Flask Web Routes ---
@app.route('/')
def index():
    """Serves the main HTML page for image upload."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_image():
    """
    Handles image upload from the client.
    Triggers the Master Node processing task.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file:
        # Save uploaded file
        filename = file.filename
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(upload_path)

        # Validate image size
        try:
            img = cv2.imread(upload_path)
            if img.shape[0] < MIN_IMG_SIZE or img.shape[1] < MIN_IMG_SIZE:
                os.remove(upload_path)
                return jsonify({'error': f'Image must be at least {MIN_IMG_SIZE}x{MIN_IMG_SIZE} pixels'}), 400
        except Exception as e:
             return jsonify({'error': f'Invalid image file: {e}'}), 400

        # Generate a unique job ID
        job_id = str(uuid.uuid4())

        # Initialize job in database
        db = get_db()
        db.execute(
            """
            INSERT INTO jobs (job_id, status, original_filename, total_tiles, processed_count) 
            VALUES (?, 'Queued', ?, 0, 0)
            """,
            (job_id, filename)
        )
        db.commit()

        # Start the master's processing task in a background thread
        threading.Thread(target=process_image_task, args=(job_id, upload_path, filename)).start()

        # Return the job ID to the client
        return jsonify({'job_id': job_id})

@app.route('/status')
def get_status():
    """
    Provides real-time job progress and worker activity.
    """
    db = get_db()
    
    # Get worker count
    worker_count_result = db.execute("SELECT COUNT(*) FROM workers").fetchone()
    active_worker_count = worker_count_result[0]
    
    job_id = request.args.get('job_id')
    if not job_id:
        # Return general status if no job_id
        return jsonify({
            'active_workers': active_worker_count,
            'active_worker_list': [row['worker_id'] for row in db.execute("SELECT worker_id FROM workers").fetchall()]
        })

    status_row = db.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    if not status_row:
        return jsonify({'error': 'Job not found'}), 404

    # Convert row to dictionary
    status = dict(status_row)
    
    return jsonify({
        'job_id': job_id,
        'status': status.get('status'),
        'progress_percent': (status.get('processed_count', 0) * 100) / status.get('total_tiles') if status.get('total_tiles', 0) > 0 else 0,
        'total_tiles': status.get('total_tiles'),
        'processed_count': status.get('processed_count'),
        'result_image_url': f"/results/{status.get('result_image')}" if status.get('status') == 'Complete' else None,
        'error': status.get('error'),
        'active_workers': active_worker_count
    })

@app.route('/results/<filename>')
def get_result_image(filename):
    """Serves the final, reconstructed image."""
    return send_from_directory(app.config['RESULT_FOLDER'], filename)


# --- 7. Main Execution ---
if __name__ == '__main__':
    print("--- Starting Distributed Image Processor (Master) ---")

    # 2. Start Kafka listener threads
    print("Starting Kafka consumer threads...")
    threading.Thread(target=result_consumer_thread, daemon=True).start()
    threading.Thread(target=heartbeat_consumer_thread, daemon=True).start()

    # 3. Run the Flask web server
    print("\nStarting Flask web server on http://localhost:5000 ...")
    app.run(host='0.0.0.0', port=5000)
