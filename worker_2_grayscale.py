import time
import json
import threading
import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer, errors

# --- ⚙️ 1. Configuration ---
# IMPORTANT: Use the EXTERNAL address of your Broker (e.g., 192.168.192.246:9092)
KAFKA_BROKER = '192.168.192.246:9092' 

TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'

# Worker ID and Group (Must be unique for the worker, group ID must match all workers)
WORKER_ID = 'Worker_2_Grayscale' 
CONSUMER_GROUP_ID = 'image_processing_group' 

# --- 🛠️ 2. Custom Encoders/Decoders (Matches Master's Logic) ---

# Custom JSON Encoder (Required for sending results back to Master)
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            # Master expects bytes to be encoded to a string via 'latin-1'
            return obj.decode('latin-1')
        return json.JSONEncoder.default(self, obj)

# Custom JSON Decoder (Required for receiving tasks from Master)
def custom_decoder(obj):
    # Reverse the Master's encoding to get back to raw bytes
    if 'tile_data' in obj:
        # Convert the 'latin-1' string back into a byte object
        obj['tile_data'] = obj['tile_data'].encode('latin-1')
    
    # We don't strictly need to decode processed_tile_data here, 
    # but include for completeness if this worker consumes results for any reason.
    if 'processed_tile_data' in obj: 
        obj['processed_tile_data'] = obj['processed_tile_data'].encode('latin-1')
    return obj


# --- 🛠️ 3. Utility Functions ---

def serialize_image(image_np):
    """Encodes a NumPy image array to JPEG bytes for Kafka payload."""
    # Use JPEG compression
    is_success, buffer = cv2.imencode(".jpg", image_np)
    if is_success:
        return buffer.tobytes()
    return None

def deserialize_image(image_bytes):
    """Decodes image bytes back into a NumPy array (Color/BGR)."""
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        # Decode as COLOR (3-channel) since the input (from Master) is a raw tile
        image_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR) 
        return image_np
    except Exception as e:
        # Returns None if data is corrupt
        return None

# --- ❤️ 4. Heartbeat Function ---

def send_heartbeat(producer):
    """Sends a periodic heartbeat message to the Master Node."""
    try:
        message = {
            'worker_id': WORKER_ID,
            'timestamp': time.time(),
            'status': 'ALIVE',
            'task_type': 'Grayscale'
        }
        producer.send(
            HEARTBEAT_TOPIC, 
            value=json.dumps(message).encode('utf-8')
        )
        producer.flush()
    except errors.KafkaTimeoutError:
        print(f"[{time.ctime()}] 💔 Heartbeat failed: Kafka broker unreachable.")
    except Exception as e:
        print(f"[{time.ctime()}] 💔 Heartbeat failed: {e}")

    # Schedule the next heartbeat (e.g., every 5 seconds)
    threading.Timer(5.0, send_heartbeat, args=[producer]).start()


# --- 🧠 5. Main Worker Logic ---

def worker_2_grayscale_processor():
    """
    Worker 2 Grayscale: Consumes JSON-wrapped tiles, converts to grayscale, 
    and publishes JSON-wrapped results.
    """
    print(f"[{time.ctime()}] Starting {WORKER_ID}...")

    # 1. Setup Kafka Producer (for results and heartbeats)
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            api_version=(0, 10, 1)
        )
    except errors.NoBrokersAvailable:
        print(f"[{time.ctime()}] FATAL: Could not connect to Kafka broker at {KAFKA_BROKER}. Exiting.")
        return

    send_heartbeat(producer)
    
    # 2. Setup Kafka Consumer
    consumer = KafkaConsumer(
        TASK_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP_ID, 
        api_version=(0, 10, 1)
    )
    
    print(f"[{time.ctime()}] Subscribed to '{TASK_TOPIC}'. Awaiting tasks...")

    try:
        # 3. Start main consumption loop
        for message in consumer:
            start_time = time.time()
            
            # --- Consumption & JSON Deserialization ---
            try:
                # The message value is a JSON string containing the image data
                task_message = json.loads(message.value.decode('utf-8'), object_hook=custom_decoder)
                
                # Extract the necessary data (now converted back to bytes by custom_decoder)
                key_data = task_message['tile_id']
                image_data_bytes = task_message['tile_data']
            except Exception as e:
                print(f"[{time.ctime()}] ERROR: Failed to decode JSON payload. Skipping message. Error: {e}")
                continue
            
            # --- Image Deserialization ---
            tile_np = deserialize_image(image_data_bytes)
            
            if tile_np is None:
                # This is the error you were seeing: the extracted bytes failed to decode
                print(f"[{time.ctime()}] ERROR: Could not decode image for key: {key_data}. Skipping message.")
                continue

            # --- Image Processing (The Grayscale Task) ---
            processed_tile = cv2.cvtColor(tile_np, cv2.COLOR_BGR2GRAY)
            
            # Re-serialize the processed tile (back into raw bytes)
            processed_image_bytes = serialize_image(processed_tile)
            
            if processed_image_bytes is None:
                print(f"[{time.ctime()}] ERROR: Could not serialize grayscale tile for key: {key_data}")
                continue
                
            # --- Production to Results Topic (JSON Re-wrap) ---
            result_message = {
                'job_id': task_message['job_id'],
                'tile_id': f"{task_message['tile_id']}_GS",
                'processed_tile_data': processed_image_bytes, # CustomEncoder will convert this to string
                'coordinates': task_message['coordinates']
            }
            
            # Use CustomEncoder to send the JSON string with base64-like bytes back to the Master
            producer.send(
                RESULT_TOPIC, 
                key=task_message['job_id'].encode('utf-8'),
                value=json.dumps(result_message, cls=CustomEncoder).encode('utf-8')
            )
            producer.flush()

            # --- Logging ---
            end_time = time.time()
            elapsed = (end_time - start_time) * 1000
            
            print(
                f"[{time.ctime()}] ✅ Processed Tile {key_data} "
                f"-> Grayscale. Sent to '{RESULT_TOPIC}'. "
                f"Time taken: {elapsed:.2f}ms"
            )

    except KeyboardInterrupt:
        print(f"\n{WORKER_ID} shutting down...")
    except Exception as e:
        print(f"[{time.ctime()}] Unhandled error in consumption loop: {e}")
    finally:
        consumer.close()
        producer.close()
        print(f"[{time.ctime()}] {WORKER_ID} Kafka connections closed.")


if __name__ == '__main__':
    worker_2_grayscale_processor()
