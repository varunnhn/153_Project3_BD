# app.py
import os
from flask import Flask, request, render_template, jsonify, send_file, abort, url_for
from werkzeug.utils import secure_filename
from uuid import uuid4
from PIL import Image
import math
import storage

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

ALLOWED_EXT = {"png", "jpg", "jpeg"}
MIN_DIM = 1024
TILE = 512

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

storage.init_db()

@app.route("/")
def index():
    return render_template("index.html")

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT

@app.route("/upload", methods=["POST"])
def upload():
    if "image" not in request.files:
        return jsonify({"ok": False, "error": "no file part"}), 400
    f = request.files["image"]
    if f.filename == "":
        return jsonify({"ok": False, "error": "no selected file"}), 400
    if not allowed_file(f.filename):
        return jsonify({"ok": False, "error": "file type not allowed"}), 400

    filename = secure_filename(f.filename)
    job_id = str(uuid4())
    job_folder = os.path.join(UPLOAD_DIR, job_id)
    os.makedirs(job_folder, exist_ok=True)
    save_path = os.path.join(job_folder, filename)
    f.save(save_path)

    # validate size
    img = Image.open(save_path)
    w, h = img.size
    if w < MIN_DIM or h < MIN_DIM:
        os.remove(save_path)
        return jsonify({"ok": False, "error": f"image dimensions must be >= {MIN_DIM}px. Yours: {w}x{h}"}), 400

    # compute tiling metadata
    nx = math.ceil(w / TILE)
    ny = math.ceil(h / TILE)

    # save metadata file
    meta = {
        "job_id": job_id,
        "filename": filename,
        "orig_path": save_path,
        "orig_width": w,
        "orig_height": h,
        "tile_width": TILE,
        "tile_height": TILE,
        "nx": nx,
        "ny": ny
    }
    with open(os.path.join(job_folder, "meta.json"), "w") as mf:
        import json
        json.dump(meta, mf, indent=2)

    # create DB rows (tiles, job)
    storage.create_job(job_id, save_path, nx, ny, w, h)

    # In the full pipeline you'd now call master.split_and_publish(job_id)
    # For frontend work we stop here and expose status endpoints.

    return jsonify({"ok": True, "job_id": job_id, "nx": nx, "ny": ny})

@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    prog = storage.get_job_progress(job_id)
    if not prog:
        return jsonify({"ok": False, "error": "job not found"}), 404
    active_workers = storage.get_active_workers_count()
    return jsonify({
        "ok": True,
        "job": prog["job"],
        "total_tiles": prog["total_tiles"],
        "done_tiles": prog["done_tiles"],
        "active_workers": active_workers
    })

@app.route("/result/<job_id>", methods=["GET"])
def result(job_id):
    # For now we don't reconstruct processed tiles. We'll return the original to let you
    # show something; real pipeline will replace this with reconstructed image path.
    conn_job = storage.get_job_progress(job_id)
    if not conn_job:
        return jsonify({"ok": False, "error": "job not found"}), 404
    # if completed, serve a placeholder final image (for now the original)
    job = conn_job["job"]
    orig_path = job["orig_path"]
    if not os.path.exists(orig_path):
        return jsonify({"ok": False, "error": "original file missing"}), 500
    return send_file(orig_path, mimetype="image/png")

# Simulation helper endpoints (for demo while workers not implemented)

@app.route("/simulate/progress/<job_id>", methods=["POST"])
def simulate_progress(job_id):
    """Call with JSON { "advance": <n> } to mark n pending tiles as done."""
    data = request.get_json() or {}
    n = int(data.get("advance", 1))
    marked = storage.mark_tiles_done(job_id, n)
    return jsonify({"ok": True, "marked": marked})

@app.route("/simulate/worker/<worker_id>", methods=["POST"])
def simulate_worker(worker_id):
    storage.simulate_worker_heartbeat(worker_id)
    return jsonify({"ok": True, "worker_id": worker_id})

if __name__ == "__main__":
    # for dev on ubuntu
    app.run(host="0.0.0.0", port=5000, debug=True)

