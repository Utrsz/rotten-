import os
import eventlet
# Essential for high-concurrency networking
eventlet.monkey_patch()

import json
import time
from flask import Flask, request, jsonify, render_template_string
from flask_socketio import SocketIO
from huggingface_hub import HfApi

app = Flask(__name__)
app.config['SECRET_KEY'] = 'ultra_fast_persist_1337'
# Eventlet allows handling hundreds of concurrent chunks without blocking
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

HF_TOKEN = os.environ.get("HF_TOKEN")
DATASET_ID = "Stlcx/daten"
api = HfApi()

UPLOAD_DIR = "/tmp/uploads"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

# ================= UI (Real-time & Persistent) =================
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>HF Ultra Pro: Persistent</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        body { background: #020617; color: #f8fafc; font-family: ui-sans-serif, system-ui; }
        .glass { background: rgba(30, 41, 59, 0.4); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,0.05); }
        .progress-fill { transition: width 0.2s linear; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 10px; }
    </style>
</head>
<body class="p-4 md:p-10">
    <div class="max-w-5xl mx-auto">
        <header class="flex justify-between items-center mb-10">
            <div>
                <h1 class="text-3xl font-black bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent italic">ULTRA PERSIST</h1>
                <p class="text-[10px] text-slate-500 font-mono tracking-[0.2em]">RESUMABLE PARALLEL STREAMER</p>
            </div>
            <div id="status-pill" class="px-4 py-1.5 rounded-full glass text-[10px] font-bold text-slate-500 border border-slate-700">OFFLINE</div>
        </header>

        <div id="drop-zone" class="glass rounded-[2rem] p-16 mb-8 border-2 border-dashed border-slate-800 hover:border-cyan-500/50 transition-all cursor-pointer text-center group">
            <div class="space-y-4">
                <div class="text-5xl">⚡</div>
                <p class="text-slate-400 text-lg">Drop files or <span class="text-cyan-400 font-semibold">click to browse</span></p>
                <input type="file" id="fileInput" multiple class="hidden">
            </div>
        </div>

        <div id="upload-container" class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-12"></div>

        <div class="glass rounded-2xl p-6">
            <h3 class="text-xs font-bold text-slate-500 uppercase mb-4 tracking-widest">Dataset Files</h3>
            <div id="file-list" class="space-y-2 max-h-60 overflow-y-auto pr-2"></div>
        </div>
    </div>

<script>
const socket = io();
const CHUNK_SIZE = 5 * 1024 * 1024; 
const PARALLEL_WORKERS = 5; 
let activeUploads = JSON.parse(localStorage.getItem('hf_uploads') || '{}');

socket.on('connect', () => {
    const pill = document.getElementById('status-pill');
    pill.innerText = 'LIVE ENGINE';
    pill.className = 'px-4 py-1.5 rounded-full glass text-[10px] font-bold text-green-400 border border-green-500/30 bg-green-500/10';
    resumeExistingUploads();
});

const fileInput = document.getElementById('fileInput');
document.getElementById('drop-zone').onclick = () => fileInput.click();
fileInput.onchange = e => handleFiles(e.target.files);

function handleFiles(files) {
    Array.from(files).forEach(f => {
        const uid = btoa(f.name + f.size).substring(0, 16).replace(/[/+=]/g, '');
        // Store file reference for persistence (Note: actual File objects can't be stored in localStorage, 
        // but we keep the metadata to resume the UI)
        activeUploads[uid] = { name: f.name, size: f.size, status: 'uploading' };
        localStorage.setItem('hf_uploads', JSON.stringify(activeUploads));
        uploadFile(f, uid);
    });
}

async function resumeExistingUploads() {
    // If you refresh, this helps show the cards, but user must re-select files 
    // if the browser cleared the file handler memory for security.
    Object.keys(activeUploads).forEach(uid => {
        if (!document.getElementById(`card-${uid}`)) {
            createCard(uid, activeUploads[uid].name, activeUploads[uid].size);
            document.getElementById(`stats-${uid}`).innerText = "Waiting for file re-selection or resume...";
        }
    });
}

function createCard(uid, name, size) {
    if (document.getElementById(`card-${uid}`)) return;
    const card = document.createElement('div');
    card.id = `card-${uid}`;
    card.className = 'glass p-5 rounded-2xl border border-slate-800 animate-in fade-in duration-300';
    card.innerHTML = `
        <div class="flex justify-between items-start mb-4">
            <div class="w-full truncate">
                <div class="flex justify-between items-center mb-1">
                    <p class="font-bold text-sm truncate pr-4">${name}</p>
                    <button onclick="cancelTask('${uid}')" class="text-slate-500 hover:text-red-400 transition-colors">✕</button>
                </div>
                <p id="stats-${uid}" class="text-[9px] font-mono text-slate-500 uppercase tracking-tighter italic">Initializing Stream...</p>
            </div>
        </div>
        <div class="w-full bg-slate-900/50 h-1.5 rounded-full overflow-hidden mb-3">
            <div id="bar-${uid}" class="progress-fill bg-gradient-to-r from-cyan-500 to-blue-500 h-full w-0 shadow-[0_0_12px_rgba(6,182,212,0.4)]"></div>
        </div>
        <div class="flex justify-between items-center text-[10px] font-black italic">
            <span id="speed-${uid}" class="text-cyan-400 tracking-widest">---</span>
            <span id="eta-${uid}" class="text-slate-500 tracking-widest">---</span>
        </div>
    `;
    document.getElementById('upload-container').prepend(card);
}

async function uploadFile(file, uid) {
    createCard(uid, file.name, file.size);
    const startTime = Date.now();
    
    const initRes = await fetch(`/init?uid=${uid}&size=${file.size}`);
    const { existing_size } = await initRes.json();
    
    let uploadedBytes = existing_size || 0;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    const queue = [];
    for (let i = Math.floor(uploadedBytes / CHUNK_SIZE); i < totalChunks; i++) queue.push(i);

    window[`cancel_${uid}`] = false;

    const worker = async () => {
        while (queue.length > 0 && !window[`cancel_${uid}`]) {
            const index = queue.shift();
            const start = index * CHUNK_SIZE;
            const end = Math.min(file.size, start + CHUNK_SIZE);
            const chunk = file.slice(start, end);

            const fd = new FormData();
            fd.append('file', chunk);
            fd.append('uid', uid);
            fd.append('offset', start);

            try {
                await fetch('/upload_chunk', { method: 'POST', body: fd });
                uploadedBytes += chunk.size;
                
                // Metrics
                const elapsed = (Date.now() - startTime) / 1000;
                const mbps = (uploadedBytes / 1024 / 1024 / (elapsed || 1)).toFixed(2);
                const pct = ((uploadedBytes / file.size) * 100).toFixed(1);
                const eta = Math.round((file.size - uploadedBytes) / (uploadedBytes / (elapsed || 1)));

                document.getElementById(`bar-${uid}`).style.width = pct + '%';
                document.getElementById(`stats-${uid}`).innerText = `${(uploadedBytes/1024/1024).toFixed(1)}MB / ${(file.size/1024/1024).toFixed(1)}MB`;
                document.getElementById(`speed-${uid}`).innerText = `${mbps} MB/S`;
                document.getElementById(`eta-${uid}`).innerText = `ETA: ${eta}S`;
            } catch (e) {
                queue.push(index); // Network retry
            }
        }
    };

    // Parallel Worker Spawning
    await Promise.all(Array(Math.min(PARALLEL_WORKERS, queue.length)).fill(0).map(worker));

    if (!window[`cancel_${uid}`]) {
        document.getElementById(`speed-${uid}`).innerText = "PUSHING TO HUB...";
        fetch(`/finalize?uid=${uid}&name=${encodeURIComponent(file.name)}`);
    }
}

function cancelTask(uid) {
    window[`cancel_${uid}`] = true;
    delete activeUploads[uid];
    localStorage.setItem('hf_uploads', JSON.stringify(activeUploads));
    fetch(`/cancel?uid=${uid}`);
    document.getElementById(`card-${uid}`).remove();
}

socket.on('complete', (data) => {
    const speedEl = document.getElementById(`speed-${data.uid}`);
    if (speedEl) {
        speedEl.innerText = "STORED ✅";
        speedEl.className = "text-green-400 font-black italic tracking-widest";
        delete activeUploads[data.uid];
        localStorage.setItem('hf_uploads', JSON.stringify(activeUploads));
        refreshList();
    }
});

async function refreshList() {
    const res = await fetch('/list_files');
    const files = await res.json();
    document.getElementById('file-list').innerHTML = files.map(f => `
        <div class="flex justify-between items-center p-3 bg-slate-900/40 rounded-xl border border-slate-800 text-[10px] font-mono group hover:bg-slate-800">
            <span class="truncate pr-4 text-slate-300">📄 ${f}</span>
            <span class="text-cyan-500/50 uppercase font-black italic">Safe</span>
        </div>
    `).join('');
}
refreshList();
</script>
</body>
</html>
"""

# ================= SERVER LOGIC =================

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/init")
def init():
    uid = request.args.get('uid')
    path = f"{UPLOAD_DIR}/{uid}.part"
    existing_size = os.path.getsize(path) if os.path.exists(path) else 0
    return jsonify({"existing_size": existing_size})

@app.route("/upload_chunk", methods=["POST"])
def upload_chunk():
    uid = request.form['uid']
    offset = int(request.form['offset'])
    file_chunk = request.files['file'].read()
    
    path = f"{UPLOAD_DIR}/{uid}.part"
    # 'r+b' allows writing to specific parts of a file simultaneously
    mode = "r+b" if os.path.exists(path) else "wb"
    
    with open(path, mode) as f:
        f.seek(offset)
        f.write(file_chunk)
    return jsonify({"status": "ok"})

@app.route("/finalize")
def finalize():
    uid = request.args.get('uid')
    name = request.args.get('name')
    path = f"{UPLOAD_DIR}/{uid}.part"

    def background_task():
        try:
            api.upload_file(
                path_or_fileobj=path,
                path_in_repo=name,
                repo_id=DATASET_ID,
                repo_type="dataset",
                token=HF_TOKEN
            )
            if os.path.exists(path):
                os.remove(path)
            socketio.emit('complete', {'uid': uid})
        except Exception as e:
            print(f"HF Error: {e}")

    # Eventlet non-blocking thread
    eventlet.spawn(background_task)
    return jsonify({"status": "queued"})

@app.route("/cancel")
def cancel():
    uid = request.args.get('uid')
    path = f"{UPLOAD_DIR}/{uid}.part"
    if os.path.exists(path):
        os.remove(path)
    return jsonify({"status": "deleted"})

@app.route("/list_files")
def list_files():
    try:
        files = api.list_repo_files(repo_id=DATASET_ID, repo_type="dataset", token=HF_TOKEN)
        return jsonify([f for f in files if "." in f])
    except:
        return jsonify([])

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=7860)
