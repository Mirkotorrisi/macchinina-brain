from flask import Flask, render_template, request, jsonify
import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
import socket
from flask_socketio import SocketIO
import threading
import re
import json
import time
import urllib.request
from pathlib import Path
from demo_recorder import DemoRecorder

load_dotenv()
ROBOT_IP = os.getenv("ROBOT_IP")
print(f"🤖 Robot IP: {ROBOT_IP}", flush=True)
app = Flask(__name__)
socketio = SocketIO(app, 
    cors_allowed_origins="*",
    max_http_buffer_size=1000000,
    ping_interval=5,
    ping_timeout=10,
    async_handlers=True  # Permette di gestire più richieste audio in parallelo
)

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)


# In alto nel file app.py
AUDIO_CONNECTIONS = {}


def on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    # le sottoscrizioni vanno rifatte a ogni riconnessione al broker
    client.subscribe("robot/perception")
    client.subscribe("robot/autonomy/log")


mqtt_client.on_connect = on_mqtt_connect


def connect_mqtt():
    try:
        mqtt_client.connect(MQTT_HOST, 1883, 60)
        mqtt_client.loop_start() # Avvia il thread per gestire i messaggi
        print(f"✅ Connesso al Broker MQTT ({MQTT_HOST})", flush=True)
    except Exception as e:
        print(f"❌ Errore connessione MQTT: {e}", flush=True)

connect_mqtt()

@app.route('/')
def index():
    return render_template('index.html', ROBOT_IP=ROBOT_IP)

demos = DemoRecorder(os.getenv("DEMOS_DIR", "data/demos"))
last_perception = {}

# --- Dataset delle stanze: foto etichettate dallo snapshot di ustreamer sul Pi ---
ROOMS_DIR = Path(os.getenv("ROOMS_DIR", "data/rooms"))

def room_counts():
    if not ROOMS_DIR.exists():
        return {}
    return {d.name: len(list(d.glob("*.jpg"))) for d in sorted(ROOMS_DIR.iterdir()) if d.is_dir()}

@app.route('/api/rooms')
def api_rooms():
    return jsonify(room_counts())

@app.route('/api/rooms/capture', methods=['POST'])
def api_rooms_capture():
    label = re.sub(r"[^a-z0-9_-]+", "-", (request.get_json(silent=True) or {}).get("label", "").strip().lower()).strip("-")[:30]
    if not label:
        return jsonify(error="etichetta mancante"), 400
    if not ROBOT_IP:
        return jsonify(error="ROBOT_IP non impostato"), 500
    try:
        with urllib.request.urlopen(f"http://{ROBOT_IP}:8081/snapshot", timeout=5) as r:
            jpg = r.read()
    except Exception as e:
        return jsonify(error=f"camera non disponibile: {e}"), 503
    folder = ROOMS_DIR / label
    folder.mkdir(parents=True, exist_ok=True)
    stamp = int(time.time() * 1000)
    (folder / f"{stamp}.jpg").write_bytes(jpg)
    if last_perception.get("wifi"):  # impronta WiFi accanto alla foto, per confrontare WiFi e visione
        (folder / f"{stamp}.json").write_text(json.dumps({"wifi": last_perception["wifi"]}))
    return jsonify(label=label, counts=room_counts())

DRIVE_COMMANDS = {"FORWARD", "BACKWARD", "LEFT", "RIGHT", "STOP",
                  "CAM_LEFT", "CAM_RIGHT", "CAM_UP", "CAM_DOWN", "CAM_CENTER"}

@socketio.on('drive_command')
def handle_drive(data):
    command = data.get('command') if isinstance(data, dict) else None
    if command not in DRIVE_COMMANDS:  # stessa whitelist del Pi: niente payload arbitrari su robot/commands
        return
    demos.command(command)
    # Forza il publish e controlla se ha successo
    info = mqtt_client.publish("robot/commands", command, qos=1)
    # Questo ti dirà nei log se il messaggio è stato effettivamente inviato
    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"📤 Inviato a MQTT: {command}", flush=True)
    else:
        print(f"❌ Fallito invio MQTT: {info.rc}", flush=True)

# La camera del robot è accesa solo finché la pagina è aperta e visibile (keepalive ogni pochi secondi)
@socketio.on('video_keepalive')
def handle_video_keepalive():
    mqtt_client.publish("robot/video", "ON")

@socketio.on('video_off')
def handle_video_off():
    mqtt_client.publish("robot/video", "OFF")

# Registrazione delle guide manuali (data/demos/*.jsonl): stato percepito + comando dato
@socketio.on('demo_start')
def handle_demo_start(data):
    path = demos.start(data.get('goal', '') if isinstance(data, dict) else '')
    socketio.emit('demo_status', {'recording': True, 'file': os.path.basename(path), 'rows': 0})

@socketio.on('demo_stop')
def handle_demo_stop():
    rows = demos.stop()
    socketio.emit('demo_status', {'recording': False, 'rows': rows})

# Percezione del robot: interruttore dalla UI e flusso di ciò che vede (robot/perception)
@socketio.on('perception_toggle')
def handle_perception_toggle(data):
    mqtt_client.publish("robot/perception/control", "ON" if data.get('on') else "OFF")

# Autonomia: modo (OFF/DRYRUN/ON, "ON" va rinnovato ogni 10 s o il Pi si spegne da solo) e obiettivo
@socketio.on('autonomy_mode')
def handle_autonomy_mode(data):
    mode = data.get('mode') if isinstance(data, dict) else None
    if mode in ("OFF", "DRYRUN", "ON"):
        mqtt_client.publish("robot/autonomy/control", mode, qos=1)

@socketio.on('autonomy_goal')
def handle_autonomy_goal(data):
    room = data.get('room', '') if isinstance(data, dict) else ''
    mqtt_client.publish("robot/goal", re.sub(r"[^a-z0-9_-]+", "-", str(room).strip().lower())[:30], qos=1)

# Nel file app.py del Mac
@socketio.on('client_audio')
def handle_client_audio(data):
    sid = request.sid
    if sid not in AUDIO_CONNECTIONS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ROBOT_IP, 9999))
            AUDIO_CONNECTIONS[sid] = s
        except: return

    try:
        AUDIO_CONNECTIONS[sid].sendall(data)
    except:
        AUDIO_CONNECTIONS[sid].close()
        del AUDIO_CONNECTIONS[sid]

@socketio.on('end_audio') # Aggiungi questo evento quando rilasci il tasto V
def handle_end_audio():
    sid = request.sid
    if sid in AUDIO_CONNECTIONS:
        AUDIO_CONNECTIONS[sid].close()
        del AUDIO_CONNECTIONS[sid]

def handle_mqtt_message(client, userdata, message):
    if message.topic == "robot/audio_out":
        # Invia l'audio al browser via SocketIO
        socketio.emit('robot_audio', {'data': message.payload.decode()})
    elif message.topic == "robot/perception":
        p = json.loads(message.payload.decode())
        last_perception.clear()
        last_perception.update(p)
        demos.update_state(p)
        if demos.active:
            socketio.emit('demo_status', {'recording': True, 'rows': demos.rows})
        socketio.emit('perception', p)
    elif message.topic == "robot/autonomy/log":
        socketio.emit('autonomy_log', json.loads(message.payload.decode()))
    elif message.topic == "robot/speech":
        # Invia la trascrizione di Whisper al browser
        socketio.emit('speech_transcript', {'text': message.payload.decode()})

def robot_audio_bridge():
    # Creiamo un socket che sta in ascolto del Robot
    bridge_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bridge_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    bridge_socket.bind(('0.0.0.0', 9998))
    bridge_socket.listen(1)
    print("👂 Bridge Audio Robot-Mac attivo sulla porta 9998")

    while True:
        conn, addr = bridge_socket.accept()
        print(f"🔗 Robot connesso per streaming audio: {addr}")
        try:
            while True:
                # Riceve i byte grezzi (Int16 Mono)
                data = conn.recv(4096)
                print(f"📥 Ricevuti {len(data)} byte audio dal Robot")
                if not data: break
                # Spedisce i byte al Browser via Socket.IO
                socketio.emit('robot_audio_stream', data)
        except Exception as e:
            print(f"⚠️ Errore bridge audio: {e}")
        finally:
            conn.close()

threading.Thread(target=robot_audio_bridge, daemon=True).start()
mqtt_client.on_message = handle_mqtt_message

if __name__ == '__main__':
    # '0.0.0.0' è fondamentale: dice a Flask di ascoltare 
    # tutte le connessioni, non solo quelle interne al container
    socketio.run(app, host='0.0.0.0', port=5000)