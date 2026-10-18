from flask import Flask, render_template, request
import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
import socket
from flask_socketio import SocketIO
import threading

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

@socketio.on('drive_command')
def handle_drive(data):
    command = data.get('command')
    # Forza il publish e controlla se ha successo
    info = mqtt_client.publish("robot/commands", command, qos=1)
    # Questo ti dirà nei log se il messaggio è stato effettivamente inviato
    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"📤 Inviato a MQTT: {command}", flush=True)
    else:
        print(f"❌ Fallito invio MQTT: {info.rc}", flush=True)

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