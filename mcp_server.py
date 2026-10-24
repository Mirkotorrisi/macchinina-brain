"""MCP server: dà a Claude un corpo (Raspbot) via MQTT. Gira sul Mac, in stdio, lanciato da .mcp.json.

Non sostituisce nessun livello di sicurezza: timeout dei movimenti, stop a 200 mm e watchdog
restano sul Pi. Qui si aggiungono solo limiti di durata ai comandi di Claude.
"""
import json
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
import wave
from collections import deque
from pathlib import Path

import numpy as np
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP, Image

from ear import Ear

load_dotenv(Path(__file__).with_name(".env"))
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")  # il broker del compose espone 1883 sul Mac
ROBOT_HOST = os.getenv("ROBOT_HOST", "macchinina.local")  # nome mDNS: l'IP del Pi cambia (DHCP). Il server gira sul Mac, non in Docker

MOVES = {"forward": "FORWARD", "backward": "BACKWARD", "left": "LEFT", "right": "RIGHT"}
CAMERA = {"left": "CAM_LEFT", "right": "CAM_RIGHT", "up": "CAM_UP", "down": "CAM_DOWN", "center": "CAM_CENTER"}
MAX_DRIVE_S = 3.0  # il Pi scade dopo 0,8 s: qui si rinnova ogni 250 ms, ma mai oltre questo tetto
REPEAT_S = 0.25
SAY_MAX_CHARS = 300
SAY_RATE = 44100  # audio_server.py riproduce a 44100 Hz (aplay -r 44100)
SAY_PEAK = 0.08  # audio_server.py moltiplica x10 (pensato per il microfono del browser): così il picco arriva a ~0,8
SAY_CHUNK = 1024  # byte per invio (multiplo di 4): il server scarta la sessione se riceve float32 spezzati

state = {"perception": None, "perception_at": 0.0}
speech = deque(maxlen=50)  # (timestamp, testo) non ancora letti
speech_lock = threading.Condition()


def heard(text):
    with speech_lock:
        speech.append((time.time(), text))
        speech_lock.notify_all()


ear = Ear(heard, model_size=os.getenv("WHISPER_MODEL", "small"))  # il modello si carica alla prima voce


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe([("robot/perception", 0), ("robot/speech", 0), ("robot/audio_in", 0)])


def on_message(client, userdata, msg):
    if msg.topic == "robot/audio_in":
        ear.feed(msg.payload)
    elif msg.topic == "robot/perception":
        try:
            state["perception"] = json.loads(msg.payload.decode())
            state["perception_at"] = time.time()
        except ValueError:
            pass
    elif msg.topic == "robot/speech":
        heard(msg.payload.decode())


mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect_async(MQTT_HOST, 1883, 10)  # il server parte anche se il broker è giù; si riconnette da solo
mqtt_client.loop_start()

mcp = FastMCP("macchinina")


def send(command):
    # niente comandi in coda: un movimento consegnato in ritardo a una riconnessione sarebbe pericoloso
    if not mqtt_client.is_connected():
        raise RuntimeError(f"broker MQTT non raggiungibile su {MQTT_HOST}:1883 (docker compose up?)")
    mqtt_client.publish("robot/commands", command, qos=1)


def perception_summary():
    p = state["perception"]
    if p is None:
        return "nessuna percezione ricevuta: chiama perception_enable(true) e riprova"
    return {**p, "age_s": round(time.time() - state["perception_at"], 1)}


@mcp.tool()
def drive(direction: str, seconds: float = 1.0) -> dict:
    """Muove il robot per qualche secondo e poi lo ferma. direction: forward|backward|left|right.
    seconds è limitato a 3. Il Pi rifiuta di andare avanti sotto 200 mm di distanza e non c'è
    nessun sensore anti-caduta: guida solo a piccoli passi e controlla get_perception tra l'uno e l'altro.
    Spegne l'autonomia a regole del Pi."""
    cmd = MOVES.get(direction.lower())
    if cmd is None:
        return {"error": f"direction deve essere una tra {sorted(MOVES)}"}
    seconds = max(0.1, min(float(seconds), MAX_DRIVE_S))
    end = time.time() + seconds
    try:
        while time.time() < end:
            send(cmd)
            time.sleep(REPEAT_S)
    finally:
        send("STOP")
    time.sleep(0.3)  # lascia arrivare una percezione fresca
    return {"moved": direction, "seconds": seconds, "perception": perception_summary()}


@mcp.tool()
def stop() -> str:
    """Ferma subito i motori."""
    send("STOP")
    return "STOP inviato"


@mcp.tool()
def camera_look(direction: str) -> str:
    """Muove il servo della camera di un passo: left|right|up|down|center."""
    cmd = CAMERA.get(direction.lower())
    if cmd is None:
        return f"direction deve essere una tra {sorted(CAMERA)}"
    send(cmd)
    return f"{cmd} inviato"


@mcp.tool()
def perception_enable(on: bool = True) -> str:
    """Accende/spegne la percezione continua del Pi (oggetti, testo, distanza, WiFi, stanza). Di default è spenta e la camera resta spenta."""
    mqtt_client.publish("robot/perception/control", "ON" if on else "OFF", qos=1)
    return "percezione " + ("accesa" if on else "spenta")


@mcp.tool()
def get_perception() -> dict | str:
    """Ultimo stato percepito: oggetti visti, testo letto, distance_mm (ostacolo davanti), impronta WiFi, stanza stimata (null = non sicuro). Include age_s: se è alto la percezione è ferma."""
    return perception_summary()


@mcp.tool()
def get_camera_image() -> Image:
    """Scatta una foto con la camera del robot e la restituisce, così puoi vedere cosa c'è davanti."""
    last_error = None
    for _ in range(15):
        mqtt_client.publish("robot/video", "ON")  # ustreamer parte su richiesta e si spegne senza keepalive
        try:
            with urllib.request.urlopen(f"http://{ROBOT_HOST}:8081/snapshot", timeout=3) as r:
                return Image(data=r.read(), format="jpeg")
        except Exception as e:
            last_error = e
            time.sleep(1.0)
    raise RuntimeError(f"camera non disponibile: {last_error}")


@mcp.tool()
def listen(timeout_s: float = 10.0) -> list | str:
    """Accende il microfono del robot e aspetta fino a timeout_s secondi (max 30) che qualcuno parli; restituisce le frasi trascritte (Whisper sul Mac) non ancora lette, con age_s. La prima volta Whisper può impiegare un minuto a scaricare il modello. Il microfono si spegne da solo pochi secondi dopo. Da fermo: i motori coprono la voce."""
    timeout_s = max(0.0, min(float(timeout_s), 30.0))
    deadline = time.time() + timeout_s
    with speech_lock:
        while not speech and time.time() < deadline:
            mqtt_client.publish("robot/audio/control", "ON")  # keepalive: sul Pi scade dopo 15 s
            speech_lock.wait(min(5.0, deadline - time.time()))
        got = [{"age_s": round(time.time() - t, 1), "text": text} for t, text in speech]
        speech.clear()
    return got or "nessuno ha parlato"


@mcp.tool()
def say(text: str, voice: str = "Alice") -> str:
    """Il robot parla: sintesi vocale sul Mac (voce italiana, di default Alice; altre: Eddy, Flo) riprodotta dall'altoparlante del robot. Massimo 300 caratteri: frasi brevi. L'ascolto è sospeso mentre parla, per non sentire sé stesso. Torna quando ha finito di parlare."""
    text = text.strip()[:SAY_MAX_CHARS]
    if not text:
        return "testo vuoto"
    if not re.fullmatch(r"[A-Za-z]+( \([A-Za-z() ]+\))?", voice):
        return "voce non valida"
    with tempfile.TemporaryDirectory() as d:
        aiff, wav = Path(d) / "say.aiff", Path(d) / "say.wav"
        subprocess.run(["say", "-v", voice, "-o", str(aiff), "-f", "-"], input=text.encode(), check=True, timeout=30)
        subprocess.run(["afconvert", "-f", "WAVE", "-d", f"LEI16@{SAY_RATE}", "-c", "1", str(aiff), str(wav)],
                       check=True, timeout=30)
        with wave.open(str(wav)) as w:
            pcm = np.frombuffer(w.readframes(w.getnframes()), dtype=np.int16)
    audio = pcm.astype(np.float32) / 32768.0
    audio = audio / (float(np.abs(audio).max()) or 1.0) * SAY_PEAK
    audio = np.concatenate([audio, np.zeros(int(0.6 * SAY_RATE), dtype=np.float32)])  # coda: il server taglia aplay alla chiusura
    data = audio.astype("<f4").tobytes()
    seconds = len(audio) / SAY_RATE
    ear.mute(seconds + 1.5)
    with socket.create_connection((ROBOT_HOST, 9999), timeout=5) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        start = time.monotonic()
        per_chunk = SAY_CHUNK / 4 / SAY_RATE
        for n, i in enumerate(range(0, len(data), SAY_CHUNK)):
            s.sendall(data[i:i + SAY_CHUNK])
            time.sleep(max(0.0, start + (n + 1) * per_chunk * 0.97 - time.monotonic()))
    return f"detto ({seconds:.1f} s): {text}"


@mcp.tool()
def autonomy_off() -> str:
    """Spegne l'autonomia a regole del Pi (OFF) e ferma i motori."""
    mqtt_client.publish("robot/autonomy/control", "OFF", qos=1)
    send("STOP")
    return "autonomia OFF, motori fermi"


if __name__ == "__main__":
    mcp.run()
