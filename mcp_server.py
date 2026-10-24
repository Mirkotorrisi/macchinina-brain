"""MCP server: dà a Claude un corpo (Raspbot) via MQTT. Gira sul Mac, in stdio, lanciato da .mcp.json.

Non sostituisce nessun livello di sicurezza: timeout dei movimenti, stop a 200 mm e watchdog
restano sul Pi. Qui si aggiungono solo limiti di durata ai comandi di Claude.
"""
import json
import os
import threading
import time
import urllib.request
from collections import deque
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP, Image

load_dotenv(Path(__file__).with_name(".env"))
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")  # il broker del compose espone 1883 sul Mac
ROBOT_HOST = os.getenv("ROBOT_HOST", "macchinina.local")  # nome mDNS: l'IP del Pi cambia (DHCP). Il server gira sul Mac, non in Docker

MOVES = {"forward": "FORWARD", "backward": "BACKWARD", "left": "LEFT", "right": "RIGHT"}
CAMERA = {"left": "CAM_LEFT", "right": "CAM_RIGHT", "up": "CAM_UP", "down": "CAM_DOWN", "center": "CAM_CENTER"}
MAX_DRIVE_S = 3.0  # il Pi scade dopo 0,8 s: qui si rinnova ogni 250 ms, ma mai oltre questo tetto
REPEAT_S = 0.25

state = {"perception": None, "perception_at": 0.0}
speech = deque(maxlen=50)  # (timestamp, testo) non ancora letti
speech_lock = threading.Condition()


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe([("robot/perception", 0), ("robot/speech", 0)])


def on_message(client, userdata, msg):
    if msg.topic == "robot/perception":
        try:
            state["perception"] = json.loads(msg.payload.decode())
            state["perception_at"] = time.time()
        except ValueError:
            pass
    elif msg.topic == "robot/speech":
        with speech_lock:
            speech.append((time.time(), msg.payload.decode()))
            speech_lock.notify_all()


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
    """Aspetta fino a timeout_s secondi (max 30) che qualcuno parli al robot e restituisce le trascrizioni Whisper non ancora lette. Se nessuno parla torna una stringa vuota di risultati."""
    timeout_s = max(0.0, min(float(timeout_s), 30.0))
    deadline = time.time() + timeout_s
    with speech_lock:
        while not speech and time.time() < deadline:
            speech_lock.wait(deadline - time.time())
        heard = [{"age_s": round(time.time() - t, 1), "text": text} for t, text in speech]
        speech.clear()
    return heard or "nessuno ha parlato"


@mcp.tool()
def autonomy_off() -> str:
    """Spegne l'autonomia a regole del Pi (OFF) e ferma i motori."""
    mqtt_client.publish("robot/autonomy/control", "OFF", qos=1)
    send("STOP")
    return "autonomia OFF, motori fermi"


if __name__ == "__main__":
    mcp.run()
