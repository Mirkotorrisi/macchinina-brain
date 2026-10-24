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

state = {"perception": None, "perception_at": 0.0, "blocked_at": 0.0, "blocked_s": 0.0,  # blocked: rifiuto del Pi (l'operatore ha la precedenza)
         "dialog_until": 0.0,  # fino a quando ogni frase passa senza parola chiave (dopo che il robot ha parlato)
         "path_status": None, "path_at": 0.0,  # ultimo esito di un percorso (robot/ai/status)
         "approach_status": None, "approach_at": 0.0}  # ultimo messaggio di un inseguimento
WAKE = re.compile(os.getenv("WAKE_WORD", r"macchin|mattin|machin|maccin"), re.IGNORECASE)
DIALOG_S = float(os.getenv("DIALOG_S", "25"))
speech = deque(maxlen=50)  # (timestamp, testo) non ancora letti
speech_lock = threading.Condition()


def heard(text):
    if not WAKE.search(text) and time.time() > state["dialog_until"]:
        return  # non rivolta al robot (conversazioni in casa): scartata senza salvarla né passarla a Claude
    with speech_lock:
        speech.append((time.time(), text))
        speech_lock.notify_all()


ear = None  # creato alla prima chiamata a listen: con voice_watch.py in ascolto non si trascrive due volte


def get_ear():
    global ear
    if ear is None:
        ear = Ear(heard, model_size=os.getenv("WHISPER_MODEL", "small"))  # il modello si carica alla prima voce
    return ear


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe([("robot/perception", 0), ("robot/speech", 0), ("robot/audio_in", 0), ("robot/ai/status", 0)])


def on_message(client, userdata, msg):
    if msg.topic == "robot/audio_in":
        if ear is not None:
            ear.feed(msg.payload)
    elif msg.topic == "robot/ai/status":
        try:
            status = json.loads(msg.payload.decode())
            if "path" in status:  # fine/abort/rifiuto di un percorso (drive_path)
                state["path_status"], state["path_at"] = status, time.time()
            elif "approach" in status:  # avanzamento/esito di un inseguimento (approach)
                state["approach_status"], state["approach_at"] = status, time.time()
            elif "blocked_s" in status:  # solo questi dicono "l'operatore ha la precedenza" (non la telemetria né i rifiuti di drive)
                state["blocked_s"] = float(status["blocked_s"])
                state["blocked_reason"] = str(status.get("reason", "operatore"))
                state["blocked_at"] = time.time()
        except (ValueError, AttributeError):
            pass
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


def blocked_message():
    """Errore per i movimenti rifiutati dal Pi: precedenza dell'operatore oppure sospensione breve dopo uno STOP."""
    n = state["blocked_s"]
    if state.get("blocked_reason") == "stop recente":
        return {"error": f"movimenti sospesi ancora per ~{n:.0f} s dopo uno STOP (sicurezza): riprova tra poco."}
    return blocked_message()


def send(command):
    # niente comandi in coda: un movimento consegnato in ritardo a una riconnessione sarebbe pericoloso
    if not mqtt_client.is_connected():
        raise RuntimeError(f"broker MQTT non raggiungibile su {MQTT_HOST}:1883 (docker compose up?)")
    # topic separato da quello dell'operatore (pagina web, tastiera): il Pi dà la precedenza a lui, non a me
    # QoS 0 per i movimenti: una ritrasmissione dopo una riconnessione consegnerebbe un movimento in ritardo.
    # Lo STOP resta QoS 1: un duplicato è innocuo.
    mqtt_client.publish("robot/ai/commands", command, qos=1 if command in ("STOP", "HALT") else 0)


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
    Spegne l'autonomia a regole del Pi. Se Mirko sta guidando a mano (pagina web o tastiera) o ha premuto STOP,
    il Pi ignora i miei movimenti e drive torna un errore: non insistere, chiedi a lui."""
    cmd = MOVES.get(direction.lower())
    if cmd is None:
        return {"error": f"direction deve essere una tra {sorted(MOVES)}"}
    seconds = max(0.1, min(float(seconds), MAX_DRIVE_S))
    start = time.time()
    end = start + seconds
    blocked = None
    stop_error = None
    try:
        while time.time() < end:
            send(cmd)
            time.sleep(REPEAT_S)
            if state["blocked_at"] >= start:  # il Pi ha rifiutato: l'operatore ha la precedenza
                blocked = state["blocked_s"]
                break
    finally:
        if blocked is None:  # se l'operatore ha la precedenza il mio STOP fermerebbe anche il suo movimento
            try:
                send("HALT")  # fine regolare del movimento: non è un'emergenza, non sospende i comandi successivi
            except RuntimeError as e:  # broker caduto: il Pi ferma comunque il robot da solo dopo 0,8 s
                stop_error = str(e)
    if stop_error is not None:
        return {"error": f"STOP non inviato ({stop_error}). Il Pi ferma il robot da solo dopo 0,8 s."}
    if blocked is not None:
        return blocked_message()
    time.sleep(0.3)  # lascia arrivare una percezione fresca
    return {"moved": direction, "seconds": seconds, "perception": perception_summary()}


@mcp.tool()
def drive_path(steps: list[dict]) -> dict:
    """Esegue una sequenza di movimenti concatenati sul robot, senza pause né giri da me tra un passo e l'altro: movimenti fluidi.
    steps: lista di {"d": "forward|backward|left|right", "s": secondi}, ad esempio
    [{"d": "forward", "s": 0.5}, {"d": "right", "s": 0.3}, {"d": "forward", "s": 0.5}]. Massimo 12 passi, 2 s per passo, 8 s in tutto.
    Rotazioni: 0,2 s da fermo non partono su piastrelle, usa 0,3 s o più. Il Pi interrompe il percorso se Mirko prende il controllo
    (pagina web/tastiera) o preme STOP, se un passo viene rifiutato (ostacolo davanti), se i motori vengono fermati o se la distanza
    davanti è sconosciuta prima di un 'forward'. Torna l'esito (done/aborted, quanti passi, motivo) e la percezione."""
    if not mqtt_client.is_connected():
        raise RuntimeError(f"broker MQTT non raggiungibile su {MQTT_HOST}:1883 (docker compose up?)")
    try:
        total = sum(float(s["s"]) for s in steps)
    except (KeyError, TypeError, ValueError):
        return {"error": 'ogni passo deve essere {"d": direzione, "s": secondi}'}
    start = time.time()
    mqtt_client.publish("robot/ai/path", json.dumps(steps), qos=0)  # QoS 0: un percorso consegnato in ritardo sarebbe pericoloso
    deadline = start + total + 3.0
    while time.time() < deadline:
        if state["path_at"] >= start:
            break
        if state["blocked_at"] >= start:
            return blocked_message()
        time.sleep(0.05)
    else:
        send("STOP")
        return {"error": "nessuna risposta dal Pi: percorso annullato con uno STOP"}
    time.sleep(0.2)  # lascia arrivare una percezione fresca
    return {"path": state["path_status"], "perception": perception_summary()}


APPROACH_DONE = ("arrivato", "perso", "non trovato", "bloccato", "limite", "timeout", "annullato", "operatore", "rejected")
SEEK_ACK_S = 8.0  # il pilota visivo del Mac deve rispondere (started/running) entro questo tempo, altrimenti non è attivo


@mcp.tool()
def approach(target: str, stop_area: float = 0.3, timeout_s: float = 25.0, engine: str = "owlv2") -> dict:
    """Il robot va da solo verso ciò che si nomina, sterzando in continuo (nessun comando da me tra un passo e l'altro).
    target: nome italiano di un oggetto o luogo visibile (frigorifero, forno, divano, sedia, porta, persona, letto, tavolo, tv,
    lavandino, pianta, orsacchiotto, finestra, armadio, scaffale, lavatrice, specchio, culla, box) oppure testo libero in inglese
    ("red bag"). Lo riconosce il pilota visivo sul Mac (processo vision_pilot.py, rilevatore OWLv2 a vocabolario aperto, ~3 cicli
    al secondo): va avviato prima. engine="yolo" usa invece il vecchio rilevatore sul Pi (solo classi COCO, poco affidabile).
    stop_area: frazione dell'immagine che il bersaglio deve occupare per dire "sono arrivato" (0.05-0.9; 0.3 = vicino).
    timeout_s: 2-30 s, ricerca compresa. Se il bersaglio non si vede, il robot lo cerca girando piano sul posto (a sinistra, al massimo
    12 s). Si ferma da solo quando arriva, se lo perde per 2,5 s, a timeout, se Mirko prende il controllo o preme STOP (anche tu puoi
    chiamare stop). Non avanza se la distanza davanti è ignota o sotto 250 mm: in quel caso gira soltanto.
    Torna l'esito ('approach': arrivato|perso|non trovato|bloccato|limite|timeout|annullato|operatore|rejected), l'ultima posizione/area
    del bersaglio e la percezione."""
    if not mqtt_client.is_connected():
        raise RuntimeError(f"broker MQTT non raggiungibile su {MQTT_HOST}:1883 (docker compose up?)")
    visual = engine != "yolo"
    start = time.time()
    topic = "robot/ai/seek" if visual else "robot/ai/approach"
    mqtt_client.publish(topic, json.dumps({"target": target, "stop_area": stop_area, "timeout_s": timeout_s}), qos=0)
    deadline = start + float(timeout_s) + 6.0
    while time.time() < deadline:
        st = state["approach_status"]
        fresh = state["approach_at"] >= start and st
        if fresh and st.get("approach") in APPROACH_DONE:
            time.sleep(0.2)  # lascia arrivare una percezione fresca
            return {"result": st, "perception": perception_summary()}
        if state["blocked_at"] >= start:
            return blocked_message()
        if visual and not fresh and time.time() - start > SEEK_ACK_S:
            return {"error": "il pilota visivo non risponde: il processo vision_pilot.py sul Mac non è attivo (va avviato; il primo avvio scarica il modello).",
                    "avvio": "uv run --python 3.12 --with torch --with transformers --with pillow --with scipy --with paho-mqtt --with numpy python macchinina-brain/vision_pilot.py"}
        time.sleep(0.1)
    send("STOP")
    return {"error": "nessun esito dal pilota: inseguimento annullato con uno STOP"}


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
    """Accende il microfono del robot e aspetta fino a timeout_s secondi (max 30) che qualcuno parli; restituisce le frasi trascritte (Whisper sul Mac) non ancora lette, con age_s. Arrivano solo le frasi che contengono "macchinina" (anche storpiato) o dette entro 25 s dopo che il robot ha parlato: le altre conversazioni in casa vengono scartate. La prima volta Whisper può impiegare un minuto a scaricare il modello. Il microfono si spegne da solo pochi secondi dopo. Da fermo: i motori coprono la voce."""
    get_ear()
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
    state["dialog_until"] = time.time() + seconds + DIALOG_S
    if ear is not None:
        ear.mute(seconds + 1.5)
    mqtt_client.publish("robot/ai/speaking", f"{seconds:.1f}")  # voice_watch.py smette di ascoltare e apre la finestra di dialogo
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
