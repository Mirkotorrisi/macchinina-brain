"""Ascolto continuo: tiene acceso il microfono del robot e stampa su stdout una riga per ogni frase che mi riguarda.

Pensato per essere lanciato con lo strumento Monitor di Claude Code: ogni riga stampata sveglia Claude, che risponde
con il tool `say`. Si ferma con TaskStop (il microfono sul Pi si spegne da solo entro 15 s).

Riservatezza: arrivano a Claude solo le frasi che contengono la parola chiave (default "macchinina", anche storpiata)
o che sono dette entro DIALOG_S dopo che il robot ha parlato (conversazione in corso). Le altre vengono scartate
senza essere stampate né salvate. WAKE_WORD=tutto le fa passare tutte.
"""
import os
import re
import sys
import time

import paho.mqtt.client as mqtt

from ear import Ear

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
WAKE = os.getenv("WAKE_WORD", r"macchin|mattin|machin|maccin")
DIALOG_S = float(os.getenv("DIALOG_S", "25"))  # dopo che il robot ha parlato, ogni frase passa senza parola chiave
KEEPALIVE_S = 5.0  # sul Pi il microfono scade dopo 15 s senza "ON"

wake_re = None if WAKE.lower() == "tutto" else re.compile(WAKE, re.IGNORECASE)
dialog_until = 0.0


def log(*args):
    print(*args, file=sys.stderr, flush=True)


def on_text(text):
    if wake_re is not None and not wake_re.search(text) and time.time() > dialog_until:
        log("(frase scartata: senza parola chiave)")
        return
    print(f"VOCE: {text}", flush=True)  # stdout: una riga = una notifica a Claude


ear = Ear(on_text, model_size=os.getenv("WHISPER_MODEL", "small"))


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe([("robot/audio_in", 0), ("robot/ai/speaking", 0)])


def on_message(client, userdata, msg):
    global dialog_until
    if msg.topic == "robot/audio_in":
        ear.feed(msg.payload)
    elif msg.topic == "robot/ai/speaking":  # il robot sta per parlare per N secondi: non sentirsi, poi dialogo
        try:
            seconds = float(msg.payload.decode())
        except ValueError:
            return
        ear.mute(seconds + 1.5)
        dialog_until = time.time() + seconds + DIALOG_S


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect_async(MQTT_HOST, 1883, 10)
client.loop_start()

print("VOCE: (ascolto continuo attivo)", flush=True)
try:
    while True:
        client.publish("robot/audio/control", "ON")
        time.sleep(KEEPALIVE_S)
except KeyboardInterrupt:
    pass
finally:
    client.publish("robot/audio/control", "OFF")
    time.sleep(0.3)
