import paho.mqtt.client as mqtt
import requests
import os
import time

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434/api/generate")

ALLOWED_COMMANDS = {"HELLO", "FORWARD", "STOP"}

def ask_gemma(text):
    prompt = f"""
    Analyze the text: '{text}'.
    Rules:
    - If it's a greeting (ciao, hello, ehi): reply ONLY 'HELLO'
    - If it's a move command (avanti, vieni): reply ONLY 'FORWARD'
    - If it's stop/fermati: reply ONLY 'STOP'
    - If it's nonsense or noise: reply ONLY 'IGNORE'
    
    CRITICAL: Respond with ONE WORD only. No explanations. No analysis.
    Response:"""
    try:
        # Aggiungiamo 'stop' e 'temperature' per forzare la brevità
        r = requests.post(OLLAMA_URL, json={
            "model": "gemma:2b", 
            "prompt": prompt, 
            "stream": False,
            "options": {
                "temperature": 0.0, # Mantiene la risposta deterministica
                "stop": ["\n", "."]  # Si ferma alla prima riga o punto
            }
        }, timeout=5)
        answer = r.json().get('response', 'IGNORE').strip().upper()
        return answer if answer in ALLOWED_COMMANDS else "IGNORE"
    except Exception:
        return "IGNORE"
def on_message(client, userdata, msg):
    text = msg.payload.decode()
    print(f"📩 Ricevuto dal robot: '{text}'")
    
    command = ask_gemma(text)
    
    if "IGNORE" not in command:
        print(f"🤖 Gemma ha deciso: {command}")
        client.publish("robot/commands", command)
    else:
        print("🤫 Rumore ignorato.")

# In brain.py sul Mac
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) # Assicurati sia v2
client.on_message = on_message
client.connect(MQTT_HOST, 1883)
client.subscribe("robot/speech")

# Invece di loop_forever, usa questo:
client.loop_start() 
print("🧠 Cervello MQTT attivo e in ascolto...")

while True:
    time.sleep(1) # Mantiene il container vivo