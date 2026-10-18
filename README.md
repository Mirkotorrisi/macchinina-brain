# macchinina-brain

Lato "Mac" del robot Macchinina: broker MQTT (Mosquitto), cervello vocale (Ollama `gemma:2b`) e web UI per guidare il robot.

## Configurazione
```bash
cp .env.example .env     # imposta ROBOT_IP e MAC_IP
```

## Avvio
```bash
ollama pull gemma:2b     # una tantum, Ollama deve essere in esecuzione
docker compose up -d --build
docker logs -f raspbot_controller
```
Apri http://localhost:5001/ — W/A/S/D guidano, frecce/C muovono la camera, V (tenuto premuto) parla al robot.

## Servizi
- `mosquitto` — broker MQTT, porta 1883 (anonimo: usare solo su rete fidata).
- `brain` — `robot/speech` → Gemma → `robot/commands` (solo `HELLO`, `FORWARD`, `STOP`).
- `controller` — Flask + Socket.IO (porta 5001), ponte audio sulla 9998.

`remote_control.py` permette di guidare da tastiera: `MQTT_HOST=localhost python3 remote_control.py`.
