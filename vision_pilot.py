"""Pilota visivo sul Mac: il robot va verso ciò che gli si dice a parole ("frigorifero", "porta", "divano"...).

Un processo residente: tiene caricato OWLv2 (rilevatore a vocabolario aperto, GPU Apple in mezza precisione, ~190 ms a
immagine), prende i fotogrammi dallo stream del Pi e guida con `robot/ai/drive`. La logica di sterzata, ricerca e arresto è
quella di `macchina-setup/pilot.py` (Pilot): qui si aggiungono solo il rilevatore e un adattatore che trasforma "hw" in MQTT.

Protezioni: ogni comando drive vale 0,5 s sul Pi (se questo processo o la rete si fermano, il robot si ferma da solo),
il Pi rifiuta l'avanti con distanza ignota o sotto 200 mm e dà la precedenza all'operatore (pagina web, tastiera).

Avvio:  uv run --python 3.12 --with torch --with transformers --with pillow --with scipy --with paho-mqtt --with numpy \\
            python macchinina-brain/vision_pilot.py
Richiesta (MQTT robot/ai/seek):  {"target": "frigorifero", "stop_area": 0.35, "timeout_s": 25}
Esito (MQTT robot/ai/status):    {"approach": "arrivato|non trovato|perso|bloccato|timeout|annullato|operatore", ...}
"""
import hashlib
import io
import json
import os
import re
import sys
import threading
import time
import urllib.request
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "macchina-setup"))
import pilot as pl  # noqa: E402

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
ROBOT_HOST = os.getenv("ROBOT_HOST", "macchinina.local")  # nome mDNS: l'IP del Pi cambia (DHCP)
SNAPSHOT = f"http://{ROBOT_HOST}:8081/snapshot"
MODEL_ID = "google/owlv2-base-patch16-ensemble"
HOME = (90, 30)  # pan, tilt di "guarda avanti" (come CAMERA_HOME sul Pi)
SCORE_MIN = float(os.getenv("SEEK_SCORE_MIN", "0.2"))  # punteggio minimo perché un riquadro valga come bersaglio
RIVAL_RATIO = 0.9  # un altro oggetto "ruba" il riquadro se il suo punteggio supera questa frazione di quello del bersaglio
DRIVE_S = 0.5  # validità di ogni comando drive sul Pi (il massimo accettato): copre un ciclo lento
DEBUG_DIR = os.getenv("SEEK_DEBUG_DIR")  # se impostata, ogni fotogramma analizzato viene salvato con il punteggio nel nome
STATUS_MAX_AGE = 0.8  # una distanza più vecchia di così vale "ignota"
REFUSAL_S = 0.35  # un rifiuto del Pi pesa solo sul ciclo successivo
OWN_STOP_GRACE = 0.5  # i nostri stessi STOP tornano indietro dal broker: non vanno scambiati per quelli di qualcun altro

PROMPTS = {
    "frigorifero": "a refrigerator", "frigo": "a refrigerator", "forno": "an oven", "divano": "a sofa",
    "sedia": "a chair", "porta": "a door", "persona": "a person", "letto": "a bed", "tavolo": "a table",
    "tv": "a television", "televisione": "a television", "lavandino": "a sink", "pianta": "a potted plant",
    "orsacchiotto": "a teddy bear", "finestra": "a window", "armadio": "a wardrobe", "scaffale": "a bookshelf",
    "lavatrice": "a washing machine", "specchio": "a mirror", "culla": "a baby crib", "box": "a playpen",
}
DISTRACTORS = ["a door", "a refrigerator", "an oven", "a sofa", "a chair", "a person", "a table", "a bed",
               "a window", "a wardrobe", "a wall"]  # oggetti simili: se vincono loro su un riquadro, quello non è il bersaglio
FREE_TEXT = re.compile(r"^[a-zA-Z ]{3,40}$")  # un bersaglio in inglese libero ("red bag") è ammesso


def prompt_for(target):
    """Testo da cercare per un bersaglio: nome italiano noto, oppure inglese libero. None se non valido."""
    if not isinstance(target, str):
        return None
    key = target.strip().lower()
    if key in PROMPTS:
        return PROMPTS[key]
    return "a " + key if FREE_TEXT.match(key) else None


class Targets:
    """names() per Pilot: ammette tutto ciò per cui prompt_for dà un testo."""

    def __contains__(self, target):
        return prompt_for(target) is not None


def choose(scores, boxes, frame_wh, thr=SCORE_MIN):
    """Il miglior riquadro del bersaglio, o None.

    scores (N, Q): probabilità per query, la query 0 è il bersaglio e le altre i "rivali". boxes (N, 4): cx, cy, w, h
    normalizzati nel quadrato (lato = max(W, H)) su cui OWLv2 lavora. Un riquadro conta se il bersaglio supera thr e
    non è battuto da un rivale. Ritorna (x, area, punteggio) con x in 0..1 (0 = sinistra) e area = frazione del fotogramma.
    """
    t = scores[:, 0]
    rival = scores[:, 1:].max(axis=1) if scores.shape[1] > 1 else np.zeros_like(t)
    ok = (t >= thr) & (t >= RIVAL_RATIO * rival)
    if not ok.any():
        return None
    i = int(np.argmax(np.where(ok, t, -1.0)))
    w_img, h_img = frame_wh
    side = max(w_img, h_img)
    cx, _cy, bw, bh = (float(v) for v in boxes[i])
    x = min(1.0, max(0.0, cx * side / w_img))
    area = min(1.0, (bw * side) * (bh * side) / (w_img * h_img))
    return x, area, float(t[i])


class FrameGuard:
    """Scarta i fotogrammi vecchi o congelati: con la camera bloccata ustreamer restituisce ancora l'ultima immagine."""

    MAX_AGE_S = 0.6  # età massima (invio - cattura, entrambi sul Pi)
    MAX_IDENTICAL = 3  # tanti fotogrammi byte-identici di fila = camera congelata

    def __init__(self):
        self._last, self._same = None, 0

    def accept(self, headers, data):
        if str(headers.get("X-UStreamer-Online", "true")).lower() != "true":
            return False
        try:
            if float(headers["X-UStreamer-Send-Timestamp"]) - float(headers["X-UStreamer-Grab-Timestamp"]) > self.MAX_AGE_S:
                return False
        except (KeyError, TypeError, ValueError):
            pass  # senza i timestamp resta il controllo dei fotogrammi identici
        digest = hashlib.md5(data).digest()
        self._same = self._same + 1 if digest == self._last else 0
        self._last = digest
        return self._same < self.MAX_IDENTICAL


class OwlDetector:
    """OWLv2 sulla GPU Apple (mezza precisione). set_target() sceglie cosa cercare; detect() lavora su un fotogramma."""

    def __init__(self):
        import torch
        from transformers import Owlv2ForObjectDetection, Owlv2Processor

        self.torch = torch
        self.device = "mps" if torch.backends.mps.is_available() else "cpu"
        self.dtype = torch.float16 if self.device == "mps" else torch.float32
        self.proc = Owlv2Processor.from_pretrained(MODEL_ID)
        self.model = Owlv2ForObjectDetection.from_pretrained(MODEL_ID, torch_dtype=self.dtype).eval().to(self.device)
        self._lock = threading.Lock()
        self._text = None
        self._name = None
        self._guard = FrameGuard()
        self._stats = {"best": None, "rival": None, "ok": 0, "scartati": 0}
        self._frame_no = 0

    def set_target(self, target):
        prompt = prompt_for(target)
        queries = [prompt] + [d for d in DISTRACTORS if d != prompt]
        tok = self.proc.tokenizer(queries, padding="max_length", max_length=16, truncation=True, return_tensors="pt")
        with self._lock:
            self._text = (tok["input_ids"].to(self.device), tok["attention_mask"].to(self.device))
            self._name = target

    def grab(self, timeout=2):
        """Un fotogramma RGB (PIL) dallo snapshot del Pi, o None."""
        try:
            from PIL import Image

            with urllib.request.urlopen(SNAPSHOT, timeout=timeout) as r:
                data, headers = r.read(), r.headers
            if not self._guard.accept(headers, data):
                self._stats["scartati"] += 1
                return None  # fotogramma vecchio o camera congelata: per il pilota è "non vedo"
            self._stats["ok"] += 1
            return Image.open(io.BytesIO(data)).convert("RGB")
        except Exception:
            return None

    def detect(self, frame):
        """[(nome, punteggio, x, area)] del bersaglio corrente (lista vuota se non si vede)."""
        torch = self.torch
        with self._lock:
            text, name = self._text, self._name
        pixel = self.proc(images=frame, return_tensors="pt")["pixel_values"].to(self.device, self.dtype)
        with torch.no_grad():
            out = self.model(input_ids=text[0], attention_mask=text[1], pixel_values=pixel)
        logits = out.logits[0].float().cpu().numpy()
        boxes = out.pred_boxes[0].float().cpu().numpy()
        scores = 1.0 / (1.0 + np.exp(-logits))
        found = choose(scores, boxes, frame.size)
        self._stats["best"] = round(float(scores[:, 0].max()), 2)          # miglior punteggio del bersaglio, anche sotto soglia
        self._stats["rival"] = round(float(scores[:, 1:].max()), 2) if scores.shape[1] > 1 else None
        if DEBUG_DIR:
            self._frame_no += 1
            os.makedirs(DEBUG_DIR, exist_ok=True)
            frame.save(os.path.join(DEBUG_DIR, f"{self._frame_no:04d}_{name}_{self._stats['best']:.2f}.jpg"))
        return [] if found is None else [(name, found[2], found[0], found[1])]

    def info(self):
        """Diagnostica accodata allo stato di ogni ciclo."""
        return dict(self._stats)


class RobotLink:
    """Fa passare Pilot (pensato per girare sul Pi) per un robot lontano: comandi via MQTT, distanza dalla telemetria.

    Offre insieme le interfacce che Pilot si aspetta da "hw", "video" e "ploop".
    """

    def __init__(self, client, clock=time.monotonic):
        self.client, self.clock = client, clock
        self.hold_camera = False  # Pilot lo imposta; il Pi non fa panoramiche perché lo spegniamo a inizio ricerca
        self._distance, self._distance_t = None, -1e9
        self._blocked_until = -1e9
        self._refused_t = -1e9
        self._own_stop_t = -1e9

    # --- "hw"
    def distance(self):
        return self._distance if self.clock() - self._distance_t < STATUS_MAX_AGE else None

    def drive(self, left, right, duration=None, autonomous=False):
        connected = getattr(self.client, "is_connected", None)
        if connected is not None and not connected():
            return False  # senza broker il comando non parte: conta come rifiuto (dopo qualche ciclo si rinuncia)
        self.client.publish("robot/ai/drive", json.dumps({"l": round(left), "r": round(right), "s": DRIVE_S}))
        return self.clock() - self._refused_t > REFUSAL_S  # il Pi dice di no con un messaggio "rejected"

    def stop_all(self):
        self._own_stop_t = self.clock()
        self.client.publish("robot/ai/commands", "HALT", qos=1)  # fine regolare: HALT, non STOP (lo STOP sospende i comandi per 1 s)

    def set_camera(self, pan=None, tilt=None):
        self.client.publish("robot/ai/commands", "CAM_CENTER")  # CAM_CENTER = "guarda avanti" sul Pi

    # --- "video"
    def keepalive(self):
        self.client.publish("robot/video", "ON")

    # --- precedenza dell'operatore e telemetria
    def blocked(self):
        return max(0.0, self._blocked_until - self.clock())

    def own_stop_recent(self):
        return self.clock() - self._own_stop_t < OWN_STOP_GRACE

    def on_status(self, status):
        now = self.clock()
        if not isinstance(status, dict):
            return
        if "blocked_s" in status:
            try:
                self._blocked_until = now + float(status["blocked_s"])
            except (TypeError, ValueError):
                pass
            return
        if status.get("drive") in ("ok", "rejected"):
            d = status.get("distance_mm")
            self._distance, self._distance_t = (d if isinstance(d, (int, float)) else None), now
            if status["drive"] == "rejected":
                self._refused_t = now


class SeekService:
    """Riceve le richieste (robot/ai/seek) e le affida a Pilot; annulla se l'operatore o Claude comandano altro."""

    MOVES = ("FORWARD", "BACKWARD", "LEFT", "RIGHT", "STOP")

    def __init__(self, client, detector, clock=time.monotonic):
        self.client, self.detector = client, detector
        self.link = RobotLink(client, clock)
        targets = Targets()
        self.pilot = pl.Pilot(self.link, lambda: detector, self.link, self.link, self._publish_status,
                              self.link.blocked, lambda: targets, home=HOME)

    def _publish_status(self, status):
        self.client.publish("robot/ai/status", json.dumps(status))

    def handle(self, topic, payload):
        text = payload.decode(errors="ignore") if isinstance(payload, bytes) else str(payload)
        if topic == "robot/ai/seek":
            self._seek(text)
        elif topic == "robot/ai/status":
            try:
                self.link.on_status(json.loads(text))
            except (ValueError, RecursionError):
                pass
        elif topic == "robot/commands":  # l'operatore: qualunque movimento o STOP interrompe la ricerca
            if text.strip().upper() in self.MOVES:
                self.pilot.cancel()
        elif topic == "robot/ai/commands":  # Claude: uno STOP o un movimento interrompono (tranne i nostri STOP che tornano)
            if text.strip().upper() in self.MOVES and not self.link.own_stop_recent():
                self.pilot.cancel()
        elif topic in ("robot/ai/path", "robot/ai/approach"):  # un altro modo di guidare è partito: si cede il passo
            self.pilot.cancel()

    def _seek(self, text):
        try:
            cmd = json.loads(text) if len(text) <= 256 else None
        except (ValueError, RecursionError):
            cmd = None
        if not isinstance(cmd, dict):
            self._publish_status({"approach": "rejected", "reason": "JSON non valido"})
            return
        if self.link.blocked() > 0:
            self._publish_status({"approach": "operatore", "reason": f"precedenza ancora per {self.link.blocked():.0f} s"})
            return
        kwargs = {k: cmd[k] for k in ("stop_area", "timeout_s") if k in cmd}
        target = cmd.get("target")
        if prompt_for(target) is None:
            self._publish_status({"approach": "rejected", "reason": f"bersaglio non valido: {target!r}"})
            return
        target = target.strip().lower()  # lo stesso nome per il rilevatore e per Pilot (che confronta le stringhe)
        error = self.pilot.check(target, **kwargs)  # prima di toccare il rilevatore: una richiesta rifiutata non deve cambiare il bersaglio in corso
        if error:
            self._publish_status({"approach": "rejected", "reason": error})
            return
        self.client.publish("robot/perception/control", "OFF")  # niente panoramiche del servo mentre si guida
        self.detector.set_target(target)
        ok, error = self.pilot.approach(target, **kwargs)
        if not ok:
            self._publish_status({"approach": "rejected", "reason": error})


def main():
    import paho.mqtt.client as mqtt

    print("👁️ Carico OWLv2 (la prima volta scarica il modello)...", file=sys.stderr, flush=True)
    detector = OwlDetector()
    detector.set_target("sedia")
    t0 = time.time()
    from PIL import Image

    detector.detect(Image.new("RGB", (640, 480), (90, 90, 90)))  # riscaldamento: la prima inferenza è lenta
    print(f"👁️ Pronto ({detector.device}, riscaldamento {time.time() - t0:.1f} s)", file=sys.stderr, flush=True)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="vision-pilot")
    service = SeekService(client, detector)
    topics = ["robot/ai/seek", "robot/ai/status", "robot/commands", "robot/ai/commands", "robot/ai/path", "robot/ai/approach"]
    client.on_connect = lambda c, u, f, rc, p=None: c.subscribe([(t, 0) for t in topics])
    client.on_message = lambda c, u, m: service.handle(m.topic, m.payload)
    client.on_disconnect = lambda *a, **k: service.pilot.cancel()  # senza il broker non si guida
    client.connect_async(MQTT_HOST, 1883, 10)
    client.loop_start()
    print("👁️ In attesa di richieste su robot/ai/seek", file=sys.stderr, flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        service.pilot.cancel()
        client.loop_stop()


if __name__ == "__main__":
    main()
