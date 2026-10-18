import json
import os
import re
import threading
import time

ACTION_WINDOW = 0.8  # un comando vale come "azione in corso" per tanto (come la scadenza sul Pi)
MOVES = {"FORWARD", "BACKWARD", "LEFT", "RIGHT"}


class DemoRecorder:
    """Registra le guide manuali: cosa vede il robot (robot/perception) e quale comando dai tu.

    Un file JSONL per sessione in data/demos/. Due tipi di riga:
      src "cmd":  ogni comando che mandi (anche STOP), con lo stato più recente;
      src "tick": a ogni messaggio di percezione (~1 Hz), con l'azione ancora in corso o "NONE".
    """

    def __init__(self, root="data/demos", clock=time.time):
        self.root, self.clock = root, clock
        self.path = None
        self.rows = 0
        self.state = None
        self._cmd = ("NONE", -1e9)
        self._lock = threading.Lock()

    @property
    def active(self):
        return self.path is not None

    def start(self, goal):
        goal = re.sub(r"[^a-z0-9_-]+", "-", str(goal or "").strip().lower()).strip("-")[:30] or "libero"
        os.makedirs(self.root, exist_ok=True)
        with self._lock:
            self.path = os.path.join(self.root, f"{time.strftime('%Y%m%d-%H%M%S', time.localtime(self.clock()))}-{goal}.jsonl")
            self.rows, self.goal = 0, goal
            self._write({"src": "meta", "goal": goal})
        return self.path

    def stop(self):
        with self._lock:
            n, self.path = self.rows, None
        return n

    def update_state(self, perception):
        """Chiamare a ogni messaggio robot/perception."""
        with self._lock:
            self.state = perception
            if self.active:
                cmd, t = self._cmd
                action = cmd if self.clock() - t <= ACTION_WINDOW and cmd in MOVES else "NONE"
                self._write({"src": "tick", "action": action})

    def command(self, cmd):
        """Chiamare a ogni comando di guida (drive_command)."""
        with self._lock:
            self._cmd = (cmd, self.clock())
            if self.active:
                self._write({"src": "cmd", "action": cmd})

    def _write(self, row):
        p = self.state or {}
        row = {"t": round(self.clock(), 2), "goal": self.goal, **row}
        if row["src"] != "meta":
            row["state"] = {k: p.get(k) for k in ("room", "margin", "distance_mm", "objects", "text", "wifi")}
            row["state_age"] = round(self.clock() - p["t"], 1) if p.get("t") else None
        with open(self.path, "a") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.rows += 1
