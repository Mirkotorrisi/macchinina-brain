"""Orecchio sul Mac: riceve PCM 16 kHz mono 16 bit dal Pi, isola le frasi col VAD di faster-whisper e le trascrive.

I log vanno su stderr: nel server MCP stdout è il canale del protocollo.
"""
import sys
import threading
import time

import numpy as np

SAMPLE_RATE = 16000
SILENCE_END_S = 0.8  # silenzio dopo la voce che chiude una frase
PREROLL_S = 0.3  # un po' di audio prima e dopo la voce, per non mozzare le parole
MAX_UTTERANCE_S = 15  # frase più lunga di così: si trascrive comunque
KEEP_SILENCE_S = 1.0  # senza voce si tiene solo l'ultimo secondo (il buffer non cresce)
TICK_S = 0.4
NO_SPEECH_MAX = 0.6  # segmenti che Whisper stesso giudica "non parlato" vengono scartati
PROMPT = "Macchinina, ciao macchinina, vieni avanti, indietro, gira a destra, a sinistra, fermati, stop."


def log(*args):
    print(*args, file=sys.stderr, flush=True)


class Ear:
    def __init__(self, on_text, model_size="small"):
        self.on_text = on_text  # chiamata con (testo) per ogni frase trascritta
        self.model_size = model_size
        self._model = None
        self._buf = bytearray()
        self._lock = threading.Lock()
        self._vad = None
        self._mute_until = 0.0
        threading.Thread(target=self._worker, daemon=True).start()

    def mute(self, seconds):
        """Scarta l'audio per `seconds`: serve quando il robot parla, per non trascrivere la propria voce."""
        self._mute_until = time.time() + seconds
        with self._lock:
            self._buf.clear()

    def feed(self, pcm):
        if time.time() < self._mute_until:
            return
        with self._lock:
            self._buf += pcm

    def _load(self):
        if self._model is None:
            from faster_whisper import WhisperModel
            from faster_whisper.vad import get_speech_timestamps
            log(f"👂 Carico Whisper '{self.model_size}' (la prima volta scarica il modello)...")
            self._model = WhisperModel(self.model_size, device="cpu", compute_type="int8")
            self._vad = get_speech_timestamps
            log("👂 Whisper pronto")

    def _worker(self):
        while True:
            time.sleep(TICK_S)
            try:
                self._tick()
            except Exception as e:
                log(f"⚠️ Orecchio: {e}")
                with self._lock:
                    self._buf.clear()

    def _tick(self):
        with self._lock:
            raw = bytes(self._buf[: len(self._buf) // 2 * 2])
        if len(raw) < SAMPLE_RATE:  # meno di mezzo secondo (2 byte a campione)
            return
        self._load()
        audio = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
        spans = self._vad(audio, sampling_rate=SAMPLE_RATE)
        if not spans:
            self._drop(max(0, len(audio) - int(KEEP_SILENCE_S * SAMPLE_RATE)))
            return
        start, end = spans[0]["start"], spans[-1]["end"]
        silent = len(audio) - end
        if silent < SILENCE_END_S * SAMPLE_RATE and end - start < MAX_UTTERANCE_S * SAMPLE_RATE:
            return  # la frase è ancora in corso
        pad = int(PREROLL_S * SAMPLE_RATE)
        chunk = audio[max(0, start - pad): min(len(audio), end + pad)]
        self._drop(min(len(audio), end + pad))
        text = self._transcribe(chunk)
        if text:
            self.on_text(text)

    def _drop(self, samples):
        """Toglie dal buffer i primi `samples` campioni (già trascritti o inutili)."""
        with self._lock:
            del self._buf[: samples * 2]

    def _transcribe(self, audio):
        segments, _ = self._model.transcribe(audio, language="it", beam_size=1,
                                             vad_filter=False, initial_prompt=PROMPT)
        parts = [s.text.strip() for s in segments if s.no_speech_prob < NO_SPEECH_MAX]
        return " ".join(p for p in parts if p).strip()
