"""Buzón de logs de Central: memoria acotada y lectura incremental para el panel."""
from collections import deque
from datetime import datetime
import threading
import uuid


class CentralLogs:
    def __init__(self, capacity=500):
        self.entries = deque(maxlen=capacity)
        self.lock = threading.Lock()
        self.session = uuid.uuid4().hex
        self.sequence = 0

    def append(self, message, level="INFO"):
        with self.lock:
            self.sequence += 1
            entry = dict(id=self.sequence, time=datetime.now().isoformat(timespec="seconds"),
                         level=level, message=str(message)[:4000])
            self.entries.append(entry)
            return dict(entry)

    def read(self, after=0, session=None):
        with self.lock:
            # Tras reiniciar Central, los IDs vuelven a empezar. La sesión
            # permite al navegador reconocerlo y continuar sin quedarse parado.
            reset = bool(session and session != self.session) or after > self.sequence
            if reset:
                after = 0
            oldest = self.entries[0]["id"] if self.entries else self.sequence + 1
            return dict(session=self.session, last_id=self.sequence, reset=reset,
                        truncated=after > 0 and after < oldest - 1,
                        entries=[dict(entry) for entry in self.entries if entry["id"] > after])
