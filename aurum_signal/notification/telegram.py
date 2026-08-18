"""Notifiche Telegram, facoltative e non bloccanti.

Vincolo che decide il disegno: una notifica non deve MAI ritardare una
decisione. Telegram puo' impiegare secondi o non rispondere affatto; su un
segnale con trenta secondi di anticipo, aspettarlo dentro il loop
significherebbe consegnare in ritardo proprio il messaggio che serviva presto.
Percio' l'invio sta su una coda servita da un thread, e il motore non aspetta
mai la rete.
"""

from __future__ import annotations

import json
import queue
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


def _hhmmss(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%H:%M:%S")


class TelegramNotifier:
    """Invia in background. Se non e' configurato, non fa nulla e lo dice."""

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.token = cfg.telegram_bot_token
        self.chat_id = cfg.telegram_chat_id
        self.enabled = bool(cfg.telegram_enabled and self.token and self.chat_id)
        self._queue: "queue.Queue[str]" = queue.Queue(maxsize=200)
        self._thread: threading.Thread | None = None
        self._running = False
        self.sent = 0
        self.failed = 0
        self.last_error: str | None = None
        self.last_latency_ms: float | None = None

    def start(self) -> None:
        if not self.enabled or self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="telegram",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False

    def _loop(self) -> None:
        while self._running:
            try:
                text = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            self._send_now(text)

    def _send_now(self, text: str) -> bool:
        started = time.perf_counter()
        try:
            data = urllib.parse.urlencode({
                "chat_id": self.chat_id, "text": text,
                "parse_mode": "HTML", "disable_web_page_preview": "true",
            }).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{self.token}/sendMessage", data=data)
            with urllib.request.urlopen(req, timeout=8) as resp:
                ok = resp.status == 200
            self.last_latency_ms = (time.perf_counter() - started) * 1000.0
            if ok:
                self.sent += 1
                self.last_error = None
            else:
                self.failed += 1
            return ok
        except Exception as exc:  # noqa: BLE001 - una notifica non ferma il motore
            self.failed += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            return False

    def send(self, text: str) -> bool:
        """Accoda. Non aspetta mai la rete."""
        if not self.enabled:
            return False
        try:
            self._queue.put_nowait(text)
            return True
        except queue.Full:
            self.failed += 1
            self.last_error = "coda piena"
            return False

    # ------------------------------------------------------------- messaggi
    def signal_message(self, sig, cfg) -> str:
        icon = "🟢" if sig.direction == "CALL" else "🔴"
        lead = max(0, (sig.entry_ts - sig.created_ts) // 1000)
        return (
            f"{icon} <b>AURUM M60 {sig.direction}</b>\n"
            f"{cfg.symbol}\n"
            f"Entrata: <b>{_hhmmss(sig.entry_ts)}</b> UTC\n"
            f"Scadenza: {_hhmmss(sig.expiry_ts)} UTC\n"
            f"Anticipo: {lead} sec\n"
            f"Probabilita': {sig.probability:.1%}\n"
            f"Confidenza: {sig.confidence:.1%}\n"
            f"Vantaggio sul pareggio: {sig.edge:+.1%}\n"
            f"Regime: {sig.regime}\n"
            f"Rischio news: {sig.news_risk}\n"
            f"<i>Operazione MANUALE: il sistema non esegue nulla.</i>")

    def cancel_message(self, sig) -> str:
        return (f"⚪️ <b>ANNULLATO</b> {sig.direction} delle "
                f"{_hhmmss(sig.entry_ts)}\n{sig.cancelled_reason}")

    def result_message(self, sig, balance: float, currency: str = "EUR") -> str:
        icon = {"WIN": "✅", "LOSS": "❌", "DRAW": "➖"}.get(sig.result or "", "❔")
        pnl = f"{sig.pnl:+.2f}" if sig.pnl is not None else "—"
        return (f"{icon} <b>{sig.result}</b> {sig.direction} "
                f"{_hhmmss(sig.entry_ts)}\n"
                f"Ingresso {sig.entry_price} → uscita {sig.expiry_price}\n"
                f"P&L {pnl} · saldo virtuale {balance:.2f} {currency}")

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured": bool(self.token and self.chat_id),
            "sent": self.sent, "failed": self.failed,
            "queued": self._queue.qsize(),
            "last_error": self.last_error,
            "last_latency_ms": (round(self.last_latency_ms, 1)
                                if self.last_latency_ms is not None else None),
            "note": ("Le notifiche non stanno sul percorso della decisione: "
                     "un Telegram lento non ritarda un segnale."),
        }
