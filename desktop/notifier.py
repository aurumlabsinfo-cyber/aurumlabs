#!/usr/bin/env python3
"""BTC 5-Second Quant Engine - desktop notifier.

Runs on YOUR machine (not in Docker), holds one WebSocket open to the backend,
and fires a native OS notification the moment the engine changes a signal's
state. No browser tab needed.

    python3 notifier.py                     # connect to localhost:8000
    python3 notifier.py --url ws://box:8000 --min-confidence 0.75
    python3 notifier.py --events trigger_hit,signal_settled --sound

Why a persistent socket and not a poll every minute
---------------------------------------------------
A 5-second prediction has to be delivered inside those 5 seconds. If you sample
once a minute, the window you would be predicting closed 55 seconds before your
next look. The horizon must be far shorter than the sampling interval, not
twelve times longer. A held-open WebSocket costs nothing and delivers in
milliseconds, so it is both the cheaper and the only workable option.

It reads the engine's decisions - it does not make any of its own, and it does
not read your screen. The market data the engine uses is exact and free; a
screenshot of a chart is a lossy, delayed rendering of the same numbers.

Requires: pip install websockets
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# --------------------------------------------------------------------- events
#: Lifecycle events the backend broadcasts, in order of occurrence.
ALL_EVENTS = (
    "signal_created",
    "trigger_hit",
    "trade_active",
    "trade_expired",
    "signal_settled",
    "signal_cancelled",
)

#: Sensible default: the two moments that need your attention. `trade_active`
#: and `trade_expired` fire within 5s of `trigger_hit` and would only add noise.
DEFAULT_EVENTS = ("signal_created", "trigger_hit", "signal_settled")

DIRECTION_IT = {"UP": "SU", "DOWN": "GIÙ", "NO_TRADE": "NO TRADE"}
ARROW = {"UP": "↑", "DOWN": "↓", "NO_TRADE": "—"}

TITLES = {
    "signal_created": "NUOVO SEGNALE",
    "trigger_hit": "TRIGGER RAGGIUNTO",
    "trade_active": "TRADE ATTIVO",
    "trade_expired": "TRADE SCADUTO",
    "signal_settled": "RISULTATO",
    "signal_cancelled": "SEGNALE ANNULLATO",
}


def fmt_price(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
    except (TypeError, ValueError):
        return "—"


@dataclass
class Notification:
    title: str
    body: str
    urgent: bool = False


def build_notification(
    event: str, signal: dict[str, Any], synthetic: bool
) -> Notification | None:
    """Turn one lifecycle event into what the user should actually read.

    Returns None for events that carry nothing worth interrupting for.
    """
    if event not in TITLES:
        return None

    direction = signal.get("direction", "NO_TRADE")
    arrow = ARROW.get(direction, "—")
    label = DIRECTION_IT.get(direction, direction)
    trigger = fmt_price(signal.get("trigger_price"))
    confidence = signal.get("confidence")
    conf_txt = f"{float(confidence) * 100:.0f}%" if confidence else "—"

    if event == "signal_created":
        body = (
            f"{arrow} {label}   conf {conf_txt}\n"
            f"Aspetta che BTC tocchi {trigger}\n"
            f"Il countdown parte SOLO al trigger"
        )
        urgent = False
    elif event == "trigger_hit":
        body = (
            f"{arrow} {label}   entry {fmt_price(signal.get('entry_price'))}\n"
            f"{signal.get('horizon_s', 5):.0f} secondi, countdown avviato"
        )
        urgent = True
    elif event == "trade_active":
        body = f"{arrow} {label} attivo, {signal.get('horizon_s', 5):.0f}s"
        urgent = False
    elif event == "trade_expired":
        body = f"{arrow} {label} scaduto, in liquidazione"
        urgent = False
    elif event == "signal_settled":
        result = signal.get("result") or signal.get("status") or "?"
        body = (
            f"{result}   {arrow} {label}\n"
            f"entry {fmt_price(signal.get('entry_price'))} → "
            f"expiry {fmt_price(signal.get('expiry_price'))}"
        )
        urgent = result == "LOSS"
    else:  # signal_cancelled
        body = f"{arrow} {label}: trigger {trigger} non raggiunto"
        urgent = False

    title = TITLES[event]
    if synthetic:
        # Never let simulator output look like a market call.
        title = f"[SIMULATO] {title}"
        body = f"DATI SINTETICI - non è il mercato\n{body}"
    return Notification(title=title, body=body, urgent=urgent)


def should_notify(
    event: str,
    signal: dict[str, Any],
    wanted_events: tuple[str, ...],
    min_confidence: float,
) -> bool:
    if event not in wanted_events:
        return False
    # A settled result is worth seeing whatever the original confidence was.
    if event in ("signal_settled", "signal_cancelled"):
        return True
    confidence = signal.get("confidence") or 0.0
    return float(confidence) >= min_confidence


# ----------------------------------------------------------------- platforms
class DesktopNotifier:
    """Fires native notifications, falling back to the console."""

    def __init__(self, enabled: bool = True, sound: bool = False) -> None:
        self.sound = sound
        self.system = platform.system()
        self.backend = self._pick_backend() if enabled else "console"
        self.failures = 0

    def _pick_backend(self) -> str:
        if self.system == "Darwin" and shutil.which("osascript"):
            return "osascript"
        if self.system == "Linux" and shutil.which("notify-send"):
            return "notify-send"
        if self.system == "Windows" and shutil.which("powershell"):
            return "powershell"
        return "console"

    def send(self, note: Notification) -> None:
        self.console(note)
        if self.backend == "console":
            return
        try:
            getattr(self, f"_send_{self.backend.replace('-', '_')}")(note)
        except Exception as exc:  # noqa: BLE001 - a failed toast must not stop us
            self.failures += 1
            if self.failures <= 3:
                print(f"  (notifica desktop non riuscita: {exc})", file=sys.stderr)
            if self.failures == 3:
                print("  (ulteriori errori di notifica non verranno segnalati)",
                      file=sys.stderr)

    def console(self, note: Notification) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        bell = "\a" if (self.sound and note.urgent) else ""
        first, *rest = note.body.split("\n")
        print(f"{bell}[{stamp}] {note.title}  |  {first}")
        for line in rest:
            print(f"           {line}")
        sys.stdout.flush()

    def _send_osascript(self, note: Notification) -> None:
        body = note.body.replace("\n", " · ").replace('"', "'")
        title = note.title.replace('"', "'")
        script = f'display notification "{body}" with title "{title}"'
        if self.sound and note.urgent:
            script += ' sound name "Submarine"'
        subprocess.run(["osascript", "-e", script], check=True, timeout=5)

    def _send_notify_send(self, note: Notification) -> None:
        subprocess.run(
            [
                "notify-send",
                "--app-name=BTC 5s Quant",
                f"--urgency={'critical' if note.urgent else 'normal'}",
                "--expire-time=8000",
                note.title,
                note.body,
            ],
            check=True,
            timeout=5,
        )

    def _send_powershell(self, note: Notification) -> None:
        body = note.body.replace("\n", " - ").replace("'", "''")
        title = note.title.replace("'", "''")
        # Windows Runtime toast; available on Windows 10 and later.
        script = f"""
[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
$t = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent(
        [Windows.UI.Notifications.ToastTemplateType]::ToastText02)
$texts = $t.GetElementsByTagName('text')
$texts.Item(0).AppendChild($t.CreateTextNode('{title}')) | Out-Null
$texts.Item(1).AppendChild($t.CreateTextNode('{body}')) | Out-Null
[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('BTC 5s Quant').Show(
    [Windows.UI.Notifications.ToastNotification]::new($t))
"""
        subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
            check=True,
            timeout=10,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


# ------------------------------------------------------------------- watcher
class SignalWatcher:
    def __init__(self, args: argparse.Namespace) -> None:
        self.url = args.url.rstrip("/") + "/ws/signals"
        self.events = tuple(e.strip() for e in args.events.split(",") if e.strip())
        self.min_confidence = args.min_confidence
        self.notifier = DesktopNotifier(enabled=not args.no_desktop, sound=args.sound)
        self.synthetic = False
        self.seen: set[str] = set()
        self.connected = False

    async def run(self) -> None:
        import websockets

        delay = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20
                ) as ws:
                    if not self.connected:
                        print(f"connesso a {self.url}")
                    self.connected = True
                    delay = 1.0
                    async for raw in ws:
                        self.handle(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect forever
                if self.connected:
                    print(f"\nconnessione persa ({type(exc).__name__}), riprovo…")
                    self.notifier.send(
                        Notification(
                            "MOTORE NON RAGGIUNGIBILE",
                            "Il notificatore ha perso il backend. "
                            "Nessun segnale finché non torna.",
                            urgent=True,
                        )
                    )
                self.connected = False
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    def handle(self, msg: dict[str, Any]) -> None:
        kind = msg.get("type")
        if kind == "snapshot":
            data = msg.get("data") or {}
            self.synthetic = bool(data.get("is_synthetic"))
            counters = data.get("counters") or {}
            print(
                f"stato motore: {counters.get('signals', 0)} segnali, "
                f"{counters.get('no_trade', 0)} NO TRADE"
                + ("   [DATI SINTETICI]" if self.synthetic else "")
            )
            return
        if kind != "signal":
            return  # heartbeat

        payload = msg.get("data") or {}
        event = payload.get("event")
        signal = payload.get("signal") or {}
        if signal.get("is_synthetic"):
            self.synthetic = True

        # The backend re-broadcasts state; never notify the same step twice.
        key = f"{signal.get('signal_id')}:{event}"
        if key in self.seen:
            return
        self.seen.add(key)
        if len(self.seen) > 5000:
            self.seen = set(list(self.seen)[-2000:])

        if not should_notify(event, signal, self.events, self.min_confidence):
            return
        note = build_notification(event, signal, self.synthetic)
        if note:
            self.notifier.send(note)


BANNER = """
──────────────────────────────────────────────────────────────
  BTC 5-SECOND QUANT ENGINE - notificatore desktop
──────────────────────────────────────────────────────────────
  Mostra le decisioni del motore. Non ne prende nessuna, e non
  legge lo schermo: legge il feed di mercato, che è esatto.

  PAPER TRADING. Nessun ordine viene inviato da nessuna parte.

  NESSUN EDGE È STATO DIMOSTRATO. Finché non fai girare
  `app.ml.cli search` su dati reali e ottieni qualcosa di
  diverso da NO EDGE, questi segnali sono ipotesi non validate.
  Un "82%" qui non significa che vince l'82% delle volte:
  significa che il modello lo afferma. Sono cose diverse.
──────────────────────────────────────────────────────────────
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Notifiche desktop native per il BTC 5-Second Quant Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--url", default="ws://localhost:8000",
                   help="URL WebSocket del backend (default ws://localhost:8000)")
    p.add_argument("--events", default=",".join(DEFAULT_EVENTS),
                   help=f"eventi da notificare. Disponibili: {','.join(ALL_EVENTS)}")
    p.add_argument("--min-confidence", type=float, default=0.0,
                   help="ignora i segnali sotto questa confidence (0-1)")
    p.add_argument("--sound", action="store_true", help="suono sugli eventi urgenti")
    p.add_argument("--no-desktop", action="store_true",
                   help="solo console, nessuna notifica di sistema")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if not 0.0 <= args.min_confidence <= 1.0:
        sys.exit("--min-confidence deve stare fra 0 e 1")
    unknown = set(e.strip() for e in args.events.split(",")) - set(ALL_EVENTS)
    if unknown:
        sys.exit(f"eventi sconosciuti: {', '.join(sorted(unknown))}\n"
                 f"disponibili: {', '.join(ALL_EVENTS)}")

    print(BANNER)
    watcher = SignalWatcher(args)
    print(f"eventi: {', '.join(watcher.events)}")
    print(f"notifiche: {watcher.notifier.backend}"
          + (f" (confidence >= {args.min_confidence:.0%})" if args.min_confidence else ""))
    print("Ctrl-C per uscire.\n")

    try:
        asyncio.run(watcher.run())
    except KeyboardInterrupt:
        print("\nchiuso.")


if __name__ == "__main__":
    main()
