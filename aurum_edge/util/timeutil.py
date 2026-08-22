"""Tempo. Tutto in millisecondi UTC, senza eccezioni.

Un progetto che mescola secondi e millisecondi, o UTC e ora locale, produce
errori che non fanno crashare niente: producono etichette spostate di un'ora e
accuratezze inspiegabili. Qui l'unita' e' una sola: `int` di millisecondi
epoch, UTC.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

MINUTE_MS = 60_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000


def now_ms() -> int:
    return int(time.time() * 1000)


def floor_ms(ts_ms: int, bucket_ms: int) -> int:
    """Inizio del bucket che contiene `ts_ms`."""
    if bucket_ms <= 0:
        return ts_ms
    return (ts_ms // bucket_ms) * bucket_ms


def iso(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(
        timespec="seconds")


def from_iso(text: str) -> int:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def hour_of_day(ts_ms: int) -> int:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).hour


def day_of_week(ts_ms: int) -> int:
    """0 = lunedi."""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).weekday()


def session_of(ts_ms: int) -> str:
    """La sessione dominante, in UTC.

    Le cripto non chiudono, ma il volume ha comunque un orologio: la
    sovrapposizione Londra-New York e' un regime diverso dalla notte asiatica,
    e mescolarli in un unico campione nasconde entrambi.
    """
    h = hour_of_day(ts_ms)
    if 12 <= h < 17:
        return "OVERLAP"       # Londra + New York
    if 7 <= h < 12:
        return "LONDON"
    if 17 <= h < 22:
        return "NEWYORK"
    return "ASIA"


def humanize_minutes(minutes: float | None) -> str:
    if minutes is None:
        return "n/d"
    if minutes < 1:
        return "<1 min"
    if minutes < 60:
        return f"{int(round(minutes))} min"
    hours = minutes / 60.0
    return f"{hours:.1f} h"
