"""Notizie cripto da feed RSS pubblici, con un lessico e nessuna pretesa.

Che cosa fa davvero questo modulo, detto senza abbellimenti: conta parole. Non
capisce le notizie, non sa se un annuncio e' gia' nei prezzi, non distingue un
titolo importante da uno scritto per essere cliccato. Assegna a ogni titolo un
peso in [0, 1] e un verso in base a un lessico compilato a mano.

Perche' allora tenerlo. Perche' la variabile utile non e' "questa notizia e'
positiva": e' **quanto flusso di notizie c'e' rispetto al normale**. Un'ora con
quindici titoli ad alto impatto e' un'ora diversa da una con due titoli di
routine, indipendentemente da cosa dicano, e quella differenza si misura
contando. E' anche il motivo per cui `news_impact_1h` conta piu' di
`news_direction_1h` nel resto del sistema: la prima misura qualcosa di reale, la
seconda e' un'approssimazione grossolana che va trattata come tale.

Le fonti sono le testate cripto piu' seguite. La lista sta in `config.py` e si
puo' cambiare senza toccare questo file.
"""

from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .. import config
from ..util import timeutil
from ..util.http import FetchError, get_text

# Lessico: peso di impatto e verso. I pesi non sono precisi e non devono
# esserlo; servono a separare "notizia grossa" da "notizia di routine".
BULLISH = {
    "surge": 0.6, "rally": 0.6, "soar": 0.7, "jump": 0.5, "gain": 0.4,
    "record high": 0.9, "all-time high": 0.9, "breakout": 0.6,
    "adoption": 0.5, "approval": 0.7, "approved": 0.7, "inflow": 0.6,
    "inflows": 0.6, "accumulate": 0.5, "buy": 0.3, "bullish": 0.6,
    "upgrade": 0.4, "partnership": 0.4, "institutional": 0.5,
    "etf approval": 0.9, "halving": 0.5, "short squeeze": 0.7,
}
BEARISH = {
    "crash": 0.9, "plunge": 0.8, "plummet": 0.8, "tumble": 0.6, "drop": 0.4,
    "fall": 0.4, "slump": 0.6, "selloff": 0.7, "sell-off": 0.7,
    "liquidation": 0.7, "liquidations": 0.7, "hack": 0.9, "exploit": 0.8,
    "breach": 0.7, "outflow": 0.6, "outflows": 0.6, "ban": 0.7,
    "lawsuit": 0.6, "sec sues": 0.8, "bearish": 0.6, "downgrade": 0.4,
    "bankruptcy": 0.9, "fraud": 0.8, "long squeeze": 0.7, "capitulation": 0.8,
}
# Parole che alzano l'impatto senza indicare un verso.
AMPLIFIERS = {
    "bitcoin": 0.3, "btc": 0.3, "ethereum": 0.2, "eth": 0.2,
    "federal reserve": 0.5, "fed": 0.4, "cpi": 0.5, "inflation": 0.4,
    "rate": 0.3, "sec": 0.4, "etf": 0.4, "billion": 0.3, "whale": 0.3,
    "regulation": 0.3, "fomc": 0.5,
}

BULL, BEAR, NEUTRAL = "BULL", "BEAR", "NEUTRAL"


@dataclass
class NewsItem:
    id: str
    ts: int
    source: str
    title: str
    link: str | None
    summary: str | None
    impact: float
    direction: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id, "ts": self.ts, "source": self.source,
            "title": self.title, "link": self.link, "summary": self.summary,
            "impact": self.impact, "direction": self.direction,
        }


def classify(title: str, summary: str = "") -> tuple[float, str]:
    """Peso e verso di un titolo. Somma di corrispondenze, saturata a 1."""
    text = f"{title} {summary}".lower()
    bull = sum(w for phrase, w in BULLISH.items() if phrase in text)
    bear = sum(w for phrase, w in BEARISH.items() if phrase in text)
    amp = sum(w for phrase, w in AMPLIFIERS.items() if phrase in text)

    impact = min(1.0, (bull + bear) * 0.4 + amp * 0.3)
    if bull > bear * 1.3:
        direction = BULL
    elif bear > bull * 1.3:
        direction = BEAR
    else:
        direction = NEUTRAL
    return (round(impact, 3), direction)


_DATE_FORMATS = (
    "%a, %d %b %Y %H:%M:%S %z",
    "%a, %d %b %Y %H:%M:%S %Z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
)


def _parse_date(text: str | None) -> int | None:
    if not text:
        return None
    text = text.strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    try:
        return timeutil.from_iso(text)
    except (ValueError, TypeError):
        return None


def _strip_html(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"<[^>]+>", " ", text).replace("&nbsp;", " ").strip()[:400]


def parse_feed(xml_text: str, source: str) -> list[NewsItem]:
    """RSS e Atom, entrambi, con `xml.etree` della libreria standard."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    items: list[NewsItem] = []
    # RSS 2.0
    for node in root.iter("item"):
        title = (node.findtext("title") or "").strip()
        if not title:
            continue
        link = (node.findtext("link") or "").strip() or None
        summary = _strip_html(node.findtext("description"))
        ts = (_parse_date(node.findtext("pubDate"))
              or _parse_date(node.findtext("{http://purl.org/dc/elements/1.1/}date"))
              or timeutil.now_ms())
        impact, direction = classify(title, summary)
        items.append(NewsItem(
            id=hashlib.sha1(f"{link or ''}{title}".encode()).hexdigest()[:20],
            ts=ts, source=source, title=title[:300], link=link,
            summary=summary or None, impact=impact, direction=direction))

    # Atom
    ns = "{http://www.w3.org/2005/Atom}"
    for node in root.iter(f"{ns}entry"):
        title = (node.findtext(f"{ns}title") or "").strip()
        if not title:
            continue
        link_node = node.find(f"{ns}link")
        link = link_node.get("href") if link_node is not None else None
        summary = _strip_html(node.findtext(f"{ns}summary")
                              or node.findtext(f"{ns}content"))
        ts = (_parse_date(node.findtext(f"{ns}updated"))
              or _parse_date(node.findtext(f"{ns}published"))
              or timeutil.now_ms())
        impact, direction = classify(title, summary)
        items.append(NewsItem(
            id=hashlib.sha1(f"{link or ''}{title}".encode()).hexdigest()[:20],
            ts=ts, source=source, title=title[:300], link=link,
            summary=summary or None, impact=impact, direction=direction))
    return items


def fetch_all(feeds: Iterable[tuple[str, str]] | None = None
              ) -> tuple[list[NewsItem], list[dict[str, Any]]]:
    """Scarica tutti i feed. Un feed che non risponde non ferma gli altri."""
    feeds = feeds or config.NEWS_FEEDS
    items: list[NewsItem] = []
    errors: list[dict[str, Any]] = []
    for source, url in feeds:
        try:
            items.extend(parse_feed(get_text(url, timeout=15.0, retries=2),
                                    source))
        except FetchError as exc:
            errors.append({"source": source, **exc.to_dict()})
        except Exception as exc:                                # noqa: BLE001
            errors.append({"source": source, "kind": "PARSE",
                           "detail": f"{type(exc).__name__}: {exc}"})
    return items, errors


def refresh_news(store: Any) -> dict[str, Any]:
    """Scarica e salva. Restituisce un riassunto onesto, errori compresi."""
    items, errors = fetch_all()
    cutoff = timeutil.now_ms() - 7 * timeutil.DAY_MS
    fresh = [i for i in items if i.ts >= cutoff]
    written = store.upsert_news(i.to_dict() for i in fresh) if fresh else 0
    return {
        "fetched": len(items), "stored": written,
        "sources_ok": len(config.NEWS_FEEDS) - len(errors),
        "errors": errors,
    }


def summarise(store: Any, hours: int | None = None) -> dict[str, Any]:
    """Il flusso di notizie delle ultime ore, come lo mostra la dashboard."""
    hours = hours or config.NEWS_LOOKBACK_HOURS
    since = timeutil.now_ms() - hours * timeutil.HOUR_MS
    rows = store.news(since_ms=since, limit=100)
    if not rows:
        return {"available": False, "hours": hours,
                "note": ("Nessuna notizia in archivio. Se il collector gira, "
                         "controllare che i feed RSS siano raggiungibili.")}
    weighted = sum((r["impact"] or 0.0) *
                   {"BULL": 1.0, "BEAR": -1.0}.get(r["direction"] or "", 0.0)
                   for r in rows)
    total = sum(r["impact"] or 0.0 for r in rows)
    return {
        "available": True,
        "hours": hours,
        "count": len(rows),
        "total_impact": round(total, 2),
        "direction_score": round(weighted / total, 3) if total > 0 else 0.0,
        "high_impact": [
            {"at": timeutil.iso(r["ts"]), "source": r["source"],
             "title": r["title"], "link": r["link"],
             "impact": r["impact"], "direction": r["direction"]}
            for r in sorted(rows, key=lambda r: -(r["impact"] or 0.0))[:5]
        ],
        "note": ("Il punteggio viene da un lessico, non da una comprensione "
                 "del testo. La quantita' di notizie e' un dato affidabile, "
                 "il loro verso e' un'indicazione grossolana."),
    }
