"""Prove offline, deterministiche, senza rete.

Cinque gruppi, in ordine di importanza decrescente:

1. **Causalita'.** La prova piu' importante del progetto: si costruisce il
   frame su N barre, si registra la riga all'indice i, poi si ricostruisce su
   N+K barre e si confronta la stessa riga. Se anche un solo valore cambia,
   quella feature guardava il futuro. Nessun ragionamento sul codice sostituisce
   questo confronto.

2. **Onesta' statistica.** Su un cammino casuale il motore non deve trovare
   niente; su un vantaggio piantato deve trovarlo. Servono entrambe: un
   validatore che boccia sempre e' inutile quanto uno che promuove sempre, e
   solo la coppia distingue i due casi.

3. **Sicurezza.** Si controlla il codice sorgente per verificare che non esista
   una sola chiamata capace di piazzare un ordine.

4. **Correttezza numerica.** Indicatori e metriche contro valori calcolati a
   mano.

5. **Integrazione.** Archivio, API HTTP, parser dei formati esterni.

`--quick` salta il gruppo 2, che e' lento.
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import tempfile
import threading
import time
import urllib.request
from typing import Any, Callable

PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Results:
    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose
        self.passed = 0
        self.failed = 0
        self.failures: list[tuple[str, str]] = []

    def check(self, name: str, condition: bool, detail: str = "") -> bool:
        if condition:
            self.passed += 1
            if self.verbose:
                print(f"  OK   {name}")
        else:
            self.failed += 1
            self.failures.append((name, detail))
            print(f"  FAIL {name}" + (f"\n         {detail}" if detail else ""))
        return condition

    def section(self, title: str) -> None:
        if self.verbose:
            print(f"\n{title}")
            print("-" * len(title))


# --------------------------------------------------------------------------
# 1. Causalita'
# --------------------------------------------------------------------------
def test_causality(r: Results) -> None:
    """La riga all'istante t non deve cambiare quando arrivano barre nuove."""
    from ..data.store import Store
    from ..features import builder
    from . import synthetic

    r.section("1. CAUSALITA' — nessuna feature guarda il futuro")

    klines = synthetic.random_walk_klines(n=1500, seed=42)
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "causal.db")

        store_a = Store(db)
        store_a.upsert_bars("BTCUSDT", klines[:1000])
        for sym in ("ETHUSDT", "SOLUSDT"):
            store_a.upsert_bars(sym, synthetic.random_walk_klines(
                n=1000, seed=43, start_ms=klines[0].start_ms))
        frame_a = builder.build_frame(store_a, "BTCUSDT", with_live=False)
        # Tre indici distanti: inizio informato, meta', e l'ultimo disponibile.
        indices = [400, 700, 999]
        rows_a = {i: frame_a.row(i) for i in indices}
        regimes_a = {i: frame_a.regime_at(i).name for i in indices}
        store_a.close()

        store_b = Store(db)
        store_b.upsert_bars("BTCUSDT", klines)
        for sym in ("ETHUSDT", "SOLUSDT"):
            store_b.upsert_bars(sym, synthetic.random_walk_klines(
                n=1500, seed=43, start_ms=klines[0].start_ms))
        frame_b = builder.build_frame(store_b, "BTCUSDT", with_live=False)
        rows_b = {i: frame_b.row(i) for i in indices}
        regimes_b = {i: frame_b.regime_at(i).name for i in indices}
        store_b.close()

    drift: list[str] = []
    for i in indices:
        a, b = rows_a[i], rows_b[i]
        for name in sorted(set(a) | set(b)):
            va, vb = a.get(name), b.get(name)
            if va is None and vb is None:
                continue
            if va is None or vb is None:
                drift.append(f"i={i} {name}: {va} -> {vb}")
                continue
            if abs(va - vb) > max(1e-9, abs(va) * 1e-9):
                drift.append(f"i={i} {name}: {va!r} -> {vb!r}")

    r.check("le feature non cambiano quando arrivano barre future",
            not drift,
            "; ".join(drift[:6]) + (f" (+{len(drift) - 6})" if len(drift) > 6 else ""))
    r.check("il regime non cambia retroattivamente",
            all(regimes_a[i] == regimes_b[i] for i in indices),
            f"{regimes_a} vs {regimes_b}")

    # Il futuro si legge in un posto solo, e quel posto e' `labels.py`.
    offenders: list[str] = []
    for root, _dirs, files in os.walk(PACKAGE_DIR):
        if "tests" in root or "__pycache__" in root:
            continue
        for fname in files:
            if not fname.endswith(".py"):
                continue
            path = os.path.join(root, fname)
            rel = os.path.relpath(path, PACKAGE_DIR)
            if rel in ("features/labels.py",):
                continue
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
            if "outcome_from_bars" in text and rel not in (
                    "features/builder.py", "forecast/engine.py"):
                offenders.append(rel)
    r.check("solo builder e forecast leggono gli esiti futuri",
            not offenders, f"anche: {offenders}")


# --------------------------------------------------------------------------
# 2. Onesta' statistica
# --------------------------------------------------------------------------
def test_statistical_honesty(r: Results) -> None:
    from ..data.store import Store
    from ..features import builder
    from ..model import logistic
    from ..research import validation
    from . import synthetic

    r.section("2. ONESTA' STATISTICA — il caso non deve sembrare un vantaggio")

    def evaluate(seed: int, horizon_edge: bool) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(os.path.join(tmp, f"s{seed}.db"))
            synthetic.seed_store(store, n=30000, seed=seed,
                                 horizon_edge=horizon_edge)
            ds = builder.build_dataset(builder.build_frame(store, "BTCUSDT"))
            walk = validation.walk_forward(
                ds,
                lambda tr: logistic.fit(tr, epochs=40, max_rows=2500),
                lambda mo, te: mo.predict(te), folds=5)
            store.close()
            return {"walk": walk, "verdict": validation.judge(walk)}

    nulls = [evaluate(s, False) for s in (7, 101, 202)]
    false_positives = [i for i, res in enumerate(nulls) if res["verdict"].passed]
    r.check("nessun vantaggio promosso su cammini casuali",
            not false_positives,
            f"promossi su {len(nulls)} cammini: {false_positives}")

    aucs = [(res["walk"].get("overall") or {}).get("auc_directional")
            for res in nulls]
    clean = [a for a in aucs if a is not None]
    centred = all(0.40 <= a <= 0.62 for a in clean) if clean else False
    r.check("l'AUC direzionale sul caso resta vicina a 0.5",
            centred, f"valori: {clean}")

    planted = evaluate(23, True)
    r.check("un vantaggio piantato viene trovato",
            planted["verdict"].passed,
            "motivi: " + "; ".join(planted["verdict"].reasons[:3]))

    # La purga deve togliere righe davvero: se ne toglie zero, non sta purgando.
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(os.path.join(tmp, "purge.db"))
        synthetic.seed_store(store, n=12000, seed=5)
        ds = builder.build_dataset(builder.build_frame(store, "BTCUSDT"))
        splits = validation.make_folds(ds)
        purged = [s.purged_rows for s in splits]
        r.check("la purga scarta righe fra addestramento e test",
                bool(splits) and all(p > 0 for p in purged),
                f"righe purgate per fold: {purged}")
        if splits:
            gap_ok = all(
                s.test.ts[0] - s.train_to_ms >= 0 for s in splits)
            r.check("il test comincia dopo la fine dell'addestramento", gap_ok)
        store.close()


def _code_lines(source: str) -> list[tuple[int, str]]:
    """Le righe di codice vero: via commenti, docstring e letterali di testo.

    Si usa `tokenize` invece di euristiche sulle virgolette perche' le
    euristiche sbagliano proprio dove serve precisione — una docstring su piu'
    righe che nomina un endpoint vietato per dire che non lo si usa.
    """
    import io
    import tokenize

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        # Se il file non si tokenizza, meglio scansionarlo tutto che saltarlo.
        return list(enumerate(source.splitlines(), start=1))

    lines: dict[int, list[str]] = {}
    for tok in tokens:
        if tok.type in (tokenize.COMMENT, tokenize.STRING, tokenize.NL,
                        tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT):
            continue
        lines.setdefault(tok.start[0], []).append(tok.string)
    return [(no, " ".join(parts)) for no, parts in sorted(lines.items())]


# --------------------------------------------------------------------------
# 3. Sicurezza
# --------------------------------------------------------------------------
def test_safety(r: Results) -> None:
    from .. import config

    r.section("3. SICUREZZA — nessuna esecuzione di ordini")

    r.check("l'interruttore di esecuzione e' spento",
            config.EXECUTION_ENABLED is False)

    # Endpoint e concetti che questo pacchetto non deve mai toccare.
    forbidden = [
        (r"/v5/order", "endpoint ordini di Bybit"),
        (r"/v5/position", "endpoint posizioni"),
        (r"/v5/account", "endpoint conto"),
        (r"/v5/asset", "endpoint trasferimenti"),
        (r"X-BAPI-SIGN", "firma di richieste autenticate"),
        (r"X-BAPI-API-KEY", "chiave API"),
        (r"\bhmac\b", "firma HMAC"),
        (r"api_secret|apiSecret|API_SECRET", "segreto API"),
        (r"place_order|placeOrder|create_order|submit_order", "invio ordine"),
        (r"cancel_order|cancelOrder|close_position", "gestione ordine"),
        (r"set_leverage|setLeverage", "impostazione leva"),
        (r"stop_loss|stopLoss|take_profit", "gestione stop"),
    ]
    hits: list[str] = []
    for root, _dirs, files in os.walk(PACKAGE_DIR):
        if "__pycache__" in root:
            continue
        for fname in files:
            if not fname.endswith(".py"):
                continue
            rel = os.path.relpath(os.path.join(root, fname), PACKAGE_DIR)
            # Questo file elenca le stringhe proibite per poterle cercare: e'
            # l'unica eccezione, e va dichiarata invece che nascosta.
            if rel == os.path.join("tests", "selftest.py"):
                continue
            with open(os.path.join(root, fname), encoding="utf-8") as fh:
                source = fh.read()
            # Si scansiona il CODICE, non i commenti. Le docstring di questo
            # progetto nominano di proposito gli endpoint che non usa, per
            # spiegare perche' non li usa: cercare nel testo grezzo
            # segnalerebbe quelle spiegazioni e renderebbe la prova inutile
            # (o, peggio, spingerebbe a togliere le spiegazioni).
            for line_no, code in _code_lines(source):
                for pattern, label in forbidden:
                    if re.search(pattern, code, re.IGNORECASE):
                        hits.append(f"{rel}:{line_no} [{label}] {code.strip()[:70]}")
    r.check("nessuna chiamata di esecuzione nel codice eseguibile",
            not hits, "\n         ".join(hits[:8]))

    # Il client HTTP non deve avere un modo di fare POST.
    from ..util import http
    r.check("il client HTTP non espone metodi di scrittura",
            not any(hasattr(http, n) for n in ("post", "put", "delete", "patch")))

    # Il server web rifiuta i metodi di scrittura.
    from ..web.server import Handler
    r.check("il server risponde 405 ai metodi di scrittura",
            Handler.do_POST is Handler.do_PUT is Handler.do_DELETE)


# --------------------------------------------------------------------------
# 4. Correttezza numerica
# --------------------------------------------------------------------------
def test_numerics(r: Results) -> None:
    from ..features import indicators, rolling
    from ..research import metrics
    from ..util import numeric

    r.section("4. NUMERI — indicatori e metriche contro valori noti")

    # EMA su una costante e' la costante.
    r.check("EMA di una serie costante e' la costante",
            abs((indicators.ema([5.0] * 30, 10) or 0) - 5.0) < 1e-9)

    # RSI di una salita monotona e' 100; di una discesa monotona e' 0.
    rising = [float(i) for i in range(1, 40)]
    falling = list(reversed(rising))
    r.check("RSI di una salita monotona e' 100",
            abs((indicators.rsi(rising) or 0) - 100.0) < 1e-6)
    # Attenzione: `rsi(...) or 99.0` darebbe 99.0 quando il risultato e' 0.0,
    # perche' zero e' falso in Python. Serve un confronto esplicito con None.
    rsi_down = indicators.rsi(falling)
    r.check("RSI di una discesa monotona e' 0",
            rsi_down is not None and abs(rsi_down) < 1e-6, f"trovato {rsi_down}")

    # La serie e la funzione puntuale devono coincidere sull'ultimo indice.
    series = rolling.rsi_series(rising, 14)
    r.check("rsi_series coincide con rsi sull'ultimo indice",
            series[-1] is not None
            and abs(series[-1] - (indicators.rsi(rising) or 0)) < 1e-9)

    ema_s = rolling.ema_series(rising, 10)
    r.check("ema_series coincide con ema sull'ultimo indice",
            ema_s[-1] is not None
            and abs(ema_s[-1] - (indicators.ema(rising, 10) or 0)) < 1e-9)

    # Allineamento: nessuno scivolamento in avanti.
    values = [float(i) for i in range(50)]
    means = rolling.rolling_mean(values, 10)
    expected = sum(values[40:50]) / 10
    r.check("rolling_mean non scivola in avanti",
            means[49] is not None and abs(means[49] - expected) < 1e-9,
            f"atteso {expected}, trovato {means[49]}")

    # Il percentile esclude il valore corrente dalla propria storia.
    pctl = rolling.rolling_percentile([1.0] * 20 + [99.0], 20)
    r.check("rolling_percentile esclude il valore corrente",
            pctl[-1] is not None and pctl[-1] > 0.95,
            f"trovato {pctl[-1]}")

    # align_step_series non deve mai prendere un valore futuro.
    aligned = rolling.align_step_series([10, 20, 30, 40],
                                        [15, 35], [1.0, 2.0])
    r.check("align_step_series guarda solo all'indietro",
            aligned == [None, 1.0, 1.0, 2.0], f"trovato {aligned}")

    # AUC: separazione perfetta = 1.0, inversa = 0.0, casuale = 0.5.
    r.check("AUC di una separazione perfetta e' 1",
            abs((metrics.auc([1, 2, 3, 4], [0, 0, 1, 1]) or 0) - 1.0) < 1e-9)
    auc_inv = metrics.auc([4, 3, 2, 1], [0, 0, 1, 1])
    r.check("AUC di una separazione invertita e' 0",
            auc_inv is not None and abs(auc_inv) < 1e-9, f"trovato {auc_inv}")
    r.check("AUC con punteggi identici e' 0.5",
            abs((metrics.auc([1, 1, 1, 1], [0, 0, 1, 1]) or 0) - 0.5) < 1e-9)

    # Brier e Wilson.
    brier_perfect = metrics.brier([1.0, 0.0], [1, 0])
    r.check("Brier di una previsione perfetta e' 0",
            brier_perfect is not None and abs(brier_perfect) < 1e-12,
            f"trovato {brier_perfect}")
    lo, hi = numeric.wilson_interval(50, 100)
    r.check("l'intervallo di Wilson contiene la stima puntuale",
            lo < 0.5 < hi and hi - lo < 0.25, f"[{lo}, {hi}]")
    lo2, hi2 = numeric.wilson_interval(5, 10)
    r.check("l'intervallo si allarga con pochi campioni",
            (hi2 - lo2) > (hi - lo))

    # Benjamini-Hochberg: un p piccolissimo sopravvive, molti grandi no.
    survives, adjusted = numeric.benjamini_hochberg(
        [0.0001] + [0.6] * 19, alpha=0.10)
    r.check("BH tiene il segnale forte e scarta il rumore",
            survives[0] and not any(survives[1:]),
            f"corretti: {[round(a, 3) for a in adjusted[:3]]}")
    # Venti p-value a 0.04 con soglia 0.05 sopravvivono, e non e' un difetto:
    # il p corretto vale 0.04 * 20 / 20 = 0.04. BH controlla la QUOTA di falsi
    # fra i dichiarati, non li elimina tutti. La prova giusta e' che gli stessi
    # p spariscano quando la soglia si stringe.
    survives2, adj2 = numeric.benjamini_hochberg([0.04] * 20, alpha=0.05)
    r.check("BH tiene p marginali coerenti con la soglia",
            all(survives2) and abs(adj2[0] - 0.04) < 1e-9,
            f"corretto {adj2[0]}")
    survives3, _ = numeric.benjamini_hochberg([0.04] * 20, alpha=0.01)
    r.check("...e li scarta tutti con una soglia piu' severa",
            not any(survives3))
    # Un solo p marginale in mezzo a tanti grandi viene penalizzato.
    survives4, adj4 = numeric.benjamini_hochberg([0.03] + [0.9] * 19, alpha=0.05)
    r.check("BH penalizza un p marginale fra molti test",
            not survives4[0] and adj4[0] > 0.5,
            f"corretto {adj4[0]}")

    # Accuratezza bilanciata: chi dice sempre la stessa classe prende 1/3.
    actual = ["LONG"] * 30 + ["SHORT"] * 30 + ["FLAT"] * 40
    always_flat = ["FLAT"] * 100
    bal = metrics.balanced_accuracy(always_flat, actual)
    r.check("chi dice sempre FLAT ha accuratezza bilanciata 1/3",
            bal is not None and abs(bal - 1 / 3) < 1e-9, f"trovato {bal}")
    acc = metrics.accuracy(always_flat, actual)
    r.check("...ma accuratezza semplice 0.40, ed e' l'inganno",
            acc is not None and abs(acc - 0.40) < 1e-9, f"trovato {acc}")

    # L'accuratezza direzionale non deve contare i FLAT come errori.
    pred = ["LONG"] * 10
    act = ["LONG"] * 5 + ["FLAT"] * 4 + ["SHORT"] * 1
    d = metrics.directional_accuracy(pred, act)
    # `accuracy` e' arrotondata a quattro cifre: la tolleranza deve tenerne conto.
    r.check("l'accuratezza direzionale ignora gli esiti FLAT",
            d["n"] == 6 and abs(d["accuracy"] - 5 / 6) < 1e-3,
            f"n={d['n']} acc={d['accuracy']}")
    r.check("...e riporta a parte la quota di nulla di fatto",
            abs(d["flat_rate"] - 0.4) < 1e-9, f"flat_rate={d['flat_rate']}")

    # Calibrazione: una previsione perfettamente calibrata ha ECE nullo.
    probs = [0.7] * 100
    outcomes = [1] * 70 + [0] * 30
    cal = metrics.reliability(probs, outcomes)
    r.check("ECE di una previsione calibrata e' ~0",
            cal.get("ece") is not None and cal["ece"] < 0.02,
            f"ece={cal.get('ece')}")
    bad = metrics.reliability([0.9] * 100, [1] * 40 + [0] * 60)
    r.check("ECE di una previsione troppo sicura e' grande",
            bad.get("ece") is not None and bad["ece"] > 0.4,
            f"ece={bad.get('ece')}")


# --------------------------------------------------------------------------
# 5. Etichette e campionamento
# --------------------------------------------------------------------------
def test_labels(r: Results) -> None:
    from ..features.indicators import Bar
    from ..features.labels import band_bps, class_distribution, outcome_from_bars
    from ..research.validation import independent_indices

    r.section("5. ETICHETTE — la definizione di 'aver ragione'")

    def bar(ts: int, close: float, high: float, low: float) -> Bar:
        return Bar(ts, close, high, low, close, 1.0, close)

    future = [bar(i * 60000, 100.0 + i, 100.0 + i + 0.5, 100.0 + i - 0.5)
              for i in range(1, 31)]
    out = outcome_from_bars(future, 100.0, 30, band_bps(10.0))
    r.check("una salita netta produce LONG", out.label == "LONG",
            f"trovato {out.label} con {out.return_bps} bps")
    r.check("il MFE e' almeno pari al rendimento finale",
            out.mfe_bps is not None and out.return_bps is not None
            and out.mfe_bps >= out.return_bps - 1e-9)
    r.check("il tempo al MFE e' dentro l'orizzonte",
            out.time_to_mfe_min is not None and 0 < out.time_to_mfe_min <= 30)

    flat = [bar(i * 60000, 100.0, 100.02, 99.98) for i in range(1, 31)]
    out_flat = outcome_from_bars(flat, 100.0, 30, band_bps(50.0))
    r.check("un mercato fermo produce FLAT", out_flat.label == "FLAT",
            f"trovato {out_flat.label}")

    r.check("una finestra troncata e' dichiarata incompleta",
            not outcome_from_bars(future[:10], 100.0, 30, 10.0).complete)

    r.check("la banda ha un pavimento",
            abs(band_bps(0.0) - 10.0) < 1e-9)
    r.check("la banda cresce con la volatilita'",
            band_bps(200.0) > band_bps(20.0))

    dist = class_distribution(["LONG"] * 3 + ["FLAT"] * 5 + ["SHORT"] * 2)
    r.check("la base rate e' la classe piu' frequente",
            dist["majority"] == "FLAT" and abs(dist["base_rate"] - 0.5) < 1e-9)

    # Campionamento indipendente: un punto ogni orizzonte, non uno al minuto.
    ts = [i * 60_000 for i in range(300)]
    idx = independent_indices(ts, 30 * 60_000)
    r.check("il campionamento indipendente distanzia di un orizzonte",
            len(idx) == 10, f"trovati {len(idx)} su 300 righe")
    r.check("gli indici indipendenti sono distanziati davvero",
            all(ts[idx[i + 1]] - ts[idx[i]] >= 30 * 60_000
                for i in range(len(idx) - 1)))


# --------------------------------------------------------------------------
# 6. Archivio
# --------------------------------------------------------------------------
def test_store(r: Results) -> None:
    from ..data.bybit import FundingPoint, Kline, OpenInterestPoint
    from ..data.store import Store

    r.section("6. ARCHIVIO — persistenza e idempotenza")

    with tempfile.TemporaryDirectory() as tmp:
        store = Store(os.path.join(tmp, "t.db"))
        bars = [Kline(i * 60_000, 100, 101, 99, 100.5, 1.0, 100.0)
                for i in range(100)]
        n1 = store.upsert_bars("BTCUSDT", bars)
        n2 = store.upsert_bars("BTCUSDT", bars)
        lo, hi, count = store.bar_span("BTCUSDT")
        r.check("un backfill ripetuto non duplica le barre",
                n1 == n2 == 100 and count == 100, f"conteggio {count}")
        r.check("l'intervallo temporale e' corretto",
                lo == 0 and hi == 99 * 60_000)

        store.upsert_open_interest("BTCUSDT", "5min",
                                   [OpenInterestPoint(0, 1000.0)])
        store.upsert_funding("BTCUSDT", [FundingPoint(0, 0.0001)])
        r.check("open interest e funding si rileggono",
                len(store.open_interest("BTCUSDT")) == 1
                and len(store.funding("BTCUSDT")) == 1)

        # Il CVD e' cumulativo: due minuti consecutivi devono sommarsi.
        store.add_flow("BTCUSDT", 60_000, taker_buy=10, taker_sell=4,
                       buy_notional=1000, sell_notional=400, trades=5)
        store.add_flow("BTCUSDT", 120_000, taker_buy=2, taker_sell=8,
                       buy_notional=200, sell_notional=800, trades=4)
        flow = store.flow("BTCUSDT")
        r.check("il CVD si accumula fra minuti consecutivi",
                len(flow) == 2 and abs(flow[0]["cvd"] - 6.0) < 1e-9
                and abs(flow[1]["cvd"] - 0.0) < 1e-9,
                f"cvd: {[f['cvd'] for f in flow]}")

        # Una previsione si scrive prima e si giudica dopo.
        store.add_forecast({
            "forecast_id": "f1", "ts": 0, "symbol": "BTCUSDT",
            "horizon_min": 30, "verdict": "LONG", "p_long": 0.7,
            "p_short": 0.2, "p_flat": 0.1, "price": 100.0,
        })
        pending = store.pending_forecasts(30 * 60_000)
        r.check("una previsione scaduta risulta da giudicare",
                len(pending) == 1)
        store.score_forecast("f1", outcome_ts=30 * 60_000, outcome_price=101.0,
                             outcome_return_bps=100.0, outcome_label="LONG",
                             outcome_mfe_bps=120.0, outcome_mae_bps=-10.0,
                             outcome_correct=1, time_to_mfe_min=12.0)
        r.check("dopo il giudizio non risulta piu' da giudicare",
                len(store.pending_forecasts(30 * 60_000)) == 0)
        rows = store.forecasts(scored_only=True)
        r.check("l'esito e' stato scritto",
                len(rows) == 1 and rows[0]["outcome_correct"] == 1)
        store.close()


# --------------------------------------------------------------------------
# 7. Parser esterni
# --------------------------------------------------------------------------
def test_parsers(r: Results) -> None:
    from ..data.bybit import BybitPublic, _ticker_from
    from ..news import feeds

    r.section("7. PARSER — i formati esterni, su risposte finte")

    # Kline di Bybit: liste di stringhe, dalla piu' recente alla piu' vecchia.
    raw = {
        "retCode": 0,
        "result": {"list": [
            ["120000", "101", "102", "100", "101.5", "10", "1015"],
            ["60000", "100", "101", "99", "100.5", "12", "1206"],
        ]},
    }
    client = BybitPublic()
    original = client._call
    client._call = lambda path, params: raw["result"]              # type: ignore
    bars = client.klines("BTCUSDT", "1", closed_only=False)
    client._call = original                                        # type: ignore
    r.check("le kline tornano in ordine cronologico crescente",
            len(bars) == 2 and bars[0].start_ms == 60_000
            and bars[1].start_ms == 120_000)
    r.check("i campi delle kline sono convertiti",
            bars[0].open == 100.0 and bars[0].close == 100.5
            and bars[0].volume == 12.0)

    ticker = _ticker_from({
        "lastPrice": "60000.5", "markPrice": "60001", "indexPrice": "60000",
        "openInterest": "70000", "fundingRate": "0.0001",
        "bid1Price": "60000", "ask1Price": "60001", "volume24h": "1000",
    }, "BTCUSDT")
    r.check("il ticker converte i numeri",
            ticker.last_price == 60000.5 and ticker.funding_rate == 0.0001)
    r.check("la base mark/index e' in bps",
            ticker.basis_bps is not None
            and abs(ticker.basis_bps - (1 / 60000 * 10000)) < 1e-6)

    from ..data.bybit import OrderBook
    book = OrderBook(0, [(100.0, 3.0), (99.0, 1.0)], [(101.0, 1.0), (102.0, 1.0)])
    # bid 3+1 = 4, ask 1+1 = 2  ->  (4-2)/(4+2) = 1/3.
    r.check("lo squilibrio del libro e' in [-1, 1] e ha il segno giusto",
            book.imbalance(2) is not None
            and abs(book.imbalance(2) - 1 / 3) < 1e-9,
            f"trovato {book.imbalance(2)}")
    r.check("il mid del libro e' la media dei due lati",
            book.mid == 100.5)

    # RSS con date e voci reali.
    rss = """<?xml version="1.0"?><rss version="2.0"><channel>
      <item><title>Bitcoin surges to record high</title>
        <link>https://example.com/a</link>
        <pubDate>Mon, 01 Jan 2024 10:00:00 +0000</pubDate>
        <description>BTC rally continues</description></item>
      <item><title>Exchange hack drains funds</title>
        <link>https://example.com/b</link>
        <pubDate>Mon, 01 Jan 2024 11:00:00 +0000</pubDate></item>
    </channel></rss>"""
    items = feeds.parse_feed(rss, "Test")
    r.check("il parser RSS legge due voci", len(items) == 2)
    r.check("le date RSS diventano millisecondi UTC",
            items[0].ts == 1704103200000, f"trovato {items[0].ts}")
    r.check("il lessico riconosce il verso rialzista",
            items[0].direction == "BULL", f"trovato {items[0].direction}")
    r.check("il lessico riconosce il verso ribassista",
            items[1].direction == "BEAR", f"trovato {items[1].direction}")
    r.check("l'impatto e' in [0, 1]",
            all(0.0 <= i.impact <= 1.0 for i in items))

    atom = """<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
      <entry><title>Ethereum upgrade approved</title>
        <link href="https://example.com/c"/>
        <updated>2024-01-01T12:00:00Z</updated></entry></feed>"""
    r.check("il parser legge anche Atom",
            len(feeds.parse_feed(atom, "Test")) == 1)


# --------------------------------------------------------------------------
# 8. Il cancello della decisione
# --------------------------------------------------------------------------
def test_decision_gate(r: Results) -> None:
    from ..data.store import Store
    from ..forecast import quality
    from ..forecast.engine import ForecastEngine
    from . import synthetic

    r.section("8. IL CANCELLO — meglio WAIT che una previsione falsa")

    q = quality.assess(data_age_seconds=None, feature_coverage=0.3,
                       regime_confidence=0.1, model_available=False,
                       model_holdout_ok=False, spread_bps=None,
                       bars_available=10, edge_support=False)
    r.check("senza dati la qualita' e' insufficiente",
            q.score < 0.4 and q.label == "INSUFFICIENTE", f"score {q.score}")
    r.check("...e ogni penalizzazione e' spiegata", len(q.warnings) >= 3)

    q2 = quality.assess(data_age_seconds=5, feature_coverage=0.95,
                        regime_confidence=0.8, model_available=True,
                        model_holdout_ok=True, spread_bps=0.6,
                        bars_available=2000, edge_support=True)
    r.check("con tutto a posto la qualita' e' alta",
            q2.score >= 0.8, f"score {q2.score}")

    # Una componente rotta deve pesare: e' il senso del minimo pesato.
    q3 = quality.assess(data_age_seconds=5, feature_coverage=0.95,
                        regime_confidence=0.8, model_available=False,
                        model_holdout_ok=False, spread_bps=0.6,
                        bars_available=2000, edge_support=True)
    r.check("un modello assente abbassa la qualita' anche se il resto e' ottimo",
            q3.score < q2.score - 0.2, f"{q3.score} vs {q2.score}")

    # Senza modello addestrato, il verdetto deve essere WAIT.
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(os.path.join(tmp, "gate.db"))
        synthetic.seed_store(store, n=2000, seed=9)
        forecast = ForecastEngine(store).forecast(persist=False)
        r.check("senza modello promosso il verdetto e' WAIT",
                forecast.verdict == "WAIT", f"trovato {forecast.verdict}")
        r.check("...e viene detto perche'", len(forecast.blockers) >= 1,
                str(forecast.blockers))
        total = forecast.p_long + forecast.p_short + forecast.p_flat
        r.check("le probabilita' sommano a uno", abs(total - 1.0) < 1e-6,
                f"somma {total}")
        r.check("su WAIT non si dichiara un target",
                forecast.target_low is None and forecast.invalidation is None
                and forecast.expected_duration_min is None)

        # Un archivio vuoto non deve far crollare il motore.
        empty = Store(os.path.join(tmp, "empty.db"))
        f2 = ForecastEngine(empty).forecast(persist=False)
        r.check("con archivio vuoto risponde WAIT invece di rompersi",
                f2.verdict == "WAIT" and bool(f2.blockers))
        empty.close()
        store.close()


# --------------------------------------------------------------------------
# 9. Ciclo di vita degli edge
# --------------------------------------------------------------------------
def test_lifecycle(r: Results) -> None:
    from ..research import lifecycle
    from ..util import timeutil

    r.section("9. CICLO DI VITA — entrare costa, restare costa ancora")

    now = timeutil.now_ms()
    record = lifecycle.EdgeRecord(
        edge_id="x", family="test", label="prova", definition={},
        state=lifecycle.DISCOVERED, direction="LONG", created_ts=now,
        updated_ts=now, state_ts=now)
    record.transition(lifecycle.VALIDATING, "supera le soglie")
    record.transition(lifecycle.SHADOW, "holdout conferma")
    r.check("le transizioni sono registrate con il motivo",
            len(record.history) == 2
            and record.history[0]["reason"] == "supera le soglie")
    r.check("lo stato corrente e' l'ultimo", record.state == lifecycle.SHADOW)
    record.transition(lifecycle.SHADOW, "nessun cambiamento")
    r.check("una transizione verso lo stesso stato non sporca la storia",
            len(record.history) == 2)

    # In ombra non si promuove per fretta.
    few = [{"ts": now + i * 60_000, "label": "LONG", "correct": 1}
           for i in range(10)]
    v = lifecycle.shadow_verdict(few)
    r.check("dieci osservazioni in ombra non bastano", not v["ready"])

    # Nemmeno con tante osservazioni schiacciate in poche ore.
    burst = [{"ts": now + i * 60_000, "label": "LONG", "correct": 1}
             for i in range(80)]
    r.check("ottanta osservazioni in un'ora non bastano",
            not lifecycle.shadow_verdict(burst)["ready"])

    spread = [{"ts": now + i * 30 * 60_000,
               "label": "LONG" if i % 3 else "SHORT",
               "correct": 1 if i % 4 else 0} for i in range(80)]
    v2 = lifecycle.shadow_verdict(spread)
    r.check("ottanta osservazioni su piu' giorni bastano", v2["ready"],
            v2.get("reason", ""))

    # Il decadimento si misura contro se' stessi.
    decayed = [{"ts": now + i * 60_000, "label": "LONG",
                "correct": 1 if i < 20 else 0} for i in range(80)]
    d = lifecycle.decay_verdict(decayed, 0.65)
    r.check("un edge che smette di funzionare viene dichiarato in decadimento",
            d["decaying"], d.get("reason", ""))
    steady = [{"ts": now + i * 60_000, "label": "LONG",
               "correct": 1 if i % 3 else 0} for i in range(80)]
    r.check("un edge stabile non viene dichiarato in decadimento",
            not lifecycle.decay_verdict(steady, 0.65)["decaying"])


# --------------------------------------------------------------------------
# 10. API HTTP
# --------------------------------------------------------------------------
def test_http(r: Results) -> None:
    from ..data.store import Store
    from ..web.server import build_server
    from . import synthetic

    r.section("10. API HTTP — le rotte rispondono, e non accettano comandi")

    with tempfile.TemporaryDirectory() as tmp:
        store = Store(os.path.join(tmp, "web.db"))
        synthetic.seed_store(store, n=2000, seed=3)
        server, _state = build_server("127.0.0.1", 0, store=store)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        time.sleep(0.3)
        base = f"http://127.0.0.1:{port}"

        try:
            for path, keys in (
                ("/healthz", ("ok",)),
                ("/api/status", ("symbol", "database", "safety")),
                ("/api/forecast", ("verdict", "probabilities", "panels")),
                ("/api/research", ("headline", "counts", "coverage")),
                ("/api/history", ("count", "items")),
                ("/api/edges", ("count", "edges")),
                ("/api/news", ()),
            ):
                try:
                    with urllib.request.urlopen(base + path, timeout=25) as resp:
                        body = json.loads(resp.read())
                        ok = resp.status == 200 and all(k in body for k in keys)
                        r.check(f"GET {path} risponde 200 con i campi attesi",
                                ok, f"chiavi trovate: {list(body)[:6]}")
                except Exception as exc:                        # noqa: BLE001
                    r.check(f"GET {path} risponde", False,
                            f"{type(exc).__name__}: {exc}")

            with urllib.request.urlopen(base + "/", timeout=15) as resp:
                html = resp.read().decode()
            r.check("la dashboard e' servita",
                    resp.status == 200 and "AURUM EDGE DISCOVERY" in html)
            r.check("la dashboard mostra il simbolo e il verdetto",
                    'id="verdict"' in html and 'id="symbol"' in html)

            # Una POST deve essere rifiutata.
            req = urllib.request.Request(base + "/api/forecast", data=b"{}",
                                         method="POST")
            code = None
            try:
                urllib.request.urlopen(req, timeout=10)
            except urllib.error.HTTPError as exc:
                code = exc.code
            r.check("una POST viene rifiutata con 405", code == 405,
                    f"codice {code}")

            with urllib.request.urlopen(base + "/api/status", timeout=15) as resp:
                status = json.loads(resp.read())
            r.check("lo stato dichiara che l'esecuzione e' disattivata",
                    status["safety"]["execution_enabled"] is False)
        finally:
            server.shutdown()
            server.server_close()
            store.close()


# --------------------------------------------------------------------------
# 11. Il catalogo degli edge
# --------------------------------------------------------------------------
def test_edge_catalogue(r: Results) -> None:
    from ..features.dataset import Dataset
    from ..research import edges as E

    r.section("11. CATALOGO — generazione e adattamento dei pattern")

    names = ["vol_burst", "ret_15m", "oi_accel", "funding_z", "compression"]
    rules = E.candidate_rules(names)
    r.check("il catalogo genera candidati", len(rules) > 20, f"{len(rules)}")
    r.check("ogni candidato ha un identificatore stabile",
            len({r_.edge_id for r_ in rules}) == len(rules))
    r.check("le combinazioni non uniscono due feature della stessa famiglia",
            all(len({E.FEATURE_FAMILY[c.feature] for c in r_.conditions}) ==
                len(r_.conditions) for r_ in rules))

    cat = E.describe_catalogue(names)
    families = {u["family"] for u in cat["unavailable"]}
    r.check("le liquidazioni sono dichiarate non disponibili",
            "liquidation" in families)
    r.check("il flusso ordini assente e' dichiarato", "order_flow" in families)

    # Un edge deve adattarsi sul training e attivarsi coerentemente.
    rows = [[float(i % 10), float(i % 7), 0.0, 0.0, 1.0] for i in range(400)]
    labels = ["LONG" if row[0] >= 8 else "FLAT" for row in rows]
    ds = Dataset(names=names, rows=rows, ts=[i * 60_000 for i in range(400)],
                 labels=labels, prices=[100.0] * 400)
    rule = E.EdgeRule((E.Condition("vol_burst", "high", 0.80),),
                      "volume_breakout")
    fitted = E.fit_edge(rule, ds)
    r.check("l'edge si adatta e trova una soglia", fitted is not None)
    if fitted:
        r.check("la soglia viene dal quantile del training",
                fitted.thresholds[0] >= 7.0, f"{fitted.thresholds}")
        r.check("la probabilita' empirica riflette i dati",
                fitted.p_long > fitted.p_short, str(fitted.probabilities()))
        r.check("la direzione la decidono i dati, non la regola",
                fitted.direction == "LONG", fitted.direction)
        mask = E.activation_mask(fitted, ds)
        r.check("l'edge si attiva su una minoranza di righe",
                0 < sum(mask) < len(mask) * 0.5,
                f"{sum(mask)} su {len(mask)}")

    rare = E.EdgeRule((E.Condition("vol_burst", "high", 0.999),),
                      "volume_breakout")
    r.check("un edge che si attiva troppo di rado non viene adattato",
            E.fit_edge(rare, ds, min_activations=200) is None)


# --------------------------------------------------------------------------
# 12. Regressioni: i due guasti silenziosi trovati costruendo il sistema
# --------------------------------------------------------------------------
def test_regressions(r: Results) -> None:
    from ..features.dataset import Dataset
    from ..model import logistic
    from ..research import metrics
    from ..research import validation

    r.section("12. REGRESSIONI — i guasti silenziosi gia' visti una volta")

    # --- A. Un modello a cui mancano variabili non deve rispondere lo stesso.
    #
    # Trovato per caso: togliendo dal costruttore due colonne duplicate, un
    # modello addestrato prima continuava a produrre probabilita' diverse dalle
    # sue, senza errori, perche' i valori mancanti venivano riempiti con la
    # mediana. Nessuna eccezione, nessun avviso, numeri leggermente falsi.
    names = ["a", "b", "c", "d"]
    rows = [[float(i % 5), float(i % 3), float(i % 7), 1.0] for i in range(400)]
    labels = ["LONG" if row[0] > 2 else "SHORT" if row[1] > 1 else "FLAT"
              for row in rows]
    ds = Dataset(names=names, rows=rows, ts=[i * 60_000 for i in range(400)],
                 labels=labels, prices=[100.0] * 400)
    model = logistic.fit(ds, epochs=15, max_rows=400, top_k=4)

    r.check("il modello sa quali variabili ha davvero pesato",
            set(model.selected_names) <= set(names) and model.selected_names)
    r.check("con tutte le variabili presenti non manca niente",
            model.missing_features(names) == [])
    missing = model.missing_features(["a", "b"])
    r.check("una variabile sparita viene rilevata",
            bool(missing) and set(missing) <= {"c", "d"}, str(missing))

    # --- B. Un predittore che si astiene va misurato contro il periodo intero.
    #
    # Un edge dichiara sempre la stessa direzione. Se la base rate si calcola
    # sulle sole righe in cui si e' attivato, l'accuratezza coincide con la
    # base rate per costruzione e il vantaggio e' zero qualunque cosa faccia:
    # nessun edge potrebbe MAI essere promosso, e il motore sembrerebbe solo
    # molto severo.
    actual = ["LONG"] * 8 + ["SHORT"] * 2          # sottoinsieme attivato
    predicted = ["LONG"] * 10
    whole = ["LONG"] * 50 + ["SHORT"] * 50         # periodo intero, 50/50
    probs = [{"LONG": 0.8, "SHORT": 0.1, "FLAT": 0.1}] * 10

    naive = metrics.score(probs, predicted, actual)
    fair = metrics.score(probs, predicted, actual, baseline_labels=whole)
    r.check("senza baseline il confronto e' tautologico",
            abs((naive.directional.get("accuracy") or 0)
                - (naive.notes.get("directional_base_rate") or 0)) < 1e-9,
            f"acc={naive.directional.get('accuracy')} "
            f"base={naive.notes.get('directional_base_rate')}")
    r.check("con la baseline del periodo il vantaggio si vede",
            abs((fair.notes.get("directional_base_rate") or 0) - 0.5) < 1e-9
            and (fair.directional.get("accuracy") or 0) > 0.79,
            f"acc={fair.directional.get('accuracy')} "
            f"base={fair.notes.get('directional_base_rate')}")

    # --- C. I cancelli impossibili per un edge devono essere spenti.
    walk_model = {
        "status": "COMPLETO", "abstaining": False,
        "fold_consistency": 1.0, "p_value_vs_base": 0.001,
        "overall": {
            "balanced_accuracy": 1 / 3, "auc_directional": 0.50,
            "directional_base_rate": 0.50,
            "directional_base_rate_on_calls": 0.80,
            "directional": {"n": 300, "accuracy": 0.80, "ci95": [0.75, 0.85],
                            "long_calls": 300, "short_calls": 0,
                            "flat_rate": 0.1},
            "calibration": {"ece": 0.02},
        },
    }
    walk_edge = dict(walk_model, abstaining=True)
    v_model = validation.judge(walk_model)
    v_edge = validation.judge(walk_edge)
    r.check("gli stessi numeri bocciano un modello",
            not v_model.passed, "avrebbe dovuto fallire")
    r.check("...e promuovono un edge, perche' li' quei cancelli sono murati",
            v_edge.passed, "; ".join(v_edge.reasons[:3]))
    r.check("un edge senza vantaggio reale resta bocciato",
            not validation.judge(dict(
                walk_edge,
                overall=dict(walk_edge["overall"],
                             directional={"n": 300, "accuracy": 0.50,
                                          "ci95": [0.44, 0.56],
                                          "long_calls": 300, "short_calls": 0,
                                          "flat_rate": 0.1}))).passed)


# --------------------------------------------------------------------------
# 13. Il collector, contro un exchange finto
# --------------------------------------------------------------------------
class _FakeBybit:
    """Un Bybit finto che risponde come quello vero, offline.

    Serve perche' il collector e' l'unico pezzo che non si puo' provare contro
    l'API reale in un ambiente senza rete, ed e' anche quello con la logica piu'
    facile da sbagliare in silenzio: la deduplica degli scambi. `recent-trade`
    restituisce sempre l'ultimo migliaio, quindi fra un giro e il successivo la
    sovrapposizione e' quasi totale. Contarla due volte gonfia il CVD di un
    fattore pari al numero di giri — e un CVD gonfiato non sembra un errore,
    sembra un segnale fortissimo.
    """

    def __init__(self) -> None:
        from ..data.bybit import OrderBook, Ticker, Trade
        from ..util import timeutil

        self._Trade = Trade
        now = timeutil.floor_ms(timeutil.now_ms(), 60_000)
        self.base_ts = now
        self.calls: dict[str, int] = {}
        self._tick = 0

        def ticker(sym: str, price: float) -> Ticker:
            return Ticker(symbol=sym, ts_ms=now, last_price=price,
                          mark_price=price * 1.0001, index_price=price,
                          bid1=price - 0.5, ask1=price + 0.5,
                          bid1_size=2.0, ask1_size=1.0,
                          volume_24h=1000.0, turnover_24h=price * 1000,
                          open_interest=50_000.0,
                          open_interest_value=price * 50_000,
                          funding_rate=0.0001,
                          next_funding_ms=now + 3_600_000,
                          price_24h_pcnt=0.01 if sym != "XRPUSDT" else -0.02,
                          high_24h=price * 1.02, low_24h=price * 0.98)

        self._tickers = {
            "BTCUSDT": ticker("BTCUSDT", 60_000.0),
            "ETHUSDT": ticker("ETHUSDT", 3_000.0),
            "SOLUSDT": ticker("SOLUSDT", 150.0),
            "XRPUSDT": ticker("XRPUSDT", 0.5),
            "BNBUSDT": ticker("BNBUSDT", 500.0),
        }
        self._book = OrderBook(
            now, [(59_999.5, 3.0), (59_999.0, 2.0)],
            [(60_000.5, 1.0), (60_001.0, 1.0)])

    def _count(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1

    def tickers(self, symbols=None):
        self._count("tickers")
        if symbols is None:
            return dict(self._tickers)
        return {s: t for s, t in self._tickers.items() if s in set(symbols)}

    def orderbook(self, symbol, limit=200):
        self._count("orderbook")
        return self._book

    def recent_trades(self, symbol, limit=1000):
        """Ogni giro aggiunge due scambi nuovi e ripete tutti i precedenti."""
        self._count("recent_trades")
        self._tick += 1
        out = []
        for k in range(self._tick):
            out.append(self._Trade(self.base_ts + k * 1000, 60_000.0, 1.0, "Buy"))
            out.append(self._Trade(self.base_ts + k * 1000 + 500, 60_000.0,
                                   0.4, "Sell"))
        return out

    def klines(self, symbol, interval="1", **kw):
        from ..data.bybit import Kline
        self._count("klines")
        return [Kline(self.base_ts - (60 - i) * 60_000, 60_000.0, 60_010.0,
                      59_990.0, 60_005.0, 5.0, 300_000.0) for i in range(60)]

    def open_interest(self, symbol, interval="5min", **kw):
        from ..data.bybit import OpenInterestPoint
        self._count("open_interest")
        return [OpenInterestPoint(self.base_ts - i * 300_000, 50_000.0 + i)
                for i in range(10)]

    def account_ratio(self, symbol, period="5min", limit=50):
        from ..data.bybit import AccountRatioPoint
        self._count("account_ratio")
        return [AccountRatioPoint(self.base_ts, 0.55, 0.45)]


def test_collector(r: Results) -> None:
    from ..data.collector import Collector
    from ..data.store import Store

    r.section("13. COLLECTOR — contro un exchange finto, senza rete")

    with tempfile.TemporaryDirectory() as tmp:
        store = Store(os.path.join(tmp, "collect.db"))
        fake = _FakeBybit()
        collector = Collector(store=store, client=fake)

        first = collector.poll_once()
        r.check("il primo giro registra tutti gli scambi visti",
                first["trades_new"] == 2, str(first.get("trades_new")))

        snapshot = store.latest_snapshot("BTCUSDT")
        r.check("la fotografia live viene scritta", snapshot is not None)
        if snapshot:
            r.check("prezzo, spread e squilibrio del libro sono presenti",
                    snapshot["last_price"] == 60_000.0
                    and snapshot["spread_bps"] is not None
                    and snapshot["book_imbalance"] is not None,
                    f"spread={snapshot['spread_bps']} "
                    f"imb={snapshot['book_imbalance']}")
            r.check("lo squilibrio ha il segno del lato piu' pesante",
                    snapshot["book_imbalance"] > 0,
                    str(snapshot["book_imbalance"]))
            payload = json.loads(snapshot["payload"] or "{}")
            breadth = payload.get("breadth") or {}
            r.check("la market breadth viene calcolata sul paniere",
                    breadth.get("available") and breadth.get("symbols", 0) >= 4,
                    str(breadth))
            r.check("le liquidazioni sono dichiarate non disponibili",
                    (payload.get("liquidations") or {}).get("available") is False)

        flow_before = store.flow("BTCUSDT")
        cvd_before = flow_before[-1]["cvd"] if flow_before else None

        # Il secondo giro ripete gli scambi del primo e ne aggiunge due.
        second = collector.poll_once()
        r.check("il secondo giro conta solo gli scambi nuovi",
                second["trades_new"] == 2,
                f"trovati {second['trades_new']} (la deduplica non ha retto)")

        flow_after = store.flow("BTCUSDT")
        cvd_after = flow_after[-1]["cvd"] if flow_after else None
        # Ogni giro aggiunge +1.0 comprato e -0.4 venduto: delta atteso +0.6.
        r.check("il CVD cresce del delta vero, non del doppio",
                cvd_before is not None and cvd_after is not None
                and abs((cvd_after - cvd_before) - 0.6) < 1e-6,
                f"{cvd_before} -> {cvd_after}")

        totals = sum(f["taker_buy"] for f in flow_after)
        r.check("il volume taker accumulato corrisponde agli scambi unici",
                abs(totals - 2.0) < 1e-6, f"totale {totals}")

        health = collector.health.to_dict()
        r.check("il collector riporta due giri riusciti e nessun errore",
                health["polls"] == 2 and health["failures"] == 0, str(health))
        r.check("...e si dichiara fresco", health["fresh"] is True)
        r.check("un solo giro basta per tutti i simboli del contesto",
                fake.calls.get("tickers") == 2,
                f"chiamate ticker: {fake.calls.get('tickers')}")
        store.close()


# --------------------------------------------------------------------------
# Esecuzione
# --------------------------------------------------------------------------
ALL_TESTS: list[tuple[str, Callable[[Results], None], bool]] = [
    ("causalita", test_causality, False),
    ("onesta_statistica", test_statistical_honesty, True),
    ("sicurezza", test_safety, False),
    ("numeri", test_numerics, False),
    ("etichette", test_labels, False),
    ("archivio", test_store, False),
    ("parser", test_parsers, False),
    ("cancello", test_decision_gate, False),
    ("ciclo_vita", test_lifecycle, False),
    ("http", test_http, False),
    ("catalogo", test_edge_catalogue, False),
    ("regressioni", test_regressions, False),
    ("collector", test_collector, False),
]


def run_all(verbose: bool = True, quick: bool = False) -> int:
    r = Results(verbose)
    began = time.time()
    print("=" * 72)
    print("AURUM EDGE DISCOVERY — prove offline")
    print("=" * 72)
    if quick:
        print("modalita' rapida: le prove statistiche lente sono saltate")

    for name, fn, slow in ALL_TESTS:
        if quick and slow:
            print(f"\n(saltato: {name})")
            continue
        try:
            fn(r)
        except Exception as exc:                                # noqa: BLE001
            import traceback
            r.check(f"gruppo {name} completato", False,
                    f"{type(exc).__name__}: {exc}\n" +
                    traceback.format_exc(limit=4))

    elapsed = time.time() - began
    print()
    print("=" * 72)
    print(f"RISULTATO: {r.passed} superate, {r.failed} fallite "
          f"({elapsed:.1f}s)")
    if r.failures:
        print()
        for name, detail in r.failures:
            print(f"  FALLITA  {name}")
            if detail:
                print(f"           {detail}")
    print("=" * 72)
    return 0 if r.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run_all())
