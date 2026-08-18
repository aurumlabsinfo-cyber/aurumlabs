"""Verifica del sistema su se stesso. Nessuna rete, nessun servizio esterno.

Un principio guida la scelta dei casi: si prova cio' che **puo' rompersi in
silenzio**. Un errore che fa esplodere il programma si nota; uno che sposta di
mezzo punto percentuale una probabilita', o che lascia entrare un dato dal
futuro, produce numeri credibili e sbagliati per settimane.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from ..config import Config
from ..core.agents import CALL, PUT, build_agents
from ..core.buffers import CandleAggregator, TimeSeries
from ..core.decision import MetaDecisionEngine, NO_TRADE
from ..core.engine import AurumEngine
from ..core.features import FeatureEngine
from ..core.regime import ReliabilityTracker
from ..core.signals import (CANCELLED, CONFIRMED, DRAW, ENTERED, EXPIRED, FROZEN,
                            LOSS, PRE_SIGNAL, SignalEngine, WIN)
from ..core.wallet import VirtualWallet
from ..market.fallback import DataQualityAgent, MarketFeed
from ..market.replay import ReplayAdapter, VirtualClock
from ..market.simulation import SimulationAdapter
from ..ml.calibration import (calibration_verdict, fit_isotonic, fit_platt,
                              reliability_curve, wilson_interval)
from ..ml.model import PureLogistic, fit_estimator, predict_proba
from ..notification.telegram import TelegramNotifier
from ..research.booster import SignalBooster
from ..research.engine import ResearchEngine, SetupLibrary
from ..research.validation import (Dataset, WalkForwardValidator,
                                   benjamini_hochberg, binomial_p_value,
                                   independent_indices, leakage_check)
from ..storage.database import Database

BASE_TS = 1_700_000_000_000


def _sim_config(**over) -> Config:
    values = dict(database_path=":memory:", http_port=0, research_enabled=False,
                  news_enabled=False, telegram_enabled=False)
    values.update(over)
    return Config(**values)


def _feed_features(cfg: Config, steps: int = 6000, seed: int = 7,
                   step_ms: int = 100) -> tuple[FeatureEngine, int]:
    sim = SimulationAdapter(cfg, seed=seed)
    fe = FeatureEngine(cfg)
    ts = BASE_TS
    for i in range(steps):
        ts = BASE_TS + i * step_ms
        fe.observe(sim.step(step_ms / 1000.0, ts=ts))
    return fe, ts


# --------------------------------------------------------------------------- #
#  CASI
# --------------------------------------------------------------------------- #

def t_config() -> str:
    """La configurazione impossibile deve essere rifiutata all'avvio."""
    cfg = _sim_config()
    assert abs(cfg.breakeven_win_rate - 1 / 1.8) < 1e-9, cfg.breakeven_win_rate
    assert cfg.effective_min_probability > cfg.breakeven_win_rate

    bad_cases = [
        ({"payout": -1}, "payout"),
        ({"signal_freeze_seconds": 90}, "freeze"),
        ({"virtual_stake": 900}, "stake"),
        ({"ml_embargo_seconds": 5}, "embargo"),
    ]
    for over, label in bad_cases:
        try:
            _sim_config(**over)
        except ValueError:
            continue
        raise AssertionError(f"configurazione non valida accettata: {label}")

    # Il payout DEVE guidare la soglia: e' il legame che rende la confidenza
    # un numero economico e non un'opinione.
    strict = _sim_config(payout=0.5)
    assert strict.breakeven_win_rate > cfg.breakeven_win_rate
    return (f"pareggio {cfg.breakeven_win_rate:.2%} da payout {cfg.payout:.0%}, "
            f"4 configurazioni impossibili rifiutate")


def t_database() -> str:
    """Scritture accodate, migrazione, e un guasto che non travolge il resto."""
    path = os.path.join(tempfile.mkdtemp(), "t.db")
    db = Database(path)
    db.start()
    db.add("market_ticks", {"ts": 1, "received_ts": 1, "source": "t",
                            "mode": "SIMULATION", "bid": 1.08, "ask": 1.0801,
                            "mid": 1.08005, "last": 1.08005, "spread": 0.0001,
                            "spread_bps": 0.9, "latency_ms": 12.0})
    db.upsert("signals", {"signal_id": "s1", "created_ts": 1, "entry_ts": 2,
                          "expiry_ts": 62, "direction": CALL, "state": PRE_SIGNAL})
    db.upsert("signals", {"signal_id": "s1", "created_ts": 1, "entry_ts": 2,
                          "expiry_ts": 62, "direction": CALL, "state": EXPIRED,
                          "result": WIN})
    db.flush()
    rows = db.query("SELECT * FROM signals")
    assert len(rows) == 1 and rows[0]["result"] == WIN, "l'upsert ha duplicato"
    counts = db.counts()
    assert counts["market_ticks"] == 1, counts

    # Un guasto su una tabella non deve far perdere le righe delle altre.
    db.add("market_ticks", {"ts": 2, "received_ts": 2, "source": "t",
                            "mode": "SIMULATION", "mid": 1.081, "bid": None,
                            "ask": None, "last": None, "spread": None,
                            "spread_bps": None, "latency_ms": None})
    db._upsert.put_nowait(("signals", {"signal_id": "s2", "colonna_inesistente": 1}))
    db.flush()
    assert db.counts()["market_ticks"] == 2, "una tabella rotta ha travolto le altre"
    assert not db.healthy and db.last_error, "il guasto non e' stato dichiarato"
    db.stop()
    os.unlink(path)
    return "upsert, batch, e un guasto isolato che non perde le altre tabelle"


def t_schema_coerente() -> str:
    """Ogni tabella dichiarata deve avere colonne vere e qualcuno che ci scrive.

    Due difetti reali che questa verifica intercetta.

    **Colonne inventate.** `BATCH_COLUMNS` elenca i nomi usati negli INSERT: se
    uno non esiste nella tabella, l'errore compare solo a runtime, e per come
    e' fatto il writer si traduce in righe perse in silenzio.

    **Tabelle sempre vuote.** Una tabella dichiarata e mai scritta e' peggio di
    una tabella assente: promette una capacita' che non esiste, e chi legge il
    database mesi dopo non ha modo di distinguere "non e' successo niente" da
    "non e' mai stato collegato".
    """
    import re
    from ..storage.schema import BATCH_COLUMNS, SCHEMA, UPSERT_KEYS

    path = os.path.join(tempfile.mkdtemp(), "schema.db")
    db = Database(path)
    conn = db.reader()
    declared = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", SCHEMA))

    # a) le colonne dichiarate esistono davvero.
    for table, cols in BATCH_COLUMNS.items():
        assert table in declared, f"{table} in BATCH_COLUMNS ma non nello schema"
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        unknown = [c for c in cols if c not in have]
        assert not unknown, f"{table}: colonne inesistenti {unknown}"
    for table in UPSERT_KEYS:
        assert table in declared, f"{table} in UPSERT_KEYS ma non nello schema"

    # b) ogni tabella ha almeno un punto di scrittura nel codice.
    root = Path(__file__).resolve().parent.parent
    sources = "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(root.rglob("*.py"))
        if "schema.py" not in p.name and "selftest.py" not in p.name)
    orfane = [t for t in sorted(declared)
              if f'"{t}"' not in sources and f"'{t}'" not in sources
              and t not in ("meta",)]     # `meta` la scrive il Database stesso
    assert not orfane, (
        f"tabelle dichiarate e mai scritte: {orfane}. Una tabella vuota per "
        "sempre promette una capacita' che non c'e'.")

    db.stop()
    os.unlink(path)
    return (f"{len(declared)} tabelle: colonne verificate contro lo schema "
            f"reale, nessuna orfana")


def t_buffers() -> str:
    """Le finestre devono essere CAUSALI: mai un valore dal futuro."""
    ts = TimeSeries(300_000)
    for i in range(600):
        ts.append(BASE_TS + i * 500, 1.08500 + i * 0.00001)
    # `value_at_or_before` non deve mai restituire un campione successivo.
    for probe in (BASE_TS, BASE_TS + 60_000, BASE_TS + 299_000):
        v = ts.value_at_or_before(probe)
        idx = min(int((probe - BASE_TS) / 500), 599)
        expected = 1.08500 + max(0, idx) * 0.00001
        assert v is not None and abs(v - expected) < 1e-9, (probe, v, expected)

    r60 = ts.return_bps(60_000)
    assert r60 is not None and r60 > 0, r60
    agg = CandleAggregator((5, 60))
    for i in range(130):
        agg.add(BASE_TS + i * 1000, 1.0850 + i * 0.00001, 1.08495, 1.08505)
    bars = agg.recent(60)
    assert len(bars) == 3, len(bars)
    # La prima e l'ultima barra sono parziali: il flusso non comincia su un
    # minuto tondo e non finisce su uno. Solo quella di mezzo e' completa, e
    # l'invariante vera e' che nessun tick venga perso o contato due volte.
    assert bars[1]["tick_count"] == 60, bars[1]
    assert sum(b["tick_count"] for b in bars) == 130, bars
    assert bars[0]["high"] >= bars[0]["low"], "massimo sotto il minimo"
    return f"causalita' verificata su 3 istanti, {len(bars)} barre da 1m coerenti"


def t_features() -> str:
    """Il vettore causale su EUR/USD, e cosa NON deve contenere."""
    cfg = _sim_config()
    fe, ts = _feed_features(cfg)
    vec = fe.compute(ts, "SIMULATION")
    assert vec is not None
    v = vec.values
    # Il livello di prezzo non deve entrare nel modello: imparerebbe la
    # settimana, non il mercato.
    assert "mid" not in vec.predictive(), "il prezzo assoluto e' entrato fra le feature"
    assert "hour" not in vec.predictive(), "l'ora assoluta e' entrata fra le feature"
    for k in ("return_5s_bps", "return_60s_bps", "sigma_horizon_bps",
              "realized_vol_5s_bps", "tick_imbalance_5s", "trend_strength",
              "entropy_60s", "session_overlap", "noise_floor_bps"):
        assert v.get(k) is not None, f"feature mancante: {k}"
    # La scala deve essere quella di EUR/USD, non di una criptovaluta.
    sigma = v["sigma_horizon_bps"]
    assert 0.5 < sigma < 25.0, (
        f"sigma a 60s = {sigma:.1f}bps: fuori scala per EUR/USD (attesa 2-6). "
        "Un simulatore alla scala sbagliata non prova niente sui cancelli.")
    assert 0.9 < v["mid"] < 1.3, f"prezzo {v['mid']} non e' EUR/USD"
    return (f"{len(vec.predictive())} feature predittive, sigma 60s "
            f"{sigma:.2f}bps, prezzo e ora esclusi dal modello")


def t_data_quality() -> str:
    """Blocca cio' che e' rotto, segnala il resto senza vietarlo."""
    cfg = _sim_config()
    dq = DataQualityAgent(cfg)
    sim = SimulationAdapter(cfg, seed=3)
    for i in range(200):
        dq.observe(sim.step(0.1, ts=BASE_TS + i * 100))
    rep = dq.assess(now=BASE_TS + 200 * 100)
    assert rep.usable and not rep.blocking, rep.to_dict()

    # Feed fermo: e' un guasto e deve bloccare.
    stale = dq.assess(now=BASE_TS + 200 * 100 + 60_000)
    assert "DATA_STALE" in stale.blocking, stale.to_dict()
    assert stale.stale and not stale.usable

    # Feed congelato: stesso prezzo per centinaia di aggiornamenti.
    dq2 = DataQualityAgent(cfg)
    q = sim.step(0.1, ts=BASE_TS)
    for i in range(260):
        frozen = type(q)(ts=BASE_TS + i * 100, received_ts=BASE_TS + i * 100,
                         source="t", mode="SIMULATION", mid=1.0850,
                         bid=1.08495, ask=1.08505, last=1.0850)
        dq2.observe(frozen)
    rep2 = dq2.assess(now=BASE_TS + 260 * 100)
    assert "FEED_FROZEN" in rep2.blocking, rep2.to_dict()
    return "feed sano usabile; fermo e congelato bloccati con il motivo"


def t_agents() -> str:
    """Ogni agente rispetta il contratto, e chi non ha dati si astiene."""
    cfg = _sim_config()
    fe, ts = _feed_features(cfg)
    vec = fe.compute(ts, "SIMULATION")
    agents = build_agents(cfg)
    seen = set()
    for a in agents:
        op = a.evaluate(vec, "TREND", reliability=1.0)
        seen.add(op.agent)
        assert -1.0 <= op.direction <= 1.0, (op.agent, op.direction)
        assert 0.0 <= op.confidence <= 1.0, (op.agent, op.confidence)
        assert op.latency_ms >= 0.0
        assert op.reason, f"{op.agent} non ha spiegato la sua opinione"
    assert len(seen) == len(agents), "due agenti con lo stesso nome"

    # Senza microstruttura, l'agente relativo deve ASTENERSI, non inventare.
    from ..core.features import FeatureVector
    empty = FeatureVector(ts=ts, mode="SIMULATION",
                          values={"return_5s_bps": 1.0, "return_15s_bps": 1.0})
    micro = next(a for a in agents if a.name == "microstructure")
    op = micro.evaluate(empty, "TREND")
    assert op.abstained and op.confidence == 0.0, op.to_dict()

    # Un agente che esplode non deve fermare il motore.
    class Broken(type(agents[0])):
        name = "broken"
        def _evaluate(self, f, regime):
            raise RuntimeError("guasto voluto")
    op = Broken(cfg).evaluate(vec, "TREND")
    assert op.abstained and "guasto voluto" in op.reason
    return f"{len(agents)} agenti nel contratto, astensione e guasto gestiti"


def t_reliability() -> str:
    """L'affidabilita' e' PER REGIME, e il prior impedisce di crederci subito."""
    rt = ReliabilityTracker()
    # Tre vittorie non devono rendere un agente dominante.
    for _ in range(3):
        rt.record("momentum", "TREND", True)
    r_after_3 = rt.reliability("momentum", "TREND")
    assert r_after_3 < 1.15, f"prior troppo debole: {r_after_3}"
    # Con molte osservazioni il peso si muove davvero.
    for _ in range(200):
        rt.record("momentum", "TREND", True)
    r_after_200 = rt.reliability("momentum", "TREND")
    assert r_after_200 > r_after_3 + 0.2, (r_after_3, r_after_200)
    assert r_after_200 <= rt.MAX_RELIABILITY

    # Lo stesso agente puo' essere bravo in un regime e pessimo in un altro.
    for _ in range(200):
        rt.record("momentum", "RANGE", False)
    assert rt.reliability("momentum", "RANGE") < 0.8, "il regime non separa"
    assert rt.reliability("momentum", "TREND") > 1.2
    return (f"prior: 3 vittorie -> {r_after_3:.2f}, 200 -> {r_after_200:.2f}; "
            f"TREND {rt.reliability('momentum','TREND'):.2f} vs "
            f"RANGE {rt.reliability('momentum','RANGE'):.2f}")


def t_decision() -> str:
    """La confidenza non puo' superare cio' che i dati autorizzano."""
    cfg = _sim_config()
    fe, ts = _feed_features(cfg, steps=9000)
    eng = MetaDecisionEngine(cfg)
    ctx = {"quality_score": 1.0, "warmed_up": True, "open_signals": 0,
           "wallet_can_trade": True, "blocking": []}
    confs, emitted, total = [], 0, 0
    for i in range(3000, 9000, 25):
        v = fe.compute(BASE_TS + i * 100, "SIMULATION")
        if v is None:
            continue
        d = eng.decide(v, ctx)
        confs.append(d.confidence)
        total += 1
        emitted += 1 if d.is_tradeable else 0
    assert confs, "nessuna decisione prodotta"
    top = max(confs)
    # Il punto centrale: un insieme di euristiche NON CALIBRATE non deve poter
    # dichiarare 85%. Se puo', il sistema mente con l'aria di misurare.
    assert top <= 0.72, (
        f"confidenza massima {top:.1%} senza calibrazione misurata: "
        "una euristica non ha titolo per dichiararla")
    # E deve comunque restare legata al payout.
    for d_conf in confs:
        assert d_conf >= 0.5

    # Cancelli duri: un feed rotto blocca, qualunque cosa dicano gli agenti.
    v = fe.compute(BASE_TS + 8999 * 100, "SIMULATION")
    blocked = eng.decide(v, {**ctx, "blocking": ["DATA_STALE"]})
    assert blocked.direction == NO_TRADE and "DATA_STALE" in blocked.blockers
    safe = eng.decide(v, {**ctx, "safe_mode": True})
    assert "SAFE_MODE" in safe.blockers

    # Ogni NO_TRADE deve spiegarsi.
    assert blocked.explain()["conclusion"].startswith("NO_TRADE"), blocked.explain()
    return (f"confidenza max {top:.1%} (tetto senza calibrazione), "
            f"{emitted}/{total} emesse, cancelli duri rispettati")


def t_calibration() -> str:
    """Se mostra 70%, si deve poter verificare quanto spesso ha vinto."""
    rng = random.Random(4)
    # Un modello SOVRASICURO: dichiara tanto, realizza poco.
    probs, outs = [], []
    for _ in range(1500):
        true_p = 0.55
        stated = 0.5 + (true_p - 0.5) * 3.0        # esagera di tre volte
        probs.append(min(0.97, stated))
        outs.append(1 if rng.random() < true_p else 0)
    curve = reliability_curve(probs, outs)
    verdict = calibration_verdict(curve, 1 / 1.8)
    assert verdict["overconfident"], verdict
    assert verdict["mean_gap"] < 0, verdict

    # La calibrazione deve ridurre lo scarto.
    cal = fit_platt(probs, outs)
    fixed = [cal.transform(p) for p in probs]
    gap_before = abs(sum(probs) / len(probs) - sum(outs) / len(outs))
    gap_after = abs(sum(fixed) / len(fixed) - sum(outs) / len(outs))
    assert gap_after < gap_before, (gap_before, gap_after)

    iso = fit_isotonic(probs, outs, min_samples=300)
    assert iso is not None and iso.method == "isotonic"
    # La monotonia e' l'unica ipotesi che l'isotonica impone: va rispettata.
    xs = [0.55, 0.65, 0.75, 0.85]
    ys = [iso.transform(x) for x in xs]
    assert all(ys[i] <= ys[i + 1] + 1e-6 for i in range(len(ys) - 1)), ys
    lo, hi = wilson_interval(55, 100)
    assert lo < 0.55 < hi
    return (f"sovrasicurezza rilevata (scarto {verdict['mean_gap']:+.3f}), "
            f"Platt riduce {gap_before:.3f} -> {gap_after:.3f}, isotonica monotona")


def t_signal_timing() -> str:
    """Anticipo, congelamento, conferma e annullamento: i tempi del segnale."""
    cfg = _sim_config(signal_lead_seconds=30, signal_freeze_seconds=5)
    se = SignalEngine(cfg)
    now = BASE_TS
    entry = se.next_entry_ts(now)
    lead = (entry - now) / 1000.0
    assert lead >= cfg.signal_lead_seconds, f"anticipo {lead}s sotto il richiesto"
    assert entry % 60_000 == 0, "l'entrata non e' allineata al minuto"

    from ..core.decision import Decision
    def mk(direction=CALL, prob=0.66, blockers=None):
        d = Decision(decision_id="d1", ts=now, mode="SIMULATION",
                     direction=direction, probability=prob,
                     confidence=max(prob, 1 - prob),
                     edge=max(prob, 1 - prob) - cfg.breakeven_win_rate,
                     regime="TREND")
        d.blockers = blockers or []
        return d

    sig = se.create(mk(), cycle_id=1, stake=25.0, now=now)
    assert sig is not None and sig.state == PRE_SIGNAL
    assert sig.lead_ms >= cfg.lead_ms, sig.lead_ms
    assert sig.expiry_ts - sig.entry_ts == cfg.horizon_ms

    # A T-20 si conferma.
    t = sig.entry_ts - 20_000
    se.update(mk(), t, 1.0850)
    assert sig.state == CONFIRMED, sig.state
    # A T-3 e' congelato: da li' non cambia piu'.
    se.update(mk(), sig.entry_ts - 3_000, 1.0850)
    assert sig.state == FROZEN, sig.state
    # A T entra.
    se.update(mk(), sig.entry_ts, 1.08500)
    assert sig.state == ENTERED and sig.entry_price == 1.08500
    # A T+60 si chiude.
    assert se.due_for_settlement(sig.expiry_ts) == [sig]
    assert se.settle(sig, 1.08520, sig.expiry_ts) == WIN
    assert se.settle.__doc__

    # Annullamento: la direzione si inverte prima dell'entrata. Serve che il
    # ribaltamento PERSISTA: un solo tick contrario non annulla niente.
    se2 = SignalEngine(cfg)
    s2 = se2.create(mk(), 1, 25.0, now)
    flip = mk(direction=PUT, prob=0.34)
    se2.update(flip, s2.entry_ts - 15_000, 1.0850)
    assert s2.state != CANCELLED, "annullato su un solo tick contrario"
    se2.update(flip, s2.entry_ts - 12_000, 1.0850)
    assert s2.state == CANCELLED, s2.state
    assert "invertita" in (s2.cancelled_reason or ""), s2.cancelled_reason

    # Un ribaltamento che rientra prima della soglia non deve annullare: e'
    # esattamente il rumore da cui la persistenza protegge.
    se2b = SignalEngine(cfg)
    s2b = se2b.create(mk(), 1, 25.0, now)
    se2b.update(flip, s2b.entry_ts - 15_000, 1.0850)
    se2b.update(mk(), s2b.entry_ts - 14_500, 1.0850)          # rientra
    se2b.update(flip, s2b.entry_ts - 14_000, 1.0850)
    assert s2b.state != CANCELLED, "il contatore non si e' azzerato al rientro"

    # Annullamento: la probabilita' a favore crolla sotto il pareggio.
    se3 = SignalEngine(cfg)
    s3 = se3.create(mk(), 1, 25.0, now)
    weak = mk(direction=NO_TRADE, prob=0.52, blockers=["CONFIDENCE_LOW"])
    se3.update(weak, s3.entry_ts - 15_000, 1.0850)
    se3.update(weak, s3.entry_ts - 12_000, 1.0850)
    assert s3.state == CANCELLED, s3.state
    assert "pareggio" in (s3.cancelled_reason or ""), s3.cancelled_reason

    # Un segnale aperto occupa lo slot e da solo produce MAX_CONCURRENT: se
    # quel blocco venisse letto come deterioramento, nessun segnale entrerebbe
    # mai. E' il difetto che si vedeva come "84 creati, 84 annullati".
    se4 = SignalEngine(cfg)
    s4 = se4.create(mk(), 1, 25.0, now)
    for offset in (25_000, 20_000, 15_000, 10_000):
        se4.update(mk(direction=NO_TRADE, blockers=["MAX_CONCURRENT"]),
                   s4.entry_ts - offset, 1.0850)
    assert s4.state == CONFIRMED, (s4.state, s4.cancelled_reason)

    # L'evoluzione va memorizzata: e' cio' che distingue un segnale che si
    # rafforza da uno che sta morendo.
    assert len(sig.updates) >= 4, len(sig.updates)
    return (f"anticipo {lead:.0f}s allineato al minuto, PRE->CONFIRMED->FROZEN"
            f"->ENTERED->WIN, 2 annullamenti su deterioramento persistente, "
            f"rumore e blocchi strutturali non annullano, "
            f"{len(sig.updates)} aggiornamenti registrati")


def t_wallet_cycles() -> str:
    """500 euro, 25 a operazione, fallimento, studio, ciclo nuovo."""
    path = os.path.join(tempfile.mkdtemp(), "w.db")
    cfg = _sim_config(database_path=path)
    db = Database(path)
    db.start()
    studied: list[int] = []

    def on_fail(w):
        studied.append(w.cycle_id)
        w.study_report = {"diagnosis": "prova"}
        w.start_new_cycle("dopo lo studio")

    w = VirtualWallet(cfg, db, on_cycle_failed=on_fail)
    assert w.status()["trades_to_zero"] == 20, w.status()["trades_to_zero"]

    losses = 0
    while not studied and losses < 40:
        sid = f"L{losses}"
        w.reserve(sid)
        w.settle(sid, LOSS)
        losses += 1
    assert losses == 20, f"fallito dopo {losses} perdite invece di 20"
    assert studied == [1] and w.cycle_id == 2 and w.balance == 500.0

    # Il ciclo precedente NON deve essere cancellato.
    db.flush()
    cycles = db.query("SELECT * FROM wallet_cycles ORDER BY cycle_id")
    assert len(cycles) >= 2, cycles
    assert cycles[0]["ending_balance"] is not None, "il ciclo fallito non e' stato salvato"

    # Un annullato non e' un'operazione.
    before = w.current.trades
    w.reserve("X")
    w.release("X")
    assert w.current.trades == before

    # Il pareggio non muove il saldo ma conta come operazione.
    w.reserve("D")
    w.settle("D", DRAW)
    assert w.balance == 500.0 and w.current.draws == 1

    w.reserve("W")
    w.settle("W", WIN)
    assert abs(w.balance - (500.0 + 25.0 * cfg.payout)) < 0.01, w.balance

    # Il saldo si ricostruisce dal registro, non da un contatore in memoria.
    db.flush()
    w2 = VirtualWallet(cfg, db)
    assert abs(w2.balance - w.balance) < 0.01 and w2.cycle_id == w.cycle_id
    db.stop()
    return (f"20 perdite -> fallimento -> studio -> ciclo 2; storico conservato, "
            f"pareggio e annullato trattati correttamente, saldo ricostruito")


def t_validation() -> str:
    """Purga, campioni indipendenti, test multipli, fuga dal futuro."""
    cfg = _sim_config()
    v = WalkForwardValidator(cfg)
    assert v.purge_gap_ms >= cfg.horizon_ms, "purga sotto l'orizzonte"

    ts = [BASE_TS + i * 1000 for i in range(3600)]
    idx = independent_indices(ts, 60_000)
    assert 55 <= len(idx) <= 61, len(idx)

    # Il confronto e' contro il PAREGGIO, non contro la moneta.
    p_coin = binomial_p_value(60, 100, 0.5)
    p_be = binomial_p_value(60, 100, cfg.breakeven_win_rate)
    assert p_be > p_coin * 3, (p_coin, p_be)

    # Test multipli: su rumore puro, con correzione, quasi niente sopravvive.
    rng = random.Random(11)
    ps = [binomial_p_value(rng.randint(40, 62), 100, 0.5) for _ in range(60)]
    naive = sum(1 for p in ps if p < 0.05)
    surv, _ = benjamini_hochberg(ps, 0.05)
    assert sum(surv) <= naive, (naive, sum(surv))

    def fit(d):
        return fit_estimator(PureLogistic(epochs=12), d.rows, d.y)

    def pred(m, d):
        return predict_proba(m, d.rows)

    # Dati imparabili: si deve accorgere.
    rows, y, tss = [], [], []
    for i in range(4000):
        a = rng.gauss(0, 1)
        rows.append([a, rng.gauss(0, 1)])
        y.append(1 if a + 0.4 * rng.gauss(0, 1) > 0 else 0)
        tss.append(BASE_TS + i * 1000)
    good = v.run(Dataset(rows, y, tss, ["alpha", "noise"], 60), fit, pred)
    assert good["status"] == "COMPLETO" and good["beats_breakeven"], good

    # Rumore puro: NON deve trovare niente.
    rows2 = [[rng.gauss(0, 1), rng.gauss(0, 1)] for _ in range(4000)]
    y2 = [rng.randint(0, 1) for _ in range(4000)]
    noise = v.run(Dataset(rows2, y2, tss, ["a", "b"], 60), fit, pred)
    assert not noise["beats_breakeven"], noise

    # Fuga dal futuro: una colonna uguale all'etichetta va scoperta.
    leak = leakage_check(
        Dataset([[r[0], float(t)] for r, t in zip(rows, y)], y, tss,
                ["alpha", "LEAK"], 60), 60_000)
    assert leak["status"] == "SOSPETTO" and leak["suspects"], leak
    return (f"purga {v.purge_gap_ms}ms, 3600->{len(idx)} indipendenti, "
            f"p vs moneta {p_coin:.4f} vs pareggio {p_be:.4f}, "
            f"imparabile {good['accuracy_independent']} / rumore "
            f"{noise['accuracy_independent']}, fuga rilevata")


def t_research() -> str:
    """La ricerca promuove su un vantaggio vero e tace sul rumore."""
    cfg = _sim_config(research_min_samples=200, setup_min_samples=40)
    db = Database(":memory:")
    db.start()
    re_ = ResearchEngine(cfg, db)
    rng = random.Random(9)
    names = ["return_5s_bps", "return_15s_bps", "momentum", "acceleration",
             "tick_imbalance_1s", "tick_imbalance_5s", "tick_imbalance_15s",
             "quote_velocity_10s"]
    rows, y, ts = [], [], []
    for i in range(20000):
        a = rng.gauss(0, 1)
        rows.append([a, a * 0.7 + rng.gauss(0, 0.6), a * 0.8, rng.gauss(0, 1)]
                    + [rng.gauss(0, 1) for _ in range(4)])
        y.append(1 if a + 0.5 * rng.gauss(0, 1) > 0 else 0)
        ts.append(BASE_TS + i * 1000)
    rep = re_.run_once(Dataset(rows, y, ts, names, 60))
    assert rep["status"] == "COMPLETO", rep.get("status")
    assert rep["holdout_survivors"] >= 1, rep["verdict"]

    noise_rows = [[rng.gauss(0, 1) for _ in names] for _ in range(20000)]
    noise_y = [rng.randint(0, 1) for _ in range(20000)]
    rep2 = re_.run_once(Dataset(noise_rows, noise_y, ts, names, 60))
    assert rep2["holdout_survivors"] == 0, rep2["verdict"]
    assert rep2["fdr_survivors"] == 0, rep2

    # Libreria dei setup: separa vincenti e perdenti con l'intervallo, non
    # con la stima puntuale.
    lib = SetupLibrary(cfg)
    for i in range(400):
        feats = {"tick_imbalance_5s": 0.5 if i % 2 else -0.5, "momentum": 1.5,
                 "move_over_noise": 4.0, "session_overlap": 1.0}
        won = rng.random() < (0.72 if i % 2 else 0.38)
        lib.observe(feats, "TREND", WIN if won else LOSS)
    win_setups, lose_setups = lib.classify()
    assert win_setups and lose_setups, (len(win_setups), len(lose_setups))
    sim = lib.similar({"tick_imbalance_5s": 0.5, "momentum": 1.5,
                       "move_over_noise": 4.0, "session_overlap": 1.0}, "TREND")
    assert sim and sim[0]["samples"] >= cfg.setup_min_samples, sim
    db.stop()
    return (f"vantaggio vero: {rep['holdout_survivors']} promossi su "
            f"{rep['candidates_tested']}; rumore: 0 su {rep2['candidates_tested']}; "
            f"libreria {len(win_setups)} vincenti / {len(lose_setups)} perdenti")


def t_booster() -> str:
    """In ombra il booster NON tocca il segnale live, e lo si verifica."""
    cfg = _sim_config(booster_mode="shadow", booster_min_samples=200)
    b = SignalBooster(cfg)
    from ..core.decision import Decision

    class FakeSig:
        direction = CALL

    d = Decision(decision_id="d", ts=BASE_TS, mode="SIMULATION",
                 direction=CALL, probability=0.64, confidence=0.64, edge=0.08)
    d.features = {"tick_imbalance_5s": 0.6, "acceleration": 1.2,
                  "spread_vs_average": 0.8, "noise_floor_bps": 0.4}
    history = [{"order_flow": 0.1, "probability": 0.60, "regime": "TREND"},
               {"order_flow": 0.4, "probability": 0.62, "regime": "TREND"},
               {"order_flow": 0.6, "probability": 0.64, "regime": "TREND"}]
    res = b.evaluate(FakeSig(), d, history)
    assert res.action in ("BOOST", "NEUTRAL", "DEBOOST", "VETO")
    assert not res.applied, "in ombra il booster ha toccato il segnale live"
    # Senza calibrazione NON deve spostare la probabilita'.
    assert abs(res.boosted_probability - res.base_probability) < 1e-9, (
        "il booster ha spostato la probabilita' senza calibrazione")
    assert res.latency_ms >= 0

    # Un flusso in cedimento deve produrre un giudizio negativo.
    d2 = Decision(decision_id="d2", ts=BASE_TS, mode="SIMULATION",
                  direction=CALL, probability=0.64, confidence=0.64, edge=0.08)
    d2.features = {"tick_imbalance_5s": -0.6, "spread_vs_average": 1.8,
                   "noise_floor_bps": 0.4}
    bad = b.evaluate(FakeSig(), d2,
                     [{"order_flow": 0.7, "probability": 0.70, "regime": "TREND"},
                      {"order_flow": 0.1, "probability": 0.60, "regime": "RANGE"},
                      {"order_flow": -0.5, "probability": 0.53, "regime": "RANGE"}])
    assert bad.score < res.score, (bad.score, res.score)
    assert bad.action in ("DEBOOST", "VETO"), bad.to_dict()

    # Promozione: senza prove non si esce dall'ombra.
    ok, missing = b.can_promote(cfg.breakeven_win_rate)
    assert not ok and missing, (ok, missing)
    rng = random.Random(2)
    for _ in range(400):
        score = rng.uniform(-1, 1)
        b.record(type("R", (), {"score": score, "base_probability": 0.6})(),
                 rng.random() < 0.5)
    rep = b.report(cfg.breakeven_win_rate)
    assert rep["status"] == "OK" and not rep["improves"], rep["verdict"]
    ok2, missing2 = b.can_promote(cfg.breakeven_win_rate)
    assert not ok2, "promosso senza dimostrare un miglioramento"
    return (f"ombra: non applica e non sposta la probabilita'; "
            f"flusso in cedimento -> {bad.action}; promozione negata "
            f"({len(missing2)} requisiti mancanti)")


def t_replay_backtest() -> str:
    """Il replay riproduce con un orologio virtuale, e il backtest e' onesto."""
    tmp = tempfile.mkdtemp()
    csv_path = os.path.join(tmp, "q.csv")
    with open(csv_path, "w", encoding="utf-8") as fh:
        fh.write("ts,mid\n")
        price = 1.08500
        rng = random.Random(5)
        for i in range(3000):
            price += rng.gauss(0, 0.000005)
            fh.write(f"{BASE_TS + i * 200},{price:.5f}\n")

    cfg = _sim_config()
    clock = VirtualClock()
    adapter = ReplayAdapter(cfg, csv_path, speed=0.0, clock=clock)
    n = adapter.load()
    assert n == 3000, n
    # L'orologio segue i DATI: e' cio' che rende le scadenze sensate quando
    # un'ora di mercato si ripercorre in un secondo.
    assert clock.now_ms() == BASE_TS, clock.now_ms()

    async def drain():
        seen = 0
        async for q in adapter.quotes():
            seen += 1
            assert q.mode == "REPLAY"
            if seen >= 500:
                break
        return seen

    seen = asyncio.run(drain())
    assert seen == 500
    assert clock.now_ms() > BASE_TS, "l'orologio virtuale non e' avanzato"
    # E non torna mai indietro.
    before = clock.now_ms()
    clock.set(BASE_TS)
    assert clock.now_ms() == before, "l'orologio virtuale e' tornato indietro"
    return f"{n} quotazioni caricate, orologio virtuale monotono a {seen} tick"


def t_engine_end_to_end() -> str:
    """Il sistema intero su dati simulati: dal tick all'esito, sul database."""
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "e2e.db")
    cfg = _sim_config(database_path=path, decision_interval_ms=250,
                      signal_lead_seconds=30, research_enabled=False,
                      min_edge=0.0, min_confidence=0.50)
    db = Database(path)
    engine = AurumEngine(cfg, MarketFeed(cfg, SimulationAdapter(cfg, seed=13)), db)
    db.start()

    # Si guida il motore manualmente con un orologio deterministico: cosi' un
    # test di dieci minuti di mercato dura un secondo.
    sim = engine.feed.adapter
    ts = BASE_TS
    for i in range(9000):                      # 900 secondi a 100 ms
        ts = BASE_TS + i * 100
        q = sim.step(0.1, ts=ts)
        engine.feed.quality.observe(q)
        engine.feed.last_quote = q
        engine._on_quote(q)
    db.flush()

    stats = engine.signals.stats()
    dec = engine.decisions.stats()
    assert dec["decisions"] > 100, dec
    assert stats["created"] >= 1, (
        f"nessun segnale in 15 minuti simulati: {dec['blockers']}")
    assert stats["entered"] >= 1, f"nessuna entrata: {stats}"
    settled = stats["wins"] + stats["losses"] + stats["draws"]
    assert settled >= 1, f"nessun esito: {stats}"

    rows = db.query("SELECT * FROM signals WHERE result IS NOT NULL")
    assert rows, "nessun segnale con esito sul database"
    r = rows[0]
    # L'anticipo deve essere reale e misurabile su ogni riga.
    assert r["lead_ms"] >= cfg.lead_ms, r["lead_ms"]
    assert r["expiry_ts"] - r["entry_ts"] == cfg.horizon_ms
    assert r["price_basis"] == "mid", "base del prezzo non dichiarata"
    ups = db.query("SELECT COUNT(*) n FROM signal_updates")[0]["n"]
    assert ups > 0, "l'evoluzione del segnale non e' stata registrata"
    shadow = db.query("SELECT COUNT(*) n FROM shadow_decisions")[0]["n"]
    assert shadow > 0, "le decisioni bloccate non sono state registrate"

    lat = engine.latency.report()
    p50 = lat["pipeline_p50_ms"]
    assert p50 < 500.0, f"pipeline a {p50}ms, oltre il limite"

    # Il dataset costruito dal database deve essere causale e utilizzabile.
    ds = engine.build_dataset(include_simulation=True)
    assert len(ds) > 50, ds.notes
    leak = leakage_check(ds, cfg.horizon_ms)
    assert leak["timestamps_monotonic"], "dataset non ordinato nel tempo"

    health = engine.health()
    assert health["mode"] == "SIMULATION", health["mode"]
    diag = engine.diagnostics()
    assert diag["price_basis"] == "mid"
    db.stop()
    return (f"{dec['decisions']} decisioni, {stats['created']} segnali, "
            f"{stats['entered']} entrate, {settled} esiti, {ups} aggiornamenti, "
            f"{shadow} ombra, pipeline p50 {p50:.2f}ms, {len(ds)} righe dataset")


def t_notification() -> str:
    """Le notifiche non stanno sul percorso della decisione."""
    cfg = _sim_config(telegram_enabled=False)
    tg = TelegramNotifier(cfg)
    assert not tg.enabled
    assert tg.send("prova") is False, "ha inviato senza configurazione"

    cfg2 = _sim_config(telegram_enabled=True, telegram_bot_token="x",
                       telegram_chat_id="y")
    tg2 = TelegramNotifier(cfg2)
    assert tg2.enabled
    # Accodare deve essere immediato anche se la rete non risponde: non si
    # avvia il thread, quindi la coda resta piena e non parte nulla.
    t0 = time.perf_counter()
    for i in range(50):
        tg2.send(f"messaggio {i}")
    elapsed = (time.perf_counter() - t0) * 1000.0
    assert elapsed < 50.0, f"50 notifiche accodate in {elapsed:.0f}ms: sta bloccando"
    assert tg2._queue.qsize() == 50

    class S:
        direction = CALL
        entry_ts = BASE_TS + 60_000
        expiry_ts = BASE_TS + 120_000
        created_ts = BASE_TS
        probability = 0.66
        confidence = 0.66
        edge = 0.10
        regime = "TREND"
        news_risk = "LOW"
    msg = tg2.signal_message(S(), cfg2)
    assert "CALL" in msg and "MANUALE" in msg, msg
    return f"50 messaggi accodati in {elapsed:.1f}ms senza toccare la rete"


def t_no_execution() -> str:
    """La garanzia che conta: nel progetto non esiste un percorso d'ordine.

    Non e' una promessa nei commenti: si legge il codice sorgente di tutti i
    moduli e si fallisce se qualcuno aggiunge una chiamata capace di eseguire.
    """
    import pathlib
    root = pathlib.Path(__file__).resolve().parent.parent
    forbidden = ("place" + "_order(", "create" + "_order(", "send" + "_order(",
                 "execute" + "_order(", "submit" + "_order(",
                 ".buy" + "(", ".sell" + "(", "open" + "_position(",
                 "webdriver", "selenium", "playwright")
    found: list[str] = []
    files = 0
    for path in root.rglob("*.py"):
        if path.name == "selftest.py":
            continue
        files += 1
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                found.append(f"{path.relative_to(root)}: {token}")
    assert not found, f"percorso di esecuzione trovato: {found}"
    return (f"{files} moduli analizzati: nessuna chiamata d'ordine, "
            f"nessuna automazione del browser")


def t_dashboard() -> str:
    """L'interfaccia e le sue rotte esistono e rispondono."""
    from ..dashboard.server import build_app, INDEX_HTML, transport_state
    cfg = _sim_config()
    db = Database(":memory:")
    engine = AurumEngine(cfg, MarketFeed(cfg, SimulationAdapter(cfg, seed=2)), db)
    app = build_app(cfg, engine)
    routes = {getattr(r, "path", None) for r in app.routes}
    required = {"/", "/health", "/market", "/candles", "/signal", "/signals",
                "/agents", "/wallet", "/cycles", "/statistics", "/latency",
                "/diagnostics", "/blockers", "/booster", "/research",
                "/config", "/orderflow", "/preview", "/models", "/strategy"}
    missing = required - routes
    assert not missing, f"rotte mancanti: {sorted(missing)}"
    # Il frontend deve parlare di EUR/USD e non di criptovalute.
    for banned in ("BTC", "Bitcoin", "BTCUSDT"):
        assert banned not in INDEX_HTML, f"riferimento a {banned} nel frontend"
    assert "EUR/USD" in INDEX_HTML
    for element in ("chart", "wallet", "agents", "signal", "blockers", "latency"):
        assert f'id="{element}' in INDEX_HTML, f"manca il pannello {element}"

    # Il trasporto degli aggiornamenti va DICHIARATO. Senza il pacchetto
    # `websockets`, uvicorn respinge /ws con un 404 e il browser ritenta per
    # sempre: la pagina sembra viva e non lo e'. Qui si verifica che lo stato
    # sia esposto e che il frontend sappia ripiegare invece di insistere.
    t = transport_state()
    assert set(t) == {"realtime", "transport", "reason", "fix"}, t
    assert isinstance(t["realtime"], bool)
    if not t["realtime"]:
        assert t["reason"] and t["fix"], t
    assert "p-transport" in INDEX_HTML, "manca l'indicatore del trasporto"
    assert "WSTRIES <= 3" in INDEX_HTML, (
        "il frontend ritenta il WebSocket senza limite")

    db.stop()
    return (f"{len(routes)} rotte, tutte le richieste presenti, frontend "
            f"EUR/USD, trasporto dichiarato ({t['transport']})")


CASES: list[tuple[str, Callable[[], str]]] = [
    ("configurazione e payout", t_config),
    ("database (batch, upsert, guasto isolato)", t_database),
    ("schema: colonne vere e nessuna tabella orfana", t_schema_coerente),
    ("buffer causali e candele", t_buffers),
    ("feature engine su scala EUR/USD", t_features),
    ("qualita' dei dati", t_data_quality),
    ("agenti (contratto, astensione, guasto)", t_agents),
    ("affidabilita' per regime", t_reliability),
    ("motore decisionale e tetto alla confidenza", t_decision),
    ("calibrazione (Platt, isotonica, affidabilita')", t_calibration),
    ("tempi del segnale (anticipo, freeze, annullamento)", t_signal_timing),
    ("portafoglio virtuale e cicli", t_wallet_cycles),
    ("validazione (purga, indipendenza, FDR, leakage)", t_validation),
    ("ricerca (promozione e rifiuto)", t_research),
    ("booster in ombra", t_booster),
    ("replay con orologio virtuale", t_replay_backtest),
    ("notifiche non bloccanti", t_notification),
    ("nessun percorso di esecuzione", t_no_execution),
    ("dashboard e rotte", t_dashboard),
    ("motore end-to-end", t_engine_end_to_end),
]


def run_selftest(cfg: Config, verbose: bool = True) -> int:
    from ..config import APP_NAME, VERSION
    print(f"\n{APP_NAME} {VERSION} — selftest\n")
    passed = 0
    failures: list[tuple[str, str]] = []
    for name, fn in CASES:
        started = time.perf_counter()
        try:
            detail = fn()
            elapsed = (time.perf_counter() - started) * 1000.0
            passed += 1
            print(f"[  OK  ] {name}  ({elapsed:.0f}ms)")
            if verbose and detail:
                print(f"         {detail}")
        except AssertionError as exc:
            failures.append((name, str(exc)))
            print(f"[ FAIL ] {name}")
            print(f"         {exc}")
        except Exception as exc:  # noqa: BLE001
            failures.append((name, f"{type(exc).__name__}: {exc}"))
            print(f"[ FAIL ] {name}")
            print(f"         {type(exc).__name__}: {exc}")
    total = len(CASES)
    print(f"\n{passed}/{total} verifiche superate")
    if failures:
        print("\nFallite:")
        for name, why in failures:
            print(f"  - {name}: {why[:200]}")
        return 1
    return 0
