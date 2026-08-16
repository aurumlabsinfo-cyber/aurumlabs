#!/usr/bin/env python3
"""
AURUM BURST-15  —  strategia a sessione di 15 minuti su orizzonte 5s.

Logica:
  Non prevede dove sara' il prezzo fra 15 minuti (statisticamente impossibile
  con i dati disponibili). Apre invece una FINESTRA OPERATIVA di 15 minuti e
  dentro quella finestra prende solo i burst di tape, con orizzonte 5 secondi.

Trigger (valutato a fine secondo t, entrata t+1s, scadenza entrata+5s):
    n5    = n. trade negli ultimi 5s          >= N5_MIN
    |r10| = |return ultimi 10s| in bps        >= R10_MIN
    ofi5  = order flow imbalance 5s           segno concorde con r10
    direzione = segno di r10   (momentum)

Uso:
    python3 aurum_burst15.py backtest --trades trades.csv
    python3 aurum_burst15.py sessions --trades trades.csv
    python3 aurum_burst15.py grid     --trades trades.csv
    python3 aurum_burst15.py live     --stdin        (una riga JSON per trade)
"""
from __future__ import annotations
import argparse, json, sys
from collections import deque
from dataclasses import dataclass, asdict

# --------------------------------------------------------------------------- #
#  CONFIG — le uniche manopole da toccare
# --------------------------------------------------------------------------- #

@dataclass
class Config:
    # --- trigger ---
    n5_min: int = 40          # trade negli ultimi 5s (filtro tape viva)
    r10_min_bps: float = 0.5  # ampiezza minima del movimento 10s
    require_ofi_agree: bool = True
    horizon_s: int = 5        # orizzonte del binario
    entry_delay_ms: int = 1000

    # --- sessione 15 min ---
    session_s: int = 900
    cooldown_ms: int = 6000   # niente trigger sovrapposti
    max_trades_session: int = 40
    stop_loss_units: float = -6.0   # chiude la sessione
    take_profit_units: float = 15.0

    # --- economia ---
    payout: float = 0.8       # vincita per 1 unita' rischiata
    stake: float = 1.0

    # --- guardie ---
    max_staleness_ms: int = 2000   # feed fermo -> nessun trigger
    min_book_quality: float = 0.75


# --------------------------------------------------------------------------- #
#  MOTORE FEATURE — stato incrementale su stream di trade
# --------------------------------------------------------------------------- #

class BurstState:
    """Ring buffer sui trade. O(1) ammortizzato per tick."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.trades: deque = deque()   # (ts, price, notional, is_buy)
        self.last_ts = 0

    def push(self, ts: int, price: float, notional: float, aggressor: str) -> None:
        self.trades.append((ts, price, notional, aggressor.upper() == "BUY"))
        self.last_ts = ts
        cutoff = ts - 15_000
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    # --- feature ---------------------------------------------------------- #
    def _price_at(self, ts: int):
        p = None
        for t, px, _, _ in self.trades:
            if t <= ts:
                p = px
            else:
                break
        return p

    def features(self, now: int):
        if not self.trades:
            return None
        n5 = sum(1 for t, *_ in self.trades if t >= now - 5_000)
        p_now = self.trades[-1][1]
        p_10 = self._price_at(now - 10_000)
        if p_10 is None or p_10 <= 0:
            return None
        r10 = (p_now - p_10) / p_10 * 1e4
        buy = sum(nt for t, _, nt, b in self.trades if t >= now - 5_000 and b)
        sell = sum(nt for t, _, nt, b in self.trades if t >= now - 5_000 and not b)
        ofi5 = (buy - sell) / (buy + sell) if (buy + sell) > 0 else 0.0
        return {"n5": n5, "r10": r10, "ofi5": ofi5,
                "staleness_ms": now - self.last_ts, "price": p_now}

    # --- decisione -------------------------------------------------------- #
    def decide(self, now: int, data_quality: float = 1.0):
        """Ritorna 'UP' | 'DOWN' | None con i motivi del blocco."""
        c = self.cfg
        f = self.features(now)
        if f is None:
            return None, ["feature non disponibili"], {}
        blocked = []
        if f["staleness_ms"] > c.max_staleness_ms:
            blocked.append(f"feed fermo da {f['staleness_ms']}ms")
        if data_quality < c.min_book_quality:
            blocked.append(f"data quality {data_quality:.2f}")
        if f["n5"] < c.n5_min:
            blocked.append(f"tape morta: {f['n5']} trade/5s < {c.n5_min}")
        if abs(f["r10"]) < c.r10_min_bps:
            blocked.append(f"movimento {abs(f['r10']):.2f}bps < {c.r10_min_bps}")
        direction = 1 if f["r10"] > 0 else -1
        if c.require_ofi_agree and (f["ofi5"] > 0) != (f["r10"] > 0):
            blocked.append(f"flusso discorde: ofi5 {f['ofi5']:+.2f} vs r10 {f['r10']:+.2f}")
        if blocked:
            return None, blocked, f
        return ("UP" if direction > 0 else "DOWN"), [], f


# --------------------------------------------------------------------------- #
#  SESSIONE 15 MINUTI
# --------------------------------------------------------------------------- #

class Session:
    def __init__(self, cfg: Config, start_ts: int):
        self.cfg = cfg
        self.start = start_ts
        self.end = start_ts + cfg.session_s * 1000
        self.pnl = 0.0
        self.trades = []
        self.last_entry = -10**18
        self.closed_reason = None

    def can_trade(self, ts: int) -> bool:
        if self.closed_reason:
            return False
        if ts >= self.end:
            self.closed_reason = "sessione conclusa"
            return False
        if len(self.trades) >= self.cfg.max_trades_session:
            self.closed_reason = "max trade raggiunto"
            return False
        if self.pnl <= self.cfg.stop_loss_units:
            self.closed_reason = "stop loss di sessione"
            return False
        if self.pnl >= self.cfg.take_profit_units:
            self.closed_reason = "take profit di sessione"
            return False
        if ts - self.last_entry < self.cfg.cooldown_ms:
            return False
        return True

    def settle(self, ts: int, direction: str, entry: float, expiry: float) -> float:
        if expiry == entry:
            pnl = 0.0
            res = "TIE"
        elif (expiry > entry) == (direction == "UP"):
            pnl = self.cfg.payout * self.cfg.stake
            res = "WIN"
        else:
            pnl = -self.cfg.stake
            res = "LOSS"
        self.pnl += pnl
        self.last_entry = ts
        self.trades.append({"ts": ts, "dir": direction, "entry": entry,
                            "expiry": expiry, "result": res, "pnl": pnl})
        return pnl


# --------------------------------------------------------------------------- #
#  BACKTEST SU CSV
# --------------------------------------------------------------------------- #

def load_trades(path: str):
    import pandas as pd
    df = pd.read_csv(path, usecols=["ts", "price", "quantity", "notional", "aggressor"])
    return df.sort_values("ts").reset_index(drop=True)


def run_backtest(cfg: Config, path: str, session_mode: bool = False, verbose: bool = True):
    import numpy as np, pandas as pd
    df = load_trades(path)
    TS = df["ts"].to_numpy(); PX = df["price"].to_numpy()
    NT = df["notional"].to_numpy(); IB = (df["aggressor"].to_numpy() == "BUY")

    # --- feature vettorializzate su griglia da 1s (identiche a BurstState) ---
    sec = (TS // 1000) * 1000
    grid = np.arange(sec.min(), sec.max() + 1000, 1000)
    idx = np.searchsorted(sec, grid, "right") - 1
    has = idx >= 0
    last_px = np.where(has, PX[np.clip(idx, 0, len(PX) - 1)], np.nan)
    last_px = pd.Series(last_px).ffill().to_numpy()

    cnt = np.bincount(((sec - grid[0]) // 1000).astype(int), minlength=len(grid))
    buy = np.bincount(((sec - grid[0]) // 1000).astype(int), weights=NT * IB, minlength=len(grid))
    sell = np.bincount(((sec - grid[0]) // 1000).astype(int), weights=NT * ~IB, minlength=len(grid))
    roll = lambda a, k: pd.Series(a).rolling(k).sum().to_numpy()
    n5 = roll(cnt, 5)
    b5, s5 = roll(buy, 5), roll(sell, 5)
    ofi5 = (b5 - s5) / (b5 + s5 + 1e-9)
    r10 = np.full(len(grid), np.nan)
    r10[10:] = (last_px[10:] - last_px[:-10]) / last_px[:-10] * 1e4

    trig = (n5 >= cfg.n5_min) & (np.abs(r10) >= cfg.r10_min_bps)
    if cfg.require_ofi_agree:
        trig &= (np.sign(ofi5) == np.sign(r10))
    trig &= np.isfinite(r10)

    # --- esecuzione a livello tick ---
    dec = grid + cfg.entry_delay_ms
    ie = np.searchsorted(TS, dec, "left")
    ok = ie < len(TS); ie = np.clip(ie, 0, len(TS) - 1)
    ets = TS[ie]
    ix = np.searchsorted(TS, ets + cfg.horizon_s * 1000, "left")
    ok &= ix < len(TS); ix = np.clip(ix, 0, len(TS) - 1)
    ok &= (ets - dec < cfg.max_staleness_ms)
    ok &= (TS[ix] - (ets + cfg.horizon_s * 1000) < cfg.max_staleness_ms)

    live = trig & ok
    entry, expiry = PX[ie], PX[ix]
    direction = np.sign(r10)

    if not session_mode:
        rows, last = [], -10**18
        for i in np.flatnonzero(live):
            if grid[i] - last < cfg.cooldown_ms:
                continue
            last = grid[i]
            e, x = entry[i], expiry[i]
            pnl = 0.0 if x == e else (cfg.payout if (x > e) == (direction[i] > 0) else -1.0)
            rows.append({"ts": grid[i], "dir": "UP" if direction[i] > 0 else "DOWN",
                         "entry": e, "expiry": x, "pnl": pnl,
                         "n5": n5[i], "r10": r10[i], "ofi5": ofi5[i]})
        r = pd.DataFrame(rows)
        if verbose and len(r):
            eq = r["pnl"].cumsum()
            print(f"trigger        : {len(r)}")
            print(f"win/tie/loss   : {(r.pnl>0).mean():.3f} / {(r.pnl==0).mean():.3f} / {(r.pnl<0).mean():.3f}")
            print(f"EV per trade   : {r.pnl.mean():+.4f} unita")
            print(f"PnL totale     : {r.pnl.sum():+.1f} unita")
            print(f"max drawdown   : {(eq - eq.cummax()).min():.1f} unita")
        return r

    # --- modalita' sessione ---
    sessions, cur = [], None
    for i in np.flatnonzero(live):
        ts = int(grid[i])
        if cur is None or ts >= cur.end or cur.closed_reason:
            if cur is not None and cur.trades:
                sessions.append(cur)
            cur = Session(cfg, ts)
        if not cur.can_trade(ts):
            continue
        cur.settle(ts, "UP" if direction[i] > 0 else "DOWN", entry[i], expiry[i])
    if cur is not None and cur.trades:
        sessions.append(cur)
    if verbose:
        p = np.array([s.pnl for s in sessions])
        n = np.array([len(s.trades) for s in sessions])
        print(f"sessioni       : {len(sessions)}")
        print(f"trade/sessione : media {n.mean():.1f}  mediana {np.median(n):.0f}")
        print(f"PnL/sessione   : media {p.mean():+.2f}  mediana {np.median(p):+.2f} unita")
        print(f"sessioni verdi : {(p>0).mean():.1%}")
        print(f"peggiore       : {p.min():+.1f}   migliore {p.max():+.1f}")
    return sessions


def run_grid(path: str):
    import numpy as np
    print(f"{'n5':>5}{'|r10|':>8}{'ofi':>5}{'trade':>8}{'win':>8}{'EV':>9}{'PnL':>9}")
    for n5 in (10, 20, 40, 60, 100):
        for r10 in (0.2, 0.5, 1.0, 2.0):
            for agree in (True, False):
                cfg = Config(n5_min=n5, r10_min_bps=r10, require_ofi_agree=agree)
                r = run_backtest(cfg, path, verbose=False)
                if len(r) < 50:
                    continue
                print(f"{n5:5d}{r10:8.1f}{'si' if agree else 'no':>5}"
                      f"{len(r):8d}{(r.pnl>0).mean():8.3f}{r.pnl.mean():+9.4f}{r.pnl.sum():+9.1f}")


def run_live(cfg: Config):
    """Legge trade JSON da stdin: {"ts":..,"price":..,"notional":..,"aggressor":"BUY"}"""
    st = BurstState(cfg)
    last_emit = -10**18
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            t = json.loads(line)
        except json.JSONDecodeError:
            continue
        st.push(int(t["ts"]), float(t["price"]), float(t.get("notional", 0)), t.get("aggressor", "BUY"))
        now = int(t["ts"])
        if now - last_emit < cfg.cooldown_ms:
            continue
        d, blocked, f = st.decide(now)
        if d:
            last_emit = now
            print(json.dumps({"ts": now, "signal": d, "horizon_s": cfg.horizon_s,
                              "entry_after_ms": cfg.entry_delay_ms, "features": f}), flush=True)


# --------------------------------------------------------------------------- #

def main():
    ap = argparse.ArgumentParser(description="AURUM BURST-15")
    ap.add_argument("mode", choices=["backtest", "sessions", "grid", "live", "config"])
    ap.add_argument("--trades", default="trades.csv")
    ap.add_argument("--n5", type=int)
    ap.add_argument("--r10", type=float)
    ap.add_argument("--payout", type=float)
    ap.add_argument("--stdin", action="store_true")
    a = ap.parse_args()

    cfg = Config()
    if a.n5 is not None: cfg.n5_min = a.n5
    if a.r10 is not None: cfg.r10_min_bps = a.r10
    if a.payout is not None: cfg.payout = a.payout

    if a.mode == "config":
        print(json.dumps(asdict(cfg), indent=2))
    elif a.mode == "backtest":
        run_backtest(cfg, a.trades)
    elif a.mode == "sessions":
        run_backtest(cfg, a.trades, session_mode=True)
    elif a.mode == "grid":
        run_grid(a.trades)
    elif a.mode == "live":
        run_live(cfg)


if __name__ == "__main__":
    main()
