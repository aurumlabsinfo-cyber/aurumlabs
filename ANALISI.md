# Analisi del motore e correzioni

Rapporto sul perché il motore non emetteva segnali, perché non imparava da
solo, e cosa è stato cambiato. Ogni punto indica anche **come è stato
verificato**, perché non tutto è verificabile allo stesso modo: questo
container non ha rotta verso nessun exchange (Binance, Coinbase e Kraken sono
tutti bloccati), quindi il feed reale non è stato testato qui.

---

## 1. «Non arrivano i segnali»

### 1.1 Una sola manopola controllava due cose diverse — *difetto principale*

In `signals/decision.py` il valore `SIGNAL_MIN_CONFIDENCE` veniva confrontato
con due grandezze che non hanno niente in comune:

```python
if mass < s.signal_min_confidence:        # consenso fra gli 8 agent
if confidence < s.signal_min_confidence:  # quanto è netta la direzione
```

`mass` è `accordo × confidenza media` degli agent. Chiedere **0,60** a otto
agent rumorosi e indipendenti è una soglia che su dati reali si raggiunge
raramente: è il singolo motivo per cui il motore poteva restare ore senza
emettere nulla.

**Correzione**: due manopole separate.
`SIGNAL_MIN_AGREEMENT=0.35` per il consenso, `SIGNAL_MIN_CONFIDENCE=0.55` per
la direzione.

*Verifica*: replay di 2.485 finestre registrate attraverso il motore
decisionale, stesse righe e stessi agent, cambiando solo le soglie: il blocco
per «agent agreement» scende dal **27,7% al 15,6%** delle finestre e le
emissioni passano dal 49,4% al 56,4%. (Dati sintetici: la proporzione su un
feed reale sarà diversa, la direzione dell'effetto no.)

### 1.2 `SIGNAL_MIN_EDGE` non poteva funzionare — *dimostrabile a tavolino*

Per costruzione `confidence = 0,5 + edge`. Con `SIGNAL_MIN_CONFIDENCE=0.60` la
soglia effettiva sull'edge era **0,10**, mentre `SIGNAL_MIN_EDGE=0.04`
dichiarava 0,04. Chi abbassava `SIGNAL_MIN_EDGE` non otteneva alcun effetto:
configurazione morta.

**Correzione**: `effective_min_confidence = max(min_confidence, 0.5 + min_edge)`,
un solo motivo di blocco che riporta entrambi i numeri, e i default resi
coerenti (0,55 ↔ 0,05).

### 1.3 L'agent `volatility` ignorava la configurazione

`MIN_EXPECTED_MOVE_TICKS` e `MAX_ZERO_MOVE_FRACTION` erano scritti come
costanti dentro `agents/catalog.py` (`2.0` e `0.35`). Chi allentava quei valori
nell'ambiente continuava a vedere «expected move too small to be exploitable»
per sempre, perché l'agent che genera quel messaggio non leggeva le
impostazioni.

**Correzione**: gli agent ricevono le `Settings` e le usano.
*Verifica*: test in `tests/test_agents_decision.py` (suite backend, 271 test).

### 1.4 Lo stesso agent inquinava la direzione

`VolatilityAgent` è documentato come «non dà mai una direzione», ma emetteva
`score = squash(return_1000ms) * 0.3`, che entrava nell'aggregato con il peso
dell'agent. **Correzione**: `score = 0.0`, è un cancello e basta.

### 1.5 Una sola anomalia bloccava tutto

`AnomalyDetector` produceva un veto per *qualsiasi* rilievo, incluse cose
perfettamente normali su un venue reale (book momentaneamente sbilanciato,
raffica di prints). **Correzione**: rilievi **hard** (book desincronizzato,
latenza, qualità dati, spike di prezzo, spread anomalo) vietano da soli; quelli
**soft** solo quando si accumulano oltre `ANOMALY_MAX_SEVERITY=0.5`.

### 1.6 `HTTP_PROXY_URL` era dichiarato e non usato da nessuno — *bug grave*

Era in `config.py` e `grep` non lo trovava in nessun altro file. Chi si trova
dietro una rete che non raggiunge il venue lo configurava, riavviava, non
riceveva **nessun dato** e vedeva un motore che rispondeva NO TRADE all'infinito
senza dire perché. È la causa più banale e più probabile di «non arrivano
segnali» su una macchina reale.

**Correzione**: `marketdata/net.py` instrada sia le chiamate REST (httpx) sia il
WebSocket attraverso il proxy; `websockets` è stato portato a 15.0.1 (il
supporto proxy sul client WS parte da lì) e, se la libreria fosse più vecchia,
l'engine **solleva un errore** invece di ignorare silenziosamente
l'impostazione. *Verifica*: due test in `tests/test_resilience.py`.

### 1.7 Non c'era modo di sapere *quale* cancello bloccava

I motivi cambiavano dieci volte al secondo e la UI mostrava solo l'ultimo.

**Correzione**: `GET /diagnostics` — conteggio di ogni cancello dall'avvio,
ordinato, con il verdetto in testa:

* nessun tick mai ricevuto → «NO MARKET DATA» (guarda il feed, non le soglie);
* book non sincronizzato → dice che serve lo snapshot REST;
* warm-up in corso → dice quanti secondi mancano;
* altrimenti → qual è il cancello che blocca di più, e la raccomandazione di
  controllare `/shadow` prima di allentarlo.

Gli stessi dati arrivano al dashboard sul frame `state` e il riquadro NO TRADE
ora mostra «cosa blocca, nel tempo» oltre all'ultima finestra.

### 1.8 864.000 righe al giorno di «non è successo niente»

Ogni decisione NO TRADE scriveva una riga in `signals`: a 10 Hz sono ~864k
righe al giorno che intasavano il batch writer e rendevano impossibile trovare
i segnali veri. Il registro per l'apprendimento è `shadow_decisions`, che ha una
sua cadenza.

**Correzione**: una riga ogni `NO_TRADE_ROW_INTERVAL_MS` (5s) più una ogni volta
che cambia l'insieme dei motivi.

---

## 2. «Non impara in automatico»

### 2.1 Era spento di default — e non compariva nemmeno nel template

`auto_retrain_enabled: bool = False` in `config.py`, e `AUTO_RETRAIN_ENABLED`
non era presente in `.env.example`. Non c'era modo di accorgersene leggendo la
configurazione: il riaddestramento automatico non è mai partito.

**Correzione**: default `true`, documentato in `.env.example` con cosa fa e cosa
*non* fa (non impara dal singolo trade appena chiuso — è così che si insegue il
rumore).

### 2.2 Se il database non rispondeva all'avvio, non imparava mai più

```python
if self.db_ready:            # container.py, all'avvio
    await self.retrain.start()
```

Database su in ritardo di trenta secondi = servizio mai avviato fino al riavvio
successivo. **Correzione**: il servizio parte sempre e ricontrolla a ogni ciclo,
riportando «database unavailable» nel proprio stato invece di sparire.

### 2.3 Nessun modello poteva mai essere promosso

In `ml/runner.py::_fit_and_save` il modello veniva salvato con
`calibrated=False` **fisso**. In `decision.py` un modello non calibrato pesa
0,3 invece di 0,5 nel blend: nessuna quantità di riaddestramento poteva
promuoverlo, perché nessuno calibrava mai niente.

**Correzione**: calibrazione isotonica su una **coda hold-out** separata dal
training da un intervallo pari a orizzonte + embargo, come nei fold
walk-forward. Se le righe non bastano (< 500 utili) il modello resta non
calibrato e lo dichiara, invece di fingere.

*Verifica*: `AUTO_RETRAIN_ENABLED=true` con feed sintetico, il ciclo gira
(3 esecuzioni in 90 s) e riporta correttamente «0 righe registrate» — perché le
righe sintetiche sono escluse dall'addestramento, come deve essere.

---

## 3. Strategia AURUM BURST-15 implementata

Il file `strategies/aurum_burst15.py` è nel repository **invariato**, come
riferimento. La strategia è stata portata dentro il motore:

| Cosa | Dove |
|---|---|
| Strategia live (trigger, sessione, guardie) | `backend/app/signals/burst.py` |
| Backtest offline con sessioni | `backend/app/ml/burst_backtest.py` |
| Regola d'ingresso come candidato di ricerca | `backend/app/ml/strategies.py::burst15` |
| Feature mancanti aggiunte | `return_10000ms`, `ofi_notional_5s` |
| Test | `backend/tests/test_burst.py` (25 test) |

Si attiva con `SIGNAL_STRATEGY=burst15`. Il dettaglio completo — trigger,
ingresso a tempo, regole di sessione, comandi — è in [BURST15.md](BURST15.md).

Due punti che meritano attenzione perché sono differenze reali rispetto al
percorso «ensemble»:

* **l'ingresso è a tempo, non a prezzo**: `BURST_ENTRY_DELAY_MS` dopo il
  trigger, al mercato. Non si aspetta che il prezzo torni a un livello, perché
  la premessa è che il movimento sia già in corso. Ha richiesto un
  `entry_mode="DELAY"` nel ciclo di vita del segnale;
* **la sessione è parte della strategia**: stop-loss e take-profit chiudono la
  finestra, e una finestra chiusa in stop **non riapre** prima della sua
  scadenza naturale.

*Verifica*: motore reale su feed sintetico, 90 secondi → 13 segnali, entrate a
tempo eseguite, P&L di sessione aggiornato (8 vinti / 5 persi, +1,4 unità),
cooldown e limiti rispettati.

---

## 4. Tutto in un file solo: `aurum_engine.py`

Stesso motore, una sola dipendenza: Python. Database **SQLite** invece di
PostgreSQL, client WebSocket e server HTTP scritti dentro il file, e
l'apprendimento come regressione logistica implementata a mano.

```bash
python3 aurum_engine.py selftest                      # 9 verifiche interne
python3 aurum_engine.py check                         # il venue risponde?
python3 aurum_engine.py run --strategy burst15 --payout 0.8
python3 aurum_engine.py run --source sim              # senza rete
python3 aurum_engine.py run --source csv --csv trades.csv
python3 aurum_engine.py backtest                      # walk-forward
python3 aurum_engine.py burst                         # replay BURST-15
python3 aurum_engine.py shadow                        # finestre non tradate
```

Contiene tutto quanto sopra: gli 8 agenti con i cancelli corretti, BURST-15 con
la sessione, l'order book sequenziato, ~50 feature causali, il paper trading
con trigger e countdown lato motore, la diagnostica dei cancelli, il
riaddestramento automatico con calibrazione, statistiche e calibrazione,
dashboard su `http://localhost:8000`, e il supporto proxy su REST **e**
WebSocket.

Verificato: `selftest` supera 9/9 (framing WebSocket con frammentazione e ping,
sequenza dell'order book, feature, regole di BURST-15, coerenza delle soglie,
database, apprendimento con calibrazione e guardia OOD, statistica, motore
end-to-end). Su 30 minuti di dati replayati: 18.000 righe di feature, 80 trade
su carta, 1.800 finestre shadow, e il ciclo di apprendimento che addestra,
valida walk-forward, calibra su una coda separata e attiva il modello.

Non c'e' dentro: la ricerca di strategie con correzione per test multipli,
l'importatore dell'archivio Binance, i modelli ad alberi e il frontend Next.js.
Quelli restano nel progetto completo.

---

## 5. Come verificare sulla tua macchina

```bash
cp .env.example .env          # poi: SIGNAL_STRATEGY, BINARY_PAYOUT, eventuale HTTP_PROXY_URL
docker compose up -d --build

curl localhost:8000/health | jq '.components'      # il feed arriva davvero?
curl localhost:8000/diagnostics | jq '.verdict, .blocking_gates'
curl localhost:8000/burst/session | jq             # se SIGNAL_STRATEGY=burst15
curl localhost:8000/retrain | jq                   # il ciclo di apprendimento
```

Se `/diagnostics` dice **NO MARKET DATA**, il problema non è nelle soglie: è la
connettività verso il venue. Nessuna modifica ai parametri produrrà segnali
finché quel punto non è risolto.

---

## 6. Cosa NON è stato dimostrato

Va detto chiaramente, perché è la differenza fra un motore che funziona e un
motore che guadagna:

* **nessun edge è stato dimostrato.** Le correzioni fanno sì che il motore
  emetta quando le sue regole lo prevedono e impari quando i dati lo
  consentono. Se quelle regole abbiano un vantaggio statistico su BTC è una
  domanda empirica, e gli strumenti per rispondere (`search` con correzione per
  test multipli, `backtest` walk-forward, `/shadow`) sono quelli già presenti
  nel progetto — inclusa la possibilità che la risposta sia «no»;
* **allentare un cancello non aumenta l'accuratezza**, aumenta solo il numero di
  segnali. `/shadow` misura se le finestre scartate da un cancello avrebbero
  vinto: è l'unica prova che dice se quel cancello protegge o costa;
* **il feed reale non è stato provato qui.** Questo container non raggiunge
  nessun exchange. Tutta la verifica è stata fatta su feed sintetico,
  PostgreSQL reale e le suite di test (271 backend + 67 frontend).
