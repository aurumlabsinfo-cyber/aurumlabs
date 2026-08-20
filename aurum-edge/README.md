# AURUM EDGE 1.0

Sistema di trading multi-crypto collegato **esclusivamente a Bybit V5**, in tre
blocchi e un solo ciclo. Backend e frontend sono processi separati; il backend
funziona con il frontend spento.

```
1. SCAN            Bybit -> mercato sincronizzato -> migliori opportunità
2. DECIDE          LONG / SHORT / NO TRADE -> size -> leva -> rischio -> target
3. EXECUTE + LEARN ordine -> fill -> gestione -> uscita -> registrazione -> miglioramento
```

Non ci sono agenti che si parlano. RSI, order flow, volume, regime non sono
servizi: sono campi dello stesso `MarketSnapshot`. Un solo Decision Core produce
una risposta, un solo Execution Core la porta a termine.

---

## Avvio rapido

```bash
cd aurum-edge/backend
pip install -r requirements.txt

# 1. il sistema funziona davvero? (offline, ~90s, nessuna rete)
python -m aurum_edge selftest

# 2. Bybit è davvero raggiungibile da questa macchina?
python -m aurum_edge test-bybit

# 3. PAPER (default). LIVE va abilitato esplicitamente.
python -m aurum_edge run
```

Dashboard, in un altro terminale — è un processo separato:

```bash
cd aurum-edge/frontend
python3 serve.py --api http://127.0.0.1:8100
# poi apri http://127.0.0.1:8200/?api=http://127.0.0.1:8100
```

Il backend continua a operare e a registrare anche se la dashboard non è mai
stata aperta.

---

## Comandi

| comando | cosa fa |
|---|---|
| `run` | avvia trading core + API. `--simulate` usa un feed sintetico **etichettato `fake`** (solo PAPER); `--i-understand-live` è obbligatorio per LIVE |
| `selftest` | prova l'intera pipeline offline su feed simulato e stampa 14 verifiche |
| `test-bybit` | verifica la connessione reale: REST, universo, book, trades, tickers, latenza, private WS |
| `diagnose` | interroga il backend in esecuzione e spiega **perché** non sta operando |
| `reconcile` | riconciliazione una tantum contro Bybit |
| `db` | path assoluto, schema version, migrazioni, conteggi per tabella |
| `stats` | WR, average win/loss, expectancy, fee, slippage, net, drawdown su tutti i run |
| `learn` | esegue una volta la pipeline champion/challenger |

---

## 1. SCAN

Un solo Market Core possiede la connessione a Bybit e lo stato di ogni simbolo.

* **public WS** per i perpetual USDT, **private WS** per wallet/ordini/execution/
  posizioni, **REST** solo dove serve (strumenti, filtro liquidità, ordini,
  riconciliazione).
* riconnessione automatica con backoff, heartbeat (`ping`/`pong`), resubscribe
  del set di topic, e **stale-feed detection**: un socket aperto ma silenzioso
  viene abbattuto. La freschezza si misura sui messaggi *dati*, non sui pong —
  altrimenti una connessione che risponde ai ping mentre il feed è morto sembra
  viva.
* **layout dei feed**: tutto l'universo riceve `tickers` + `publicTrade` +
  top-of-book; il *focus set* (i simboli più attivi, ricalcolato di continuo)
  viene promosso a `orderbook.50`. **Solo i simboli con book completo sono
  tradabili**: un book sottile può classificare un'opportunità, mai riempirla.

Ogni simbolo produce un `MarketSnapshot` immutabile con bid/ask, spread, mid,
last, microprice, trades, volume e accelerazione volume, variazioni a
250ms/1s/3s/5s/15s/60s, volatilità, imbalance (top e profondità), OFI,
aggressione buy/sell, open interest, timestamp Bybit, timestamp locale, latenza,
stato del book, data quality, **e la curva di liquidità** (notional disponibile
entro N bps dal mid). Quest'ultima è ciò che permette di stimare lo slippage
dallo snapshot stesso, senza rileggere un book che nel frattempo si è mosso.

Tutti i dati di una decisione appartengono a quello snapshot. Lo snapshot è
congelato (`frozen dataclass`): non può cambiare sotto la decisione.

### Errori dei vecchi programmi, corretti per costruzione

| errore | come è impedito |
|---|---|
| feed simulato spacciato per reale | ogni snapshot porta `source`; LIVE rifiuta di partire se `source != "bybit"`; la dashboard mostra `FEED: FAKE` in giallo |
| database diversi tra backend e frontend | un solo path assoluto, scritto in `schema_meta`, esposto su `/api/state`, stampato nel footer della dashboard |
| book desincronizzato usato per un trade | `u` deve incrementare di 1; un gap svuota il book e passa a `RESYNC`; `RESYNC`/`CROSSED`/`STALE` ⇒ `tradable=False` |
| dati vecchi usati come live | età del book nello snapshot, gate di staleness, e la dashboard si oscura e lo dichiara se il backend tace |
| "0 segnali" senza spiegazione | ogni decisione ha un `reason`; i rifiuti sono contati per motivo; `diagnose` risponde sempre |
| trade persi per errori di schema SQLite | schema validato all'avvio (rifiuta di partire se è derivato), scritture fallite finiscono in `write_failures` con il payload e degradano la salute |
| ACK confuso con fill | la posizione esiste solo dopo gli execution event; l'ACK scrive solo `status='ACK'` |

---

## 2. DECIDE

Un solo Decision Core. Ordine dei controlli:

```
qualità dei dati -> limiti di portafoglio -> modello -> sizing -> economics -> risposta
```

Per ogni candidato produce LONG / SHORT / NO TRADE **e** qualità, probabilità,
movimento residuo atteso, costo previsto, margine, leva, nozionale, target,
perdita massima, tempo massimo in posizione, expectancy.

**Capitale.** Riserva sempre una quota dell'equity (default 35%); più posizioni
contemporanee (default max 6); margine tipico €40–60 quando equity e rischio lo
permettono; il capitale chiuso torna subito disponibile per l'opportunità
successiva.

**Leva.** 10x di progetto, ridotta dal risk engine quando serve, **mai alzata**
per raggiungere un profitto. Il caso peggiore considerato è stop **più** il
costo del giro completo: se non entra nel cap di perdita, prima scende la leva,
poi il margine, e se ancora non basta il trade non si fa.

**Economics.** Con fee taker 0,055% per lato il giro costa ~11 bps, più lo
slippage. Su €500 di nozionale servono **~50 bps** di movimento per fare €2
netti. Il sistema non aspetta esattamente €2: prende il target più piccolo tra
"€2" e "80% del movimento atteso", non scende mai sotto il break-even × 1,2, e
lascia correre con trailing se il movimento continua. Il gate vero è
l'**expectancy**:

```
E = p × (target − costi) − (1 − p) × (stop + costi)
```

Se `E` è sotto la soglia, è NO TRADE — anche con probabilità alta. Questo rende
il sistema **selettivo**: è una proprietà, non un difetto.

**Uscita.** La posizione vive solo mentre le condizioni che l'hanno generata
reggono: stop, edge svanito (la probabilità sullo stesso lato scende sotto
soglia), tempo massimo, target con trailing, salute degradata, kill switch. Non
esiste "aspetta che torni positivo". Un vincitore ancora spinto dal flusso può
guadagnare una proroga del tempo massimo, una volta sola e mai oltre il cap
globale.

Nessun win rate obbligatorio, nessun limite di 500 trade, nessun tetto
giornaliero artificiale. Gli unici limiti sono di rischio.

---

## 3. EXECUTE + LEARN

```
MarketSnapshot -> Decision -> OrderIntent -> Bybit -> ACK -> Execution -> Fill -> Position Confirmed
```

`orderLinkId` è deterministico (`hash(run, posizione, scopo, tentativo)`): dopo
un reconnect una risubmissione viene rifiutata da Bybit come duplicata invece di
aprire una seconda posizione, e quel rifiuto viene trattato come successo.

Dopo ogni reconnect: **entrate bloccate** → wallet → ordini aperti → executions
→ posizioni → diff con lo stato locale → riparazione → `LIVE_READY`. Le
posizioni presenti su Bybit ma sconosciute in locale vengono adottate; quelle
locali che sull'exchange non esistono più vengono chiuse dalle execution
recuperate, o registrate al prezzo di riferimento dichiarando che il prezzo di
uscita è ignoto.

### Contabilità

```
net = gross(prezzi di riferimento) − slippage(vs riferimento) − fee(reali)
```

L'identità è esatta per costruzione e verificata su ogni trade nei test
(residuo < 1e-9 €). `gross` usa il mid al momento della decisione e al momento
dell'uscita; lo slippage è la differenza tra quei riferimenti e i fill reali.

Per ogni trade sono salvati: snapshot d'entrata, motivo d'entrata, previsione,
size, leva, costo previsto, prezzo reale di fill, fee reali, slippage,
evoluzione della posizione, motivo d'uscita, risultato netto.

### Apprendimento

```
dati storici -> purged walk-forward -> holdout intatto -> shadow -> paper
```

* Il **Champion non viene mai modificato**: l'addestramento scrive una nuova
  riga; la promozione è un cambio di stato con la versione precedente conservata
  per il rollback immediato.
* L'**holdout** è la fetta più recente dei dati e non entra né nel fit né nella
  scelta degli iperparametri. I fold sono **purgati**: i campioni entro
  `purge_seconds` dal confine vengono scartati.
* Si impara **anche dai NO TRADE**: ogni decisione viene etichettata a posteriori
  con quello che il mercato ha fatto davvero (movimento a orizzonte, al netto dei
  costi stimati allora). Anche le shadow decisions del challenger.
* Un WR più alto **non promuove nulla**. Contano profitto netto fuori campione,
  expectancy, drawdown, average loss, AUC. Un challenger che vince più spesso ma
  guadagna meno viene rifiutato (c'è un test apposta).
* La promozione richiede in più evidenza **live in shadow**: almeno N decisioni
  ombra con expectancy superiore a quella del champion sulle stesse condizioni.

**Se il Champion perde performance**, il sistema non aspetta il limite di perdita
giornaliero. Ogni 30 secondi rivaluta l'expectancy sulle ultime 25 operazioni di
quella versione:

| expectancy recente | conseguenza |
|---|---|
| positiva | size piena |
| ≤ 0 | **size dimezzata** (anche la soglia di expectancy scala, altrimento "metà size" diventerebbe "nessun trade") |
| ≤ −0,25 € | **nuove entrate sospese** |

Le posizioni aperte restano gestite, la pipeline continua a girare sui dati già
raccolti, e la size piena torna da sola quando i numeri tornano — o subito, se un
challenger viene promosso. Ogni passaggio è registrato in `model_events` e
visibile in dashboard.

Le performance sono studiate **per simbolo** (`symbol_stats`, in `/api/stats`) e
**per regime di volatilità** (calmo / normale / veloce / selvaggio, misurato sulla
volatilità del simbolo stesso all'entrata, in `/api/state` e in dashboard).

### Strategia Champion iniziale

`champion-1.0.0-momentum-of`: momentum + order-flow continuation, scritta a mano
e leggibile. Cerca movimenti **già iniziati** e verifica che il flusso li stia
ancora spingendo: accelerazione di prezzo su più orizzonti, volume in crescita,
aggressione coerente, OFI, imbalance, microprice, spread, liquidità, volatilità,
e movimento residuo sufficiente a coprire i costi. Non prova a prevedere grandi
movimenti futuri. Ogni versione successiva è imparata dai dati che questa
raccoglie.

---

## Configurazione

Tutto da environment (o da un `.env`, vedi `.env.example`). I principali:

| variabile | default | significato |
|---|---|---|
| `AURUM_MODE` | `paper` | `paper` o `live` |
| `BYBIT_API_KEY` / `BYBIT_API_SECRET` | — | necessarie per LIVE e per il private stream |
| `BYBIT_TESTNET` | `false` | usa gli endpoint testnet |
| `AURUM_DB_PATH` | `~/.aurum-edge/aurum_edge.sqlite3` | l'unico database, path assoluto |
| `AURUM_API_PORT` | `8100` | API + websocket per la dashboard |
| `AURUM_MARGIN_MIN_EUR` / `MAX` | `40` / `60` | banda di margine per posizione |
| `AURUM_LEVERAGE` | `10` | leva di progetto (massima, non obiettivo) |
| `AURUM_RESERVE_FRACTION` | `0.35` | quota di equity mai impegnata |
| `AURUM_MAX_POSITIONS` | `6` | posizioni contemporanee |
| `AURUM_TARGET_NET_EUR` | `2.0` | riferimento economico per buon trade |
| `AURUM_MAX_LOSS_PER_TRADE_EUR` | `2.0` | caso peggiore per trade, stop + costi |
| `AURUM_DAILY_MAX_LOSS_EUR` | `40` | stop giornaliero |
| `AURUM_TAKER_FEE` | `0.00055` | **allineala al tuo tier VIP**: cambia l'economia |
| `AURUM_KILL_SWITCH_FILE` | — | se il file esiste, niente nuove entrate |
| `AURUM_LOG_ALL_NO_TRADE` | `false` | registra ogni singola valutazione, non solo i candidati |

### LIVE

1. `python -m aurum_edge test-bybit` deve passare **tutto**, credenziali incluse;
2. `AURUM_MODE=live python -m aurum_edge run --i-understand-live`;
3. kill switch: bottone in dashboard, `POST /api/control/kill`, o creare il file
   indicato da `AURUM_KILL_SWITCH_FILE`.

LIVE si rifiuta di partire senza credenziali, senza il flag esplicito, o su un
feed che non sia Bybit reale.

---

## Cosa dimostrano i test

`python -m pytest` — 120 test offline, ~100 s, nessuna rete.

| affermazione | dove |
|---|---|
| il client Bybit parla il protocollo giusto (firma REST, batch da 10, auth WS) | `test_transport.py` |
| reconnect: risottoscrive e avvisa il motore | `test_transport.py`, `test_market_core.py` |
| un socket aperto ma silenzioso viene rilevato e abbattuto | `test_transport.py` |
| un gap di sequenza rende il simbolo non tradabile | `test_book_and_snapshot.py`, `test_market_core.py` |
| lo snapshot è una vista sincronizzata e le feature sono corrette | `test_book_and_snapshot.py` |
| i segnali vengono prodotti, e ogni NO TRADE ha un motivo | `test_decision.py` |
| leva mai alzata per il target, perdita sempre dentro il cap | `test_decision.py` |
| **un ACK non apre una posizione** | `test_execution.py` |
| execution duplicate dopo un reconnect non raddoppiano nulla | `test_execution.py` |
| costi e P&L quadrano (`gross − slippage − fee == net`) | `test_execution.py`, `test_end_to_end.py` |
| PAPER apre e chiude operazioni contro il book reale | `test_execution.py`, `test_end_to_end.py` |
| riconciliazione: adotta, chiude gli orfani, recupera i fill persi | `test_reconcile.py` |
| il database registra tutto e non perde righe in silenzio | `test_storage.py`, `test_end_to_end.py` |
| il learning crea Challenger senza toccare il Champion | `test_learning.py` |
| un WR più alto da solo non promuove | `test_learning.py` |
| un Champion che perde riduce e poi sospende le entrate, e si riprende da solo | `test_learning.py` |
| il frontend mostra esattamente lo stato del backend | `test_api_and_frontend.py` |
| la dashboard rende davvero, in Chromium | `test_dashboard_browser.py` |
| una dashboard senza backend si oscura e lo dichiara | `test_dashboard_browser.py` |
| il backend gira senza frontend | `test_api_and_frontend.py` |
| `diagnose` risponde sempre | `test_api_and_frontend.py`, `test_end_to_end.py` |
| LIVE rifiuta un feed simulato | `test_end_to_end.py` |

Il contratto con la dashboard è verificato leggendo `REQUIRED_STATE_PATHS` da
`frontend/app.js` e controllando che il backend produca ogni campo: se il
frontend legge qualcosa che il backend non manda, il test fallisce.

## Cosa i test **non** dimostrano

* **Che Bybit sia connesso.** Nessun test offline può dimostrarlo, e la suite
  offline non deve mai essere presentata come prova. La prova è
  `python -m aurum_edge test-bybit` (o `AURUM_LIVE_TESTS=1 pytest
  tests/test_live_bybit.py`) su una macchina che raggiunge `api.bybit.com`.
* **Che la strategia guadagni.** I numeri del `selftest` vengono da un mercato
  sintetico e non sono un edge. L'edge si misura solo su dati reali, in PAPER,
  abbastanza a lungo.

---

## Struttura

```
backend/aurum_edge/
  config.py          tutti i parametri, in un posto solo
  engine.py          il ciclo: scan -> decide -> execute; nessun import dell'API
  health.py          stato dei componenti e gate di trading
  simulator.py       feed sintetico etichettato "fake" (selftest e test)
  cli.py             run, selftest, test-bybit, diagnose, reconcile, db, stats, learn
  scan/              bybit_rest, bybit_ws, book, snapshot, market_core, scanner
  decide/            model (champion/challenger), risk, decision_core
  execute/           broker (+paper), bybit_broker, execution_core, reconcile
  learn/             pipeline (walk-forward purgato, holdout, gate, rollback)
  storage/           schema (migrazioni), db (dead letter), repo (statistiche)
frontend/            index.html, app.js, styles.css, serve.py  (nessuna dipendenza)
```

Nessun microservizio, nessun agente. Tre blocchi.
