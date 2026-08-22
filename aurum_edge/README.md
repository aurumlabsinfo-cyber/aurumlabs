# AURUM EDGE DISCOVERY

Strumento di **previsione manuale** per BTCUSDT Perpetual su Bybit.

Risponde a una domanda sola: *nei prossimi 30 minuti il prezzo ha una direzione
prevedibile, e con quale probabilita', entro quale intervallo, per quanto tempo,
e a quale prezzo la tesi e' sbagliata?*

La risposta piu' frequente e', per progetto, **WAIT**.

---

## Cosa NON fa

Non e' una configurazione da spegnere. Non esiste il codice per farlo.

* non apre ordini, non chiude ordini, non modifica ordini
* non si collega a un wallet, non conosce chiavi private
* non usa capitale, non usa leva, non gestisce stop loss
* non fa trading automatico

Il client HTTP espone `get` e nient'altro: non c'e' una funzione `post` in tutto
il pacchetto. Il server web risponde **405** a qualunque metodo di scrittura. Una
prova automatica scansiona il codice eseguibile — non i commenti — alla ricerca
di endpoint ordini, firme HMAC, chiavi API, chiamate di esecuzione e leva, e
fallisce se ne trova una.

```bash
python3 -m aurum_edge selftest    # gruppo 3: SICUREZZA
```

---

## Cosa produce

| Campo | Da dove viene |
|---|---|
| **LONG / SHORT / WAIT** | argmax delle probabilita', ma solo se supera tutti i cancelli |
| probabilita' LONG / SHORT / FLAT | modello logistico a tre classi, calibrato in temperatura |
| prezzo attuale | ticker live se fresco, altrimenti ultima barra chiusa — la dashboard dice quale |
| target price range | quantili 25–75 dei movimenti storici in regime analogo, riscalati sulla volatilita' di adesso |
| expected move % | mediana degli stessi analoghi |
| durata prevista | tempo mediano al massimo movimento favorevole negli analoghi |
| invalidation level | il piu' vicino fra minimo/massimo a 60 min e 1,5 ATR |
| signal quality | freschezza, copertura feature, regime, validita' del modello, spread, storia |
| regime di mercato | tendenza × volatilita', piu' l'avvertimento di compressione |
| motivazioni | contributi del modello, edge attivi, regime, analoghi |

Quando gli analoghi in quel regime sono meno di trenta, **il target non viene
mostrato**: non c'e' base per stimarlo, e un intervallo inventato e' peggio di
nessun intervallo perche' sembra una misura.

---

## Avvio rapido

Serve solo Python 3.11 o superiore. **Nessuna dipendenza da installare**: niente
numpy, niente pandas, niente scikit-learn, niente FastAPI. Tutto libreria
standard.

```bash
cd /percorso/del/repository

# PASSAGGIO 1 — cosa c'e' gia' e cosa serve
python3 -m aurum_edge analizza

# ricostruzione storica (~10-25 min per 180 giorni)
python3 -m aurum_edge backfill --days 180

# primo giro di ricerca (~3-10 min su 180 giorni)
python3 -m aurum_edge ricerca

# tutto insieme: raccolta + ricerca + dashboard
python3 -m aurum_edge serve
# -> http://127.0.0.1:8090/
```

Una previsione singola dal terminale:

```bash
python3 -m aurum_edge previsione
python3 -m aurum_edge previsione --json | python3 -m json.tool
```

### Su Ubuntu, come servizio

```ini
# /etc/systemd/system/aurum-edge.service
[Unit]
Description=Aurum Edge Discovery
After=network-online.target

[Service]
Type=simple
User=aurum
WorkingDirectory=/opt/aurum
Environment=AURUM_DB=/opt/aurum/aurum_edge.db
Environment=AURUM_HOST=127.0.0.1
Environment=AURUM_PORT=8090
ExecStart=/usr/bin/python3 -m aurum_edge serve
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now aurum-edge
journalctl -u aurum-edge -f
```

---

## I comandi

| Comando | Cosa fa |
|---|---|
| `analizza [percorsi]` | cerca database, export, log e modelli vecchi; li apre in sola lettura e dice se servono |
| `backfill --days N` | ricostruisce barre, open interest, funding, long/short e contesto ETH/SOL |
| `collect [--once]` | raccolta live: ticker, libro, scambi, breadth, news |
| `ricerca [--loop]` | un giro di ricerca, o in ciclo |
| `previsione [--json]` | la previsione corrente |
| `serve` | raccolta + ricerca + dashboard insieme |
| `stato` | cosa c'e' nell'archivio |
| `edges [--state X]` | tutti gli edge e i loro stati |
| `selftest [--quick]` | prove offline, senza rete |

---

## Le tre cose che rendono il progetto quello che e'

### 1. Un solo percorso per storico e per live

La riga che il modello vede in addestramento e quella che vede in produzione le
produce **la stessa funzione**: `Frame.row(i)`. Il percorso live e' il percorso
storico fermato all'ultimo indice.

Il motivo e' che la divergenza fra addestramento e produzione non si manifesta
come un errore, si manifesta come un modello che funzionava benissimo ieri. Una
prova lo verifica in modo diretto: costruisce le feature su N barre, poi su N+500,
e confronta le stesse righe. Se un solo valore cambia, quella feature guardava il
futuro.

```
1. CAUSALITA' — nessuna feature guarda il futuro
  OK   le feature non cambiano quando arrivano barre future
  OK   il regime non cambia retroattivamente
  OK   solo builder e forecast leggono gli esiti futuri
```

### 2. Validazione costruita per non trovare nulla

Cinque difese, e ognuna esiste per rendere **piu' difficile** dichiarare un
vantaggio:

1. **purga** di 30 minuti fra addestramento e test (un orizzonte);
2. **embargo** di altri 30 minuti;
3. **campioni indipendenti**: 259.000 righe al minuto sono ~8.600 osservazioni,
   non 259.000 — misurare sulle finestre sovrapposte restringe gli intervalli di
   confidenza di un fattore cinque;
4. **holdout fresco** del 25%, tagliato con la purga prima di ogni selezione,
   guardato una volta sola;
5. **Benjamini-Hochberg** su tutti i candidati provati: con 208 pattern e soglia
   0,05, dieci passano per caso.

E la prova che serve davvero: **il motore viene messo alla prova su dati senza
alcun vantaggio.** Su 12 cammini casuali indipendenti:

```
FALSI POSITIVI: 0/12
lift medio: -0.0128  (min -0.0692  max +0.0505)
AUC direzionale media: 0.5110  (min 0.4614  max 0.5495)
```

E su dati con un vantaggio piantato dentro, **lo trova**: AUC direzionale
0,91–0,93, coerenza fra i fold 1,0. Servono entrambe le prove — un validatore che
boccia sempre e' inutile quanto uno che promuove sempre.

### 3. Ogni previsione viene giudicata dopo

Le previsioni si scrivono in `forecasts` **prima** di conoscerne l'esito;
`score_pending` lo attacca a scadenza, con lo stesso metro delle etichette
storiche. Nessuna previsione viene rimossa perche' e' andata male. La dashboard
mostra questa accuratezza dal vivo con il suo intervallo di confidenza, sotto
*Previsioni giudicate dal vivo*.

---

## Il ciclo di vita di un edge

```
SCOPERTO -> IN_VALIDAZIONE -> IN_OMBRA -> VALIDATO -> IN_DECADIMENTO
                  |               |           |
                  +--> RIFIUTATO <+-----------+
```

Un edge avanza **di uno stato per volta**, mai due. Superare storico e holdout
lo porta IN_OMBRA, non VALIDATO: manca il pezzo che nessun dato storico puo'
dare, cioe' aver funzionato su previsioni fatte prima di conoscerne l'esito.

L'ombra richiede **60 osservazioni risolte E almeno 24 ore**. Entrambe: sessanta
osservazioni in due ore vengono tutte dalla stessa condizione di mercato.

Gli edge in ombra vengono registrati e misurati ma **non influenzano il numero
mostrato**. E' la differenza fra studiare e usare.

### Le famiglie di pattern

Volume breakout · order-flow continuation · CVD e assorbimento · OI acceleration ·
OI × prezzo · VWAP continuazione/rientro · compressione ed espansione di
volatilita' · estremi di funding e base · sentiment long/short · lead-lag
BTC/ETH/SOL · reazione alle news · e le combinazioni a coppie fra famiglie
diverse.

Le **liquidazioni** sono dichiarate non disponibili, non simulate: Bybit v5 le
pubblica solo via WebSocket e non ne conserva lo storico. Dedurle dalle candele
sarebbe inventarle.

### Come si misura un edge, e perche' non come un modello

Un edge parla solo quando le sue condizioni valgono. Giudicarlo anche sulle
righe in cui tace diluisce il risultato in migliaia di non-risposte; ma
restringere il campione alle sole righe in cui parla restringe con esso anche la
base rate, e il confronto diventa impossibile da vincere per costruzione.

La soluzione: **accuratezza dove parla, base rate dell'intero periodo**. La
domanda diventa quella giusta — *i momenti che questo edge sceglie sono piu'
prevedibili della media?*

Per la stessa ragione, tre cancelli validi per un modello sono spenti per un
edge, perche' sarebbero matematicamente impossibili da superare: accuratezza
bilanciata (un predittore costante prende 1/3 sempre), AUC (probabilita'
costanti non ordinano niente) e chiamate nei due versi (un edge afferma una
direzione sola). Restano attivi quelli che contano: campione, vantaggio sulla
base rate generale, limite inferiore dell'intervallo, coerenza fra fold,
calibrazione.

### La direzione la decidono i dati

Una versione precedente generava ogni condizione due volte, LONG e SHORT. Era
sbagliata: le due varianti producevano previsioni **identiche** — la probabilita'
dichiarata viene dalla frequenza osservata nel training, non dalla direzione
scritta nella regola — e raddoppiavano il costo statistico senza aggiungere
informazione. Ora la direzione la determina `fit_edge` dalla classe direzionale
prevalente fra le attivazioni del periodo di addestramento. E' anche piu' onesto:
non si presuppone che il volume alto significhi rialzo, lo si misura.

---

## Cosa NON si puo' studiare, e perche' e' detto

| Grandezza | Perche' |
|---|---|
| taker buy/sell, CVD | `/v5/market/recent-trade` da' solo gli ultimi ~1000 scambi |
| squilibrio del libro | il libro e' una fotografia dell'istante |
| liquidazioni | solo WebSocket su Bybit v5, nessuno storico |
| market breadth | deriva dai ticker di un paniere in un istante |
| news | i feed RSS conservano solo le voci recenti |

Queste **si riempiono in avanti** mentre il collector gira. Fino a quel momento
`drop_sparse` elimina le colonne sotto l'80% di copertura e il catalogo dichiara
quelle famiglie non disponibili con il motivo, invece di far finta che i loro
edge siano stati provati e bocciati.

L'alternativa scartata era dedurre il flusso taker dal segno della barra. Non e'
stata scartata perche' imprecisa, ma perche' *funziona*: alza l'accuratezza in
backtest e sparisce in avanti — la firma esatta di un artefatto.

---

## Il cancello della decisione

Perche' esca una direzione servono, **tutte insieme**:

* probabilita' massima ≥ 58%
* margine sulla direzione opposta ≥ 10 punti
* FLAT non e' lo scenario piu' probabile
* qualita' del segnale ≥ 0,55
* un modello che ha superato l'holdout **oppure** un edge validato attivo adesso

Se manca anche uno solo: **WAIT**, con scritto quale mancava. Un WAIT spiegato e'
un'informazione; una direzione non spiegata e' una scommessa con una faccia
sicura.

La qualita' del segnale si combina con il **minimo pesato**, non con la media:
dati fermi da mezz'ora non si compensano con un regime chiarissimo.

---

## Struttura

```
aurum_edge/
├── config.py              soglie e parametri, ognuno con il perche'
├── util/                  http (solo GET), tempo (ms UTC), statistica a mano
├── data/
│   ├── bybit.py           endpoint pubblici v5, sola lettura
│   ├── store.py           SQLite: previsioni scritte prima, giudicate dopo
│   ├── backfill.py        ricostruzione storica
│   ├── collector.py       polling live, deduplica scambi, accumula CVD
│   └── legacy.py          PASSAGGIO 1: scansione e giudizio degli archivi vecchi
├── features/
│   ├── rolling.py         serie scorrevoli in una passata, allineate
│   ├── indicators.py      RSI, MACD, EMA, VWAP, ATR, Bollinger, momentum
│   ├── labels.py          l'UNICO file che guarda il futuro
│   ├── builder.py         un solo percorso per storico e live
│   └── dataset.py         righe + timestamp + copertura
├── model/logistic.py      logistica multinomiale, AdaGrad, calibrata
├── research/
│   ├── validation.py      purga, embargo, holdout, BH, potenza
│   ├── metrics.py         AUC direzionale, Brier, ECE, MFE/MAE
│   ├── edges.py           catalogo dei pattern
│   ├── lifecycle.py       gli stati e le loro condizioni
│   └── engine.py          il giro completo
├── forecast/
│   ├── engine.py          il prodotto: previsione + giudizio a scadenza
│   ├── regime.py          due assi, sei stati
│   └── quality.py         quanto fidarsi di QUESTA previsione
├── news/feeds.py          RSS, lessico dichiarato per quello che e'
├── web/                   server sola lettura + dashboard
└── tests/selftest.py      103 prove offline
```

---

## Configurazione

Tutto per variabile d'ambiente; i default stanno in `config.py` con il motivo
accanto.

| Variabile | Default | Cosa cambia |
|---|---|---|
| `AURUM_DB` | `aurum_edge.db` | percorso dell'archivio |
| `AURUM_PORT` | `8090` | porta della dashboard |
| `AURUM_SYMBOL` | `BTCUSDT` | contratto da prevedere |
| `AURUM_MIN_PROB` | `0.58` | probabilita' minima per una direzione |
| `AURUM_MIN_MARGIN` | `0.10` | margine minimo sulla direzione opposta |
| `AURUM_MIN_QUALITY` | `0.55` | qualita' minima del segnale |
| `AURUM_MIN_INDEP` | `120` | osservazioni indipendenti minime per promuovere |
| `AURUM_FDR_ALPHA` | `0.10` | soglia della correzione per test multipli |
| `AURUM_BACKFILL_DAYS` | `180` | giorni da ricostruire |
| `AURUM_POLL_SECONDS` | `20` | passo della raccolta live |
| `AURUM_RESEARCH_INTERVAL` | `1800` | secondi fra due giri di ricerca |

Abbassare `AURUM_MIN_INDEP` o alzare `AURUM_FDR_ALPHA` fa comparire piu' edge.
Non e' un modo di trovarne di piu': e' un modo di abbassare l'asticella, e vale
la pena saperlo prima di farlo.

---

## API

Tutte in sola lettura.

| Rotta | Contenuto |
|---|---|
| `GET /` | la dashboard |
| `GET /api/forecast` | previsione corrente, pannelli compresi |
| `GET /api/research` | stato della ricerca, edge, campione, potenza |
| `GET /api/status` | archivio, collector, freschezza, soglie |
| `GET /api/history` | previsioni passate con esito |
| `GET /api/edges` | tutti gli edge, stati e osservazioni |
| `GET /api/news` | flusso di notizie recente |
| `GET /healthz` | vivo o no |

Qualunque `POST`, `PUT`, `DELETE`, `PATCH` → **405**.

---

## Limiti, detti prima

* **Trenta minuti su BTC sono in gran parte rumore.** Il sistema e' costruito per
  ammetterlo. Se dice sempre WAIT per giorni, probabilmente sta funzionando.
* **Le probabilita' valgono quanto la loro calibrazione.** L'ECE e' mostrato in
  dashboard; sopra 0,10 il modello non viene promosso.
* **Sei mesi di storia sono ~8.600 osservazioni indipendenti**, non 259.000. Con
  quel campione si distingue dal caso un vantaggio di circa il 2%. Un vantaggio
  reale piu' piccolo esiste ma resta invisibile, e la dashboard lo scrive.
* **Il lessico delle news conta parole, non capisce.** La quantita' di notizie e'
  un dato affidabile; il loro verso e' un'indicazione grossolana.
* **Il flusso ordini parte da zero.** Gli edge di CVD e libro diventano studiabili
  solo dopo qualche settimana di raccolta.
* **Un edge validato puo' smettere di funzionare.** Per questo esiste
  IN_DECADIMENTO, e per questo il confronto e' con se' stesso e non con il caso.

---

## Il principio

> **Meglio WAIT che una previsione falsa.**

Una previsione sbagliata costa una volta. Un sistema che sembra affidabile e non
lo e' costa ogni volta che gli si crede.
