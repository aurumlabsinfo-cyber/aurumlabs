# PASSAGGIO 1 — Analisi dello storico esistente

Questo documento riporta cosa e' stato trovato, cosa e' stato scartato e
perche', e da dove viene invece lo storico su cui AURUM EDGE DISCOVERY lavora.
E' scritto per essere verificabile: ogni affermazione si puo' ricontrollare con
il comando indicato accanto.

---

## 1. Cosa e' stato cercato

Sono stati cercati, nel progetto e in tutte le directory raggiungibili:

| Cosa | Dove | Esito |
|---|---|---|
| database SQLite (`*.db`, `*.sqlite`) | intero filesystem | **nessuno** |
| export compressi (`*.db.gz`, `*.tar.gz`) | intero filesystem | nessuno del progetto |
| log del motore (`*.log`) | progetto e home | nessuno |
| modelli salvati (`*.joblib`) | progetto | nessuno |
| segnali, trade, shadow decision | tabelle attese | nessuna riga |
| market tick, feature | tabelle attese | nessuna riga |
| stato research / challenger / champion | tabelle attese | nessuna riga |
| dati Bybit gia' raccolti | qualunque formato | **nessuno** |

Verifica:

```bash
python3 -m aurum_edge analizza                 # cerca in cwd e nella home
python3 -m aurum_edge analizza /percorso/extra # aggiunge altre cartelle
```

### Perche' non c'e' niente

Non e' un guasto ed e' bene capirne il motivo, perche' cambia cosa si puo'
fare. Il `.gitignore` del repository contiene:

```
*.db
aurum.db
aurum.db-wal
aurum.db-shm
```

Tutti gli archivi del vecchio Aurum erano quindi **locali alla macchina su cui
il motore girava** e non sono mai entrati nel repository. La verifica sulla
storia completa di git lo conferma:

```bash
git log --all --diff-filter=A --name-only --pretty=format: \
  | sort -u | grep -iE '\.(db|sqlite|csv|parquet|gz)$'
# nessun risultato: nessun archivio e' mai stato committato
```

Se sulla **tua** macchina esistono ancora quei file, il comando `analizza` li
trova, li apre in sola lettura e li giudica uno per uno. Questo documento
riporta il caso in cui non ci sono.

---

## 2. Cosa e' stato trovato invece: il codice

Il repository contiene due motori precedenti. Nessuno dei due e' riutilizzabile
come fonte di dati, ma il secondo lo e' come fonte di **metodo**.

### `backend/` — motore su BTCUSDT a 5 secondi

* **Mercato**: BTCUSDT, ma via **Binance e Coinbase**, non Bybit, e sul mercato
  a pronti, non sul perpetual.
* **Orizzonte**: cinque secondi.
* **Dipendenze**: PostgreSQL, SQLAlchemy, pandas, FastAPI.
* **Verdetto**: **scartato come dati, scartato come architettura.**

Il motivo non e' la qualita' del codice, che e' buona. E' che a cinque secondi
comandano il flusso ordini e la pressione del libro, e a trenta minuti comandano
struttura, posizionamento e derivati. Non sono lo stesso problema con una
costante diversa: sono due problemi con feature diverse, rumore diverso e
campione efficace diverso. Riusarne il dataset significherebbe addestrare su
un fenomeno per prevederne un altro.

Una cosa concreta e' stata invece ripresa da qui: la scelta di registrare le
decisioni **non prese** per poterle giudicare a posteriori
(`backend/app/ml/shadow.py`). E' l'idea da cui nasce la tabella
`edge_observations` del nuovo progetto.

### `aurum_signal/` — motore EUR/USD a 60 secondi

* **Mercato**: EUR/USD, opzioni binarie. Non e' crypto e non e' un perpetual.
* **Orizzonte**: sessanta secondi.
* **Verdetto**: **dati inutilizzabili, metodo ereditato.**

Il criterio economico stesso e' incompatibile: in un'opzione binaria con payout
0,80 il pareggio sta al 55,6% e l'ampiezza del movimento non conta, conta solo
il segno. In un perpetual conta quanto si muove, quanto ci mette e quanto va
contro prima. Un modello addestrato sul primo problema ottimizza la cosa
sbagliata per il secondo.

Quello che **e' stato ereditato**, riscritto per il nuovo problema:

| Da `aurum_signal` | Dove e' finito | Cosa e' cambiato |
|---|---|---|
| purged walk-forward + embargo | `research/validation.py` | tre classi invece di due; fold materializzati una volta sola e riusati da tutti i candidati |
| campionamento indipendente | `research/validation.py` | identico nello spirito, orizzonte 30 min |
| Benjamini-Hochberg | `util/numeric.py` | identico |
| holdout fresco | `research/validation.py` | aggiunta la purga anche al taglio dell'holdout |
| intervallo di Wilson | `util/numeric.py` | identico |
| calibrazione e curva di affidabilita' | `research/metrics.py` | aggiunti ECE, MCE e l'indicazione di quali fasce sono smentite |
| ciclo di vita a stadi | `research/lifecycle.py` | riferito a previsioni, non a ordini |

**Nessuna `confidence` storica e' stata importata.** Non c'erano archivi da cui
prenderle, ma anche se ci fossero stati la regola sarebbe la stessa: una
confidence prodotta da un vecchio modello e' l'uscita di quel modello, e usarla
come ingresso di uno nuovo significa ereditarne gli errori chiamandoli dati.
Se un giorno quegli archivi ricompaiono, il comando `analizza` li classifica
`SOLO DIAGNOSTICA` per questo motivo, e lo scrive.

---

## 3. Da dove viene lo storico nuovo

Poiche' non esisteva niente di riutilizzabile, lo storico e' ricostruito dagli
endpoint storici **pubblici** di Bybit, che e' comunque la fonte migliore
possibile: e' esattamente il mercato da prevedere, non un mercato simile.

```bash
python3 -m aurum_edge backfill --days 180
```

### Cosa si ricostruisce davvero

| Grandezza | Fonte | Passo | Profondita' |
|---|---|---|---|
| prezzo iniziale e a +5/10/15/20/30/45/60 min | `/v5/market/kline` | 1 min | anni |
| open, high, low, close | `/v5/market/kline` | 1 min | anni |
| volume in BTC e controvalore in USDT | `/v5/market/kline` | 1 min | anni |
| volatilita' realizzata, ATR, Bollinger | calcolate dalle barre | 1 min | quanto le barre |
| RSI, MACD, EMA, VWAP, momentum | calcolate dalle barre | 1 min | quanto le barre |
| open interest e sue variazioni | `/v5/market/open-interest` | 5 min | mesi |
| funding | `/v5/market/funding/history` | 8 ore | anni |
| rapporto conti long/short | `/v5/market/account-ratio` | 5 min | settimane |
| contesto ETH e SOL | `/v5/market/kline` | 1 min | anni |

Le etichette a tutti gli orizzonti richiesti si ricostruiscono esattamente,
perche' sono funzioni delle barre: da ogni istante si conosce il prezzo iniziale
e quello dopo 5, 10, 15, 20, 30, 45 e 60 minuti.

### Cosa NON si ricostruisce, e perche' va detto

| Grandezza | Perche' no |
|---|---|
| taker buy/sell, CVD | `/v5/market/recent-trade` restituisce solo gli ultimi ~1000 scambi. Non esiste un archivio pubblico degli scambi. |
| squilibrio del libro | Il libro e' una fotografia dell'istante. Non esiste storico. |
| liquidazioni | Bybit v5 le pubblica solo via WebSocket (`allLiquidation`), senza storico REST. |
| market breadth | Deriva dai ticker di un paniere in un istante: non ricostruibile all'indietro. |
| news | I feed RSS conservano solo le voci recenti. |

Queste cinque **restano vuote nello storico e si riempiono in avanti** mentre il
collector gira.

Le conseguenze pratiche, dette chiaramente:

1. gli edge di flusso ordini, CVD, libro, liquidazioni e news **non sono
   studiabili sui mesi passati**. Diventano studiabili dopo qualche settimana di
   raccolta;
2. il costruttore di dataset se ne accorge da solo: `drop_sparse` elimina le
   colonne sotto l'80% di copertura e scrive quali ha eliminato, che finisce
   nella dashboard sotto *Candidati provati*;
3. il catalogo dichiara quelle famiglie **non disponibili** con il motivo, invece
   di far finta che i loro edge siano stati provati e bocciati.

L'alternativa scartata era dedurre il flusso taker dal segno della barra. Non e'
stata scartata perche' imprecisa, ma perche' funziona: alza l'accuratezza in
backtest e sparisce in avanti, che e' la firma esatta di un artefatto.

---

## 4. Validazione temporale

Ogni scelta di questo capitolo esiste per rendere **piu' difficile** trovare un
vantaggio.

**Purga.** Fra la fine dell'addestramento e l'inizio del test si scartano almeno
30 minuti (l'orizzonte). Senza, l'ultima riga di addestramento ha un'etichetta
che vive dentro il periodo di test.

**Embargo.** Altri 30 minuti dopo la purga. Totale 60 minuti buttati a ogni
confine di fold. Il numero di righe purgate e' riportato per fold e una prova
automatica fallisce se e' zero.

**Campioni indipendenti.** Righe al minuto con etichette a trenta minuti si
sovrappongono per il 97%. Ogni misura di significativita' usa **solo** righe
distanziate di almeno un orizzonte. Il prezzo e' alto e va pagato:

| Periodo | Righe al minuto | Osservazioni indipendenti |
|---|---|---|
| 30 giorni | 43.200 | ~1.400 |
| 180 giorni | 259.200 | ~8.600 |

Sei mesi di storia sono ottomila osservazioni, non duecentomila. E' il numero
che determina cosa si riesce a vedere, ed e' mostrato in dashboard sotto
*Potenza statistica* insieme al vantaggio minimo rilevabile.

**Out-of-sample.** Un quarto finale dei dati e' tagliato via con la purga prima
di qualunque selezione, e guardato una volta sola alla fine.

**Test multipli.** Il catalogo genera oltre 400 candidati. Con 400 test e
soglia 0,05, venti passano per caso. Benjamini-Hochberg corregge; il numero di
candidati provati e' mostrato accanto ai sopravvissuti, perche' senza quel
numero il secondo non si puo' interpretare.

**Controllo della fuga di informazione.** Prima di guardare qualunque risultato,
si cerca una feature troppo correlata con l'etichetta. Se c'e', il giro si
**ferma** invece di produrre numeri.

### La prova che tutto questo funziona

Due prove automatiche, opposte, ed entrambe necessarie:

```bash
python3 -m aurum_edge selftest
```

1. **Cammino casuale** — dati generati senza alcuna direzione prevedibile. Il
   motore non deve promuovere niente. Misurato su 12 semi indipendenti:
   **0 falsi positivi su 12**, AUC direzionale media **0,511** (min 0,461, max
   0,550), vantaggio medio sulla base rate **−0,013**.

2. **Vantaggio piantato** — dati con dentro un regime di deriva vero. Il motore
   **deve** trovarlo, e lo trova: AUC direzionale 0,91–0,93, accuratezza
   direzionale 0,91–0,94, coerenza fra i fold 1,0.

Un validatore che boccia sempre e' inutile quanto uno che promuove sempre. Solo
la coppia distingue i due casi.

### Un difetto trovato costruendo queste prove

La prima versione usava come cancello l'**AUC uno-contro-tutti**. Le prove sul
cammino casuale l'hanno smentita: si assestava stabilmente intorno a **0,53**
dove per costruzione non c'era alcuna direzione da prevedere.

Il motivo: siccome il "resto" include FLAT, quella misura premia chi sa prevedere
la **volatilita'**. Sapere che sta per arrivare un movimento alza insieme
P(LONG) e P(SHORT) e spinge in fondo alla graduatoria i casi FLAT, e questo
basta a superare 0,5 senza sapere niente della direzione.

Il cancello e' ora l'**AUC direzionale**: solo i casi in cui il mercato si e'
mosso, ordinati per la quota relativa fra le due probabilita' direzionali. Il
canale della volatilita' e' chiuso. L'uno-contro-tutti resta come diagnostica,
etichettata per quello che e'.

Un secondo difetto, corretto allo stesso modo: l'accuratezza direzionale
contava come errore anche i casi finiti FLAT, ma veniva confrontata con una base
rate calcolata sui soli esiti direzionali. Due denominatori diversi, confronto
impossibile da vincere. Ora `accuracy` misura la direzione sui casi risolti e
`flat_rate` riporta a parte quanto spesso non e' successo niente.

---

## 5. Conclusione

**Dello storico del vecchio Aurum non e' stato riutilizzato nulla, perche' non
esisteva nulla di riutilizzabile su questa macchina, e perche' anche se fosse
esistito riguardava altri mercati e altri orizzonti.**

Quello che e' stato ereditato e' il metodo di validazione, che era la parte
migliore dei progetti precedenti, riscritto per un problema a tre classi su
trenta minuti.

Lo storico nuovo viene dagli endpoint pubblici di Bybit sullo stesso identico
contratto da prevedere, con l'elenco esplicito di cosa non e' ricostruibile e
cosa questo comporta.
