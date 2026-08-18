# AURUM SIGNAL ENGINE M60

Analisi e segnali su **EUR/USD**, opzioni binarie a **60 secondi**, con almeno
**30 secondi di anticipo** sull'entrata.

Il sistema **non invia ordini**. Non si collega a Pocket Option ne' a nessun
altro broker per operare, non usa automazione del browser, non preme pulsanti,
non tocca il tuo conto. Analizza, prevede, seleziona, notifica, studia, impara
e misura. L'operazione la decidi ed esegui tu.

Questa proprieta' non e' un'opzione di configurazione: un caso del selftest
analizza tutti i sorgenti a ogni esecuzione e fallisce se compare una chiamata
d'ordine o un'automazione del browser.

---

## 1. Installazione su Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv git

cd ~
git clone <url-del-repository> aurum
cd aurum/aurum_signal

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Le dipendenze sono **facoltative**: il sistema gira con la sola libreria
standard di Python 3.10+. Senza `fastapi`/`uvicorn` parte un server della
libreria standard con le stesse rotte; senza `numpy`/`scikit-learn` usa la
regressione logistica scritta a mano nel progetto.

Un dettaglio che conta: installa `uvicorn[standard]`, non `uvicorn`. Da solo
uvicorn **non parla WebSocket** e risponde 404 su `/ws`; la pagina continua a
funzionare interrogando il motore, ma perde gli aggiornamenti immediati. Il
sistema te lo dice all'avvio invece di lasciartelo scoprire.

### Configurazione

```bash
cp .env.example .env
nano .env          # inserisci payout e, se le hai, le chiavi gratuite
```

Nessuna chiave e' scritta nel codice. Le variabili gia' presenti
nell'ambiente hanno la precedenza sul file.

| Variabile | Cosa fa | Serve? |
|---|---|---|
| `PAYOUT` | Il payout del tuo broker. Determina il pareggio. | **Sì** |
| `TWELVEDATA_API_KEY` | Quotazioni EUR/USD reali (livello gratuito) | Per il LIVE |
| `FINNHUB_API_KEY` | Riserva, manda il timestamp della quotazione | Consigliata |
| `VIRTUAL_CAPITAL` / `VIRTUAL_STAKE` | Portafoglio virtuale (500 / 25) | No |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Notifiche | No |

---

## 2. Avvio

```bash
# Prova senza chiavi e senza rete: DATI GENERATI, non mercato.
python3 aurum.py run --simulate

# Con dati reali (richiede almeno una chiave nel .env)
python3 aurum.py run

# Dashboard: http://127.0.0.1:8100
```

Perche' non si chiuda quando chiudi il terminale:

```bash
# Semplice
nohup python3 aurum.py run > aurum.log 2>&1 &

# Meglio: servizio utente, riparte da solo
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/aurum-m60.service <<'UNIT'
[Unit]
Description=AURUM SIGNAL ENGINE M60
After=network-online.target

[Service]
WorkingDirectory=%h/aurum/aurum_signal
ExecStart=%h/aurum/aurum_signal/.venv/bin/python aurum.py run
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
UNIT

systemctl --user daemon-reload
systemctl --user enable --now aurum-m60
loginctl enable-linger "$USER"      # continua anche dopo il logout

systemctl --user status aurum-m60
journalctl --user -u aurum-m60 -f
```

### Tutti i comandi

| Comando | Cosa fa |
|---|---|
| `run` | Avvia motore e dashboard |
| `run --simulate` | Come sopra ma con dati **generati** (etichettati SIMULATION) |
| `status` | Stato sintetico su un database esistente |
| `diagnose` | Perche' non arrivano segnali: blocchi contati e classificati |
| `replay --source file.db` | Ripercorre dati registrati con un orologio virtuale |
| `backtest --source file.db` | Esito su dati registrati, con gli intervalli di confidenza |
| `research` | Un giro completo di ricerca su un database esistente |
| `export --out cartella/` | Esporta segnali, decisioni e operazioni in CSV |
| `selftest` | 20 verifiche del sistema su se stesso, nessuna rete |

---

## 3. Struttura

```
aurum_signal/
├── aurum.py                 CLI: run, status, diagnose, replay, backtest,
│                            research, export, selftest
├── config.py                Ogni parametro in un posto solo, con validazione
│                            all'avvio (payout, soglie, tempi, percorsi)
├── requirements.txt         Dipendenze, tutte facoltative
├── .env.example             Modello di configurazione
│
├── storage/
│   ├── schema.py            28 tabelle + 10 indici, colonne per il writer
│   └── database.py          Scritture accodate su thread dedicato, migrazione
│                            automatica, un guasto per tabella non travolge le altre
│
├── market/
│   ├── base.py              Quote, capacita' dichiarate, contratto dell'adapter
│   ├── realtime.py          TwelveData, Finnhub, ExchangeRate
│   ├── simulation.py        Generatore tarato sulla scala reale di EUR/USD
│   ├── replay.py            Orologio virtuale: il tempo segue i dati
│   └── fallback.py          Failover fra sorgenti + giudizio sulla qualita'
│
├── core/
│   ├── buffers.py           Serie causali O(log n), candele multi-intervallo
│   ├── features.py          50 feature predittive, mid e ora ESCLUSE dal modello
│   ├── agents.py            9 agenti; chi non ha i dati si astiene
│   ├── regime.py            Classificazione del regime + affidabilita' per regime
│   ├── decision.py          Aggregazione, soglie legate al payout, tetto alla confidenza
│   ├── signals.py           Ciclo di vita: PRE_SIGNAL → CONFIRMED → FROZEN → ENTERED
│   ├── wallet.py            Portafoglio virtuale a cicli, ricostruibile dal registro
│   ├── outcomes.py          Esiti, esiti in ombra, latenza p50/p95/p99
│   └── engine.py            L'orchestratore
│
├── ml/
│   ├── model.py             Regressione logistica pura + ponte sklearn
│   └── calibration.py       Platt, isotonica, curva di affidabilita', Brier
│
├── research/
│   ├── validation.py        Walk-forward con purga ed embargo, FDR, holdout, leakage
│   ├── booster.py           Giudizio sugli ultimi secondi, in ombra
│   └── engine.py            Scoperta → validazione → holdout → promozione
│
├── notification/telegram.py Coda e thread: una notifica non ferma mai il motore
├── dashboard/
│   ├── server.py            25 rotte, FastAPI + WebSocket, riserva stdlib
│   └── frontend/index.html  Interfaccia completa in un file, senza dipendenze
└── tests/selftest.py        20 casi
```

Circa 8.200 righe di Python piu' l'interfaccia. Nessun file supera le 900
righe, e nessun modulo importa `dashboard` o `tests`.

---

## 4. Come funziona

### Il numero che decide tutto

Con payout 0,80 il pareggio e' **1 / (1 + 0,80) = 55,56%**. Una probabilita'
del 54% non e' un segnale debole: e' un segnale perdente. Ogni soglia del
sistema nasce da questo numero, e ogni test statistico e' contro **55,56%**,
non contro il 50%.

### La linea temporale

```
T-60   osservazione, nessun impegno
T-30   PRE_SIGNAL: c'e' un candidato → notifica (Telegram, audio, dashboard)
T-20   monitoraggio: la direzione regge?
T-10   CONFIRMED oppure CANCELLED
T-5    congelato: da qui non cambia piu', hai il tempo di cliccare
T      entrata (manuale, la fai tu)
T+60   scadenza, esito, portafoglio, apprendimento
```

Un segnale si annulla quando **si deteriora davvero**: la probabilita' a suo
favore scende sotto il pareggio ed e' scesa di almeno 6 punti rispetto a
quella con cui era nato, e la caduta persiste per almeno due secondi. Non si
annulla perche' la valutazione corrente da sola non basterebbe a crearne uno
nuovo — con un solo segnale per volta quella condizione sarebbe sempre vera, e
il risultato era: 84 segnali creati, 84 annullati, zero entrate.

### La confidenza e' ristretta finche' non si guadagna

Un insieme di euristiche pesate non produce una probabilita': produce una
forza direzionale. Trasformarla in "87% di probabilita'" con una sigmoide
ripida e' il modo piu' rapido di costruire un sistema sicuro esattamente
quanto e' ignorante — e su un cammino quasi casuale la probabilita' vera resta
attorno al 50% qualunque cosa dicano gli agenti.

Percio' la componente euristica e' **ristretta verso il 50%**: con il consenso
totale degli agenti si arriva a circa il 64%, non all'88%. Il restringimento
si allarga **solo** se la calibrazione misurata lo autorizza, cioe' se il
sistema ha dichiarato in media il 62% e ha davvero vinto circa il 62%.

Prima di questa restrizione il sistema dichiarava 87,7% su un cammino casuale
ed emetteva sul 71% delle valutazioni. Adesso il massimo osservato e' 57-64% e
l'emissione sta sotto il 5%.

### Portafoglio virtuale e cicli

500 € virtuali, 25 € a operazione, payout 80%. Quando il capitale del ciclo si
esaurisce il ciclo **fallisce**, il sistema **studia** — perche' ha perso, in
quale regime, con quali configurazioni, quali filtri hanno protetto e quali
hanno tolto — e **solo dopo** ricomincia un ciclo nuovo. Lo storico non si
cancella mai, e ricominciare non recupera il capitale bruciato: il registro
conserva tutto e il saldo si ricostruisce riga per riga.

Denaro **finto**. Non rappresenta il saldo di nessun broker.

### Cosa impara, e da cosa

| Fonte | Cosa se ne ricava |
|---|---|
| Operazioni vinte | La firma delle configurazioni che funzionano |
| Operazioni perse | La firma di quelle da evitare |
| Decisioni bloccate (ombra) | Se un filtro **protegge** o **costa** |
| NO_TRADE | Anche non operare e' una scelta che si puo' sbagliare |
| Stream dei tick | Cosa segue una data configurazione di flusso, a +10/20/30/60s |

Il percorso per entrare in produzione non ha scorciatoie:

```
SCOPERTA → WALK-FORWARD CON PURGA → CORREZIONE PER TEST MULTIPLI (FDR)
        → HOLDOUT FRESCO → CALIBRAZIONE FUORI CAMPIONE → PROMOZIONE
```

Ogni passaggio elimina candidati: e' il suo scopo. Su dati finanziari, un
processo che promuove spesso e' un processo rotto. Il modello promosso entra
nella decisione con peso 0,5 se calibrato e 0,25 se no — **non sostituisce**
gli agenti, si aggiunge a loro — e `rollback_model()` lo ritira in un istante.

---

## 5. Revisione dell'architettura

Questa e' la sezione richiesta dal punto 103: l'analisi fatta **prima** e
riverificata **dopo** la scrittura del codice.

**A. Il percorso caldo non aspetta nessuno.** Feed → feature → agenti →
decisione → segnale gira nel loop asincrono e non tocca mai il disco, la rete
o un addestramento. Database, notifiche, ricerca e studio vivono su thread
separati e comunicano per code. Misurato: p50 della pipeline **5 ms**, p99
sotto i 20 ms, contro un limite dichiarato di 500 ms.

**B. Il tempo e' del motore, non dell'interfaccia.** Il countdown lo calcola
il motore e la pagina lo disegna. Due schermi aperti mostrano lo stesso
numero, e un browser lento non produce un'entrata anticipata. In replay
l'orologio segue i dati, quindi un'ora di mercato ripercorsa in un secondo ha
scadenze corrette.

**C. Un solo prezzo decide l'esito.** `PRICE_BASIS = "mid"`, dichiarato una
volta e usato ovunque. Mescolare mid all'ingresso e bid alla scadenza
produrrebbe un vantaggio o uno svantaggio sistematico che non viene dal
mercato ma dal codice.

**D. Il pareggio e' un esito a se'.** Uscita identica all'ingresso non e' ne'
vittoria ne' sconfitta: e' capitale immobilizzato per niente. Contarlo da una
parte falserebbe ogni statistica, quindi ha una categoria propria in tutto il
sistema.

**E. Astenersi e' una risposta.** Un agente senza i dati che gli servono si
astiene, e l'astensione ha peso zero. Il risultato e' un NO_TRADE spiegato al
posto di un numero inventato.

**F. Pochi cancelli duri, molti contributi.** I divieti assoluti riguardano
solo cio' che e' rotto o proibito: feed morto, dati incoerenti, dato macro in
uscita, modalita' sicura. Tutto il resto sposta un punteggio. Cosi' si puo'
sempre rispondere a "quanto manca per operare, e su cosa".

**G. Le feature sono causali per costruzione.** `value_at_or_before` non
restituisce mai un campione successivo all'istante richiesto, nemmeno di un
millisecondo. Il prezzo assoluto e l'ora del giorno sono esclusi dal modello:
su una sessione breve sarebbero il modo piu' rapido di far memorizzare il
periodo invece del mercato.

**H. La validazione e' progettata per bocciare.** Purga ed embargo di almeno
un orizzonte fra addestramento e test; campioni resi indipendenti (3600 righe
sovrapposte diventano 60 indipendenti a 60 secondi); Benjamini-Hochberg contro
i test multipli; holdout fresco messo da parte prima di guardare qualunque
cosa; intervalli di Wilson; test binomiale **contro il pareggio**. Verificato:
su un vantaggio vero 2 candidati su 3 promossi, su rumore 0 su 3.

**I. La calibrazione si stima fuori campione.** Il modello addestrato sulla
scoperta predice l'holdout, e Platt si stima su quelle previsioni. Se la
calibrazione peggiora il Brier non viene applicata.

**L. La ricerca non tocca il motore live.** Produce candidati, li valida, e al
massimo propone. La promozione e' un atto esplicito con requisiti espliciti.

**M. Il booster parte in ombra.** Osserva, registra, e non modifica il segnale.
Si salvano sia l'esito reale sia quello che si sarebbe ottenuto seguendolo. Per
uscire dall'ombra deve dimostrare, con un test a due proporzioni, che i segnali
che tiene vincono piu' di quelli che scarta — e deve scartarne davvero.

**N. Il database e' l'unica verita'.** Da `signal_id` si ricostruisce tutto:
decisione, opinioni dei singoli agenti, feature, fotografie a T-30..T-0,
booster, esito, riga del registro, saldo. Migrazione automatica: senza,
un database scritto da una versione precedente farebbe fallire ogni INSERT con
una colonna nuova, in silenzio — cosa che in questo progetto e' gia' successa.

**O. Nessuna tabella dichiarata resta vuota.** Una tabella che nessuno scrive
promette una capacita' che non esiste, e chi legge il database fra sei mesi non
puo' distinguere "non e' successo niente" da "non e' mai stato collegato". Un
caso del selftest lo verifica sui sorgenti.

**P. Simulato e reale non si confondono mai.** LIVE / REPLAY / SIMULATION e'
sempre visibile, in fascia gialla a tutta larghezza quando non e' LIVE, ed e'
scritto su ogni riga del database. Le righe simulate sono escluse per difetto
dalla ricerca: un vantaggio "dimostrato" su dati generati non e' un vantaggio.

**Q. Il fallimento e' un percorso previsto.** Feed morto → modalita' sicura.
Adapter caduto → failover. Agente in errore → astensione. Tabella rotta → le
altre continuano e il guasto compare in `/health`. Ciclo esaurito → studio →
ciclo nuovo. Il motore non muore per una notifica, per la ricerca o per un
booster.

---

## 6. Critica onesta del progetto

Questa e' la sezione richiesta dal punto 104. Non ti daro' ragione
automaticamente su niente di quanto segue.

### 6.1. L'ostacolo principale e' il payout, non il software

A 60 secondi EUR/USD e' molto vicino a un cammino casuale. Il payout dell'80%
richiede **55,56%** di vittorie solo per non perdere: significa avere ragione
su **oltre 5 punti percentuali in piu' del caso**, in modo persistente. E'
molto piu' di quanto la maggior parte delle strategie documentate ottenga su
questo orizzonte.

Nessuna quantita' di agenti, modelli o ricerca cambia questo. Il software puo'
misurare onestamente se ci si arriva; **non puo' garantire che ci si arrivi**, e
il risultato piu' probabile e' che non ci si arrivi.

### 6.2. L'anticipo di 30 secondi costa accuratezza, e non e' gratis

Un segnale emesso a T-30 per un'entrata a T con scadenza a T+60 sta predicendo
**t+90**, non t+60. Sono trenta secondi in piu' di futuro su un orizzonte gia'
quasi imprevedibile. E' un requisito operativo legittimo — devi avere il tempo
di cliccare — ma il costo statistico e' reale.

Il sistema lo misura (`lead_ms` finisce su ogni riga) invece di ignorarlo, ma
misurare un costo non e' eliminarlo. Se un giorno i dati mostrassero che a
T-10 la precisione e' sensibilmente migliore, la scelta onesta sarebbe ridurre
l'anticipo, non difendere i 30 secondi.

### 6.3. Non esistono dati FX gratuiti di qualita' adeguata

I feed gratuiti danno una quotazione al secondo, a volte meno, spesso senza
timestamp del provider. A 60 secondi puo' bastare per le feature di
minuto — non basta per la microstruttura.

E soprattutto: **non esiste nessun book di livello 2 gratuito sul forex**, e
sul forex spot non esiste nemmeno un vero book centralizzato. Il progetto
dichiara le capacita' di ogni adapter e gli agenti si astengono su cio' che
non c'e'; `order_flow_snapshots` marca `available = 0` quando la misura non e'
reale. Un book inventato sarebbe stato peggio di nessun book. Ma resta il
fatto: **una parte della microstruttura richiesta non e' osservabile con
questi dati**.

### 6.4. Troppi moduli di ricerca creano rischio di test multipli

Ventiquattro candidati per giro, ogni mezz'ora, per settimane, sugli stessi
dati: con `alpha = 0,05` qualcosa sembrera' significativo **per forza**.

Benjamini-Hochberg corregge dentro un singolo giro. Non corregge fra giri
diversi nel tempo, ed e' un limite reale di cui essere consapevoli: se giri per
un mese e alla fine un candidato passa, la probabilita' che sia rumore resta
piu' alta di quanto il suo p-value dica. L'holdout fresco e il requisito che il
limite **inferiore** dell'intervallo superi il pareggio riducono il problema,
non lo eliminano.

### 6.5. La quantita' di dati necessaria e' molto maggiore di quella che sembra

A 60 secondi di orizzonte, **un'ora di mercato produce 60 osservazioni
indipendenti**. Non 36.000 — 60. Tutto il resto sono finestre sovrapposte e
correlate.

Per distinguere il 56% dal 50% con un minimo di sicurezza servono centinaia di
osservazioni indipendenti, cioe' **giorni** di mercato. Il sistema lo dice
esplicitamente quando l'holdout e' troppo piccolo, invece di far sparire il
candidato in silenzio. E ha una conseguenza pratica: **nelle prime settimane il
sistema non promuovera' niente**, e sara' il comportamento corretto.

### 6.6. Cose implementate, ma non ancora dimostrate su dati reali

Il calendario economico e' predisposto (`news_events`, agente `news`, finestre
di divieto) ma **non collegato a una fonte**: l'agente dichiara
`NEWS DATA UNAVAILABLE` e non contribuisce, invece di fingere. Il monitoraggio
di piu' grafici EUR/USD da fonti diverse esiste come failover ma non e' ancora
un confronto sistematico di latenza fra fonti simultanee.

Il modello promosso dalla ricerca ora entra davvero nella decisione, ma
finche' non ci saranno giorni di dati reali **nessun modello verra' promosso**,
e quel percorso restera' esercitato solo dai test.

### 6.7. Quello che il software fa bene

Non e' tutto negativo, e non sarebbe onesto lasciarlo intendere. Questo
progetto misura correttamente cose che la maggior parte dei sistemi simili
sbaglia: lega ogni soglia al payout invece che a un numero scelto a mano; non
esprime una confidenza che non ha guadagnato; distingue il pareggio dagli altri
esiti; valuta le decisioni che ha bloccato per sapere se i filtri costano; non
promuove niente senza un holdout fresco; e non presenta mai dati simulati come
dati di mercato.

Se il vantaggio non c'e', questo sistema te lo dira'. Vale piu' di un sistema
che ti dice di si'.

---

## 7. Cosa il sistema non fara' mai

- Non invia ordini, a nessun broker, in nessuna modalita'.
- Non si collega a Pocket Option per operare. Non usa automazione del browser.
- Non promette profitti. Non esistono "profitto garantito", "profitto sicuro"
  o "operazione sicura": sono espressioni prive di significato su una scommessa
  binaria con un payout inferiore alla parita'.
- Non presenta dati simulati come dati di mercato.
- Non scrive chiavi API nel codice.

**Sistema sperimentale e probabilistico.** Le opzioni binarie hanno un
rendimento atteso negativo per chi non ha un vantaggio dimostrato. Rischia solo
denaro che puoi permetterti di perdere.
