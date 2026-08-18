# AURUM PORTALE 2.0.0 — un file solo

I quattro programmi caricati sono diventati **`aurum_portal.py`**: un file, una
porta, un database, un front end.

| File di partenza | Cosa faceva | Dov'e' finito |
|---|---|---|
| `aurum_engine_60s_research.py` | il motore (1.3.1-M60-RESEARCH) | **la base**: tutto conservato, niente tolto |
| `pocket_bridge.py` | confronto prezzi Aurum ↔ broker, su console | sezione `PocketLink`, dentro il motore |
| `pocket_portal.py` | server aiohttp su :8003, filtro segnali, dry-run | sezione `PocketGate` + pannello del portale, sulla stessa porta |
| `pocket_readonly_test.py` | prova di connessione e di formato della sessione | comando `pocket` |

## Perche' unirli cambia il risultato, non solo l'ordine

Il portale separato chiedeva *"questo segnale sarebbe eseguibile?"* interrogando
l'API HTTP di Aurum. Quindi **giudicava un istante diverso** da quello in cui il
segnale era nato: fra la nascita del segnale e la risposta dell'API passavano
centinaia di millisecondi, ed e' proprio in quei millisecondi che il prezzo del
broker si allontana.

Adesso il verdetto si prende **dentro il ciclo di vita del segnale**, sullo
stesso tick. E l'esito viene riscritto sulla riga del verdetto — che e' cio' che
rende possibile rispondere all'unica domanda che conta su un filtro:

```bash
python3 aurum_portal.py pocket        # i segnali approvati hanno vinto piu' degli altri?
```

Se i segnali scartati hanno vinto quanto quelli approvati, il filtro **costa**
invece di proteggere, e il referto lo dice con quelle parole.

## Nessun ordine, e non e' una promessa

In questo file non esiste una chiamata capace di piazzare un ordine. Non e'
scritto solo nei commenti: una verifica del `selftest` **legge il proprio
sorgente** e fallisce se qualcuno ne aggiunge una.

La puntata che compare nel registro (`--pocket-stake`) e' un'**annotazione**,
serve a rendere confrontabili le righe con quelle del broker. Non e'
un'istruzione a operare.

## Come si avvia

```bash
python3 aurum_portal.py selftest                 # 16 verifiche, nessuna rete
python3 aurum_portal.py run --payout 0.8         # motore, dashboard su :8002
```

Con il broker collegato (facoltativo):

```bash
pip install pocketoptionapi-async                # solo se vuoi il pannello broker
python3 aurum_portal.py run --payout 0.8 \
    --pocket --pocket-auth ~/secrets/pocket.auth --pocket-asset BTCUSD
python3 aurum_portal.py pocket                   # stato del collegamento + referto
```

**Senza la libreria il file resta quello che era**: sola libreria standard. Il
pannello si mostra spento e dice perche', invece di impedire l'avvio. E con il
collegamento spento il prezzo del broker resta `—`: non viene mai inventato, e i
verdetti sono `NON_VERIFICATO`, mai `NON_ESEGUIBILE`. Un collegamento che non
c'e' non e' una bocciatura del segnale.

## I due filtri del rumore, e cosa hanno fatto davvero

Erano la risposta alla richiesta di "stare piu' attenti al rumore". Vanno
raccontati per quello che le misure dicono, non per quello che dovevano fare.

**Soglia relativa** (`--noise-factor`, default 1.5): il movimento atteso
sull'orizzonte deve battere il rumore di fondo — spread piu' un tick — di questo
fattore. Sostituisce un minimo assoluto di tick, che su mercati diversi non vuol
dire niente.

> **Su tre archivi da 45–60 minuti non e' mai scattato.** La sigma
> dell'orizzonte era sempre molto sopra il rumore. Puo' avere senso su un
> mercato fermo; qui non ha fatto nulla, e va detto.

**Persistenza direzionale** (`--persistence`, default 3): la direzione deve
reggere per N valutazioni consecutive prima di emettere.

> **Non filtra quando il motore e' saturo.** Con `max_concurrent = 1` c'e'
> quasi sempre gia' un'operazione aperta: la persistenza non toglie segnali,
> **sposta l'ingresso** di ~200 ms. Su tre archivi il numero di segnali e' stato
> identico con e senza (114 / 84 / 84).

Cosa e' successo al tasso di vittoria su quei tre archivi:

| archivio | senza persistenza | con persistenza 3 | segnali |
|---|---|---|---|
| seme 31 | 56,2% | **61,6%** | 114 → 114 |
| seme 77 | 51,2% | 51,2% | 84 → 84 |
| seme 101 | 56,1% | **63,4%** | 84 → 84 |

Mai peggio, due volte meglio. **Non e' una prova.** Tre archivi di dati
*generati da un modello*, un meccanismo che e' un ritardo fisso di 200 ms, e
nessuna ragione perche' generalizzi: su una serie che tende, entrare dopo la
conferma aiuta; su una che torna indietro, la stessa regola danneggia. Sui dati
veri e' tutto da verificare.

Per questo `diagnose` adesso mostra **se stiano mordendo**, invece di lasciarlo
supporre:

```
  filtri del rumore : persistenza 3 finestre (ha bloccato 305x) · soglia x1.5 (ha bloccato 0x)
```

E quando la persistenza non blocca mai con un'operazione alla volta, lo dice
esplicitamente: sta solo ritardando.

## Cosa vedi nel portale

Tutto sulla stessa pagina, su `http://localhost:8002`:

* portafoglio, ciclo in corso, curva del capitale;
* grafico a candele (5s / 20s / 1m / 5m / 10m / 30m) con le operazioni sopra;
* segnale corrente con countdown, e i cancelli che stanno bloccando;
* cosa analizza: gli otto agenti, la microstruttura, il laboratorio strategie;
* **portale broker**: i due prezzi, la divergenza in bps, la latenza del broker,
  il saldo, la puntata annotata, e la tabella dei verdetti con l'esito di
  ciascuno;
* apprendimento (dati raccolti, prossimo studio, verdetto), salute, storico.

## Verificato

`selftest` **16/16**, senza rete. Le due verifiche nuove:

* **portale broker** — verdetto corretto nei quattro casi (spento, coerente,
  prezzi divergenti, broker lento), registro che non duplica, esito che torna
  sulla riga senza sovrascrivere il verdetto, e la scansione del sorgente che
  garantisce l'assenza di percorsi d'ordine;
* **filtro del rumore** — il rumore e' spread + tick, tre finestre di fila sono
  richieste, e una singola inversione azzera la serie.

Aperto in Chromium con la libreria del broker assente: nessun errore
JavaScript, prezzo del broker mostrato come sconosciuto e non inventato, esiti
che compaiono sulle righe dei verdetti.

## Cosa NON e' stato dimostrato

Nessun edge. Le misure qui sopra dicono che la macchina emette, entra, chiude,
conta e giudica correttamente. Sui tre archivi sintetici il p-value contro il
lancio di una moneta e' 0,22 / 0,91 / 0,32 senza persistenza: esattamente il
caso. Il vantaggio, se c'e', si misura sui tuoi dati veri con `backtest`,
`shadow` e `pocket`.
