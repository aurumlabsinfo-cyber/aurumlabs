# Avviare AURUM EUR/USD e collegarlo al tuo conto Pocket

Tre cose, in ordine: farlo partire, collegare il conto, e farlo restare acceso
quando chiudi il terminale.

---

## 0. Prima di tutto: cosa fa e cosa non fa

Il motore fa **paper trading**. Le operazioni che vedi sulla dashboard sono
finte e il saldo di 500 EUR e' finto. Collegare il tuo conto Pocket serve a
**leggere** saldo, payout e stato della connessione, e a confrontare il prezzo
del broker con quello del venue.

**Nessun ordine viene mai inviato.** Non esiste in questi programmi una
chiamata capace di piazzarne uno, e una verifica del `selftest` legge il
sorgente e fallisce se qualcuno la aggiunge. Se un giorno vorrai operare per
davvero, e' una decisione tua da prendere consapevolmente con un altro
programma — non una riga da aggiungere qui di nascosto.

---

## 1. Preparare la cartella

```bash
mkdir -p ~/aurum && cd ~/aurum
# copia qui aurum_binary_eurusd_m60.py e pocket_bridge_service.py
python3 aurum_binary_eurusd_m60.py config | head -3      # deve dire 1.5.0
python3 aurum_binary_eurusd_m60.py selftest              # 17/17, nessuna rete
```

Se `config` non stampa `"version": "1.5.0-BINARY-EURUSD-M60"` stai eseguendo
una copia vecchia: sovrascrivila.

---

## 2. Farlo partire, subito

```bash
cd ~/aurum
python3 aurum_binary_eurusd_m60.py run --payout 0.8 \
    --capital 500 --stake-amount 25
```

Apri `http://localhost:8002`. Se la porta e' occupata il motore prende la prima
libera e te lo dice.

Il **payout e' il tuo**: mettici il numero che ti da' davvero Pocket su EUR/USD
a 60 secondi (0.8 = 80%). Senza, il portafoglio resta spento — perche' senza
payout un saldo in denaro non e' definito, e il motore preferisce dichiararlo
invece di inventarlo.

Il primo segnale arriva dopo circa **3 minuti**: e' il tempo che serve a
misurare la volatilita' su finestre da un minuto. Prima non sarebbe una misura.

Da un altro terminale, se qualcosa non torna:

```bash
python3 aurum_binary_eurusd_m60.py diagnose     # perche' non arrivano segnali
python3 aurum_binary_eurusd_m60.py sorgenti     # calendario e latenza dei siti
python3 aurum_binary_eurusd_m60.py wallet       # saldo, cicli, registro
```

---

## 3. Collegare il tuo conto Pocket

Il motore **non vede mai le tue credenziali**. Un secondo processo — il bridge
— legge la sessione e pubblica solo saldo, valuta, payout e stato su una pagina
locale. Se il bridge cade, il motore continua a girare e mostra "dato vecchio".

### 3a. Prendere la stringa di sessione

1. entra su Pocket Option dal **browser**, con il conto che vuoi leggere;
2. apri gli strumenti da sviluppatore (`F12`) → scheda **Network** → filtro **WS**;
3. clicca sulla connessione WebSocket → **Messages**;
4. cerca il primo messaggio inviato, quello che comincia con `42["auth"`;
5. copialo **per intero**, dalla prima parentesi quadra all'ultima.

> Quella stringa e' una **credenziale**: chi ce l'ha entra nel tuo conto.
> Non incollarla in una chat, non metterla in un file dentro un repository,
> non passarla sulla riga di comando (chiunque sulla macchina la vedrebbe con
> `ps`). Se pensi di averla esposta, esci da Pocket da tutti i dispositivi:
> la sessione decade.

### 3b. Metterla al sicuro

```bash
mkdir -p ~/.aurum && chmod 700 ~/.aurum
nano ~/.aurum/pocket.auth        # incolla la stringa, salva
chmod 600 ~/.aurum/pocket.auth   # leggibile solo da te
```

Il bridge controlla i permessi all'avvio e ti avvisa se sono troppo larghi.

### 3c. Provare il collegamento

```bash
pip install pocketoptionapi-async
python3 pocket_bridge_service.py --auth ~/.aurum/pocket.auth --asset EURUSD
```

In un altro terminale:

```bash
curl -s http://127.0.0.1:8010/status | python3 -m json.tool
```

Devi vedere `"connected": true` e il tuo saldo. Nella risposta **non c'e' la
sessione**: e' verificato, non promesso.

### 3d. Dirlo al motore

```bash
python3 aurum_binary_eurusd_m60.py run --payout 0.8 \
    --capital 500 --stake-amount 25 \
    --pocket-bridge http://127.0.0.1:8010
```

Sulla dashboard il riquadro **Bridge broker** mostra saldo, payout, tipo di
conto, eta' dell'ultima lettura e `Ordini inviati 0`. E il prezzo del broker
entra nella tabella **Sorveglianza prezzi** accanto agli altri siti: se diverge
troppo, viene dichiarato anomalo.

**Comincia dal conto demo.** Nella stringa di sessione, `"isDemo":1` e' demo e
`"isDemo":0` e' reale. Il bridge te lo dice all'avvio (`conto: DEMO` o `LIVE`)
e la dashboard lo ripete. Anche in LIVE resta sola lettura, ma tanto vale
prendere confidenza con il conto che non conta.

---

## 4. Non si chiude quando chiudi il terminale

### La via giusta: due servizi di sistema

```bash
mkdir -p ~/.config/systemd/user
cp deploy/aurum-bridge.service ~/.config/systemd/user/
cp deploy/aurum-engine.service ~/.config/systemd/user/
```

Le chiavi API vanno in un file, non sulla riga di comando:

```bash
cat > ~/.aurum/env <<'EOF'
TWELVE_DATA_API_KEY=la_tua_chiave
ALPHAVANTAGE_API_KEY=la_tua_chiave
EOF
chmod 600 ~/.aurum/env
```

> Se non hai (ancora) le chiavi, crea comunque il file vuoto: senza,
> `aurum-engine` non parte. Il feed FX e le news si spegneranno da soli e la
> dashboard lo dira'.

Poi:

```bash
systemctl --user daemon-reload
systemctl --user enable --now aurum-bridge
systemctl --user enable --now aurum-engine
```

**E questo comando, che e' il punto della domanda:**

```bash
sudo loginctl enable-linger $USER
```

Senza, systemd chiude i tuoi servizi all'**ultimo logout**: e' esattamente il
"chiudo il terminale e si ferma tutto". Con `enable-linger` restano accesi, e
ripartono da soli anche dopo un riavvio della macchina.

Da qui in poi:

```bash
systemctl --user status aurum-engine       # come sta
journalctl --user -u aurum-engine -f       # i log dal vivo
systemctl --user restart aurum-engine      # riavvia
systemctl --user stop aurum-engine         # ferma
```

### La via veloce, se sei di fretta

`nohup` funziona, ma non riparte da solo se il processo muore e non sopravvive
a un riavvio della macchina:

```bash
cd ~/aurum
nohup python3 pocket_bridge_service.py --auth ~/.aurum/pocket.auth \
      > bridge.log 2>&1 &
nohup python3 aurum_binary_eurusd_m60.py run --payout 0.8 \
      --capital 500 --stake-amount 25 \
      --pocket-bridge http://127.0.0.1:8010 > motore.log 2>&1 &
```

Per fermarli: `pkill -f aurum_binary_eurusd_m60.py`

Un'alternativa comoda e' `tmux`, che ti lascia anche rientrare a guardare
l'output:

```bash
tmux new -s aurum
# lancia i due comandi, poi stacca con  Ctrl+b  d
tmux attach -t aurum        # per rientrare
```

---

## 5. Guardarlo da un altro computer

La dashboard ascolta **solo in locale** (`127.0.0.1`), ed e' la scelta giusta:
non ha autenticazione, quindi chiunque la raggiunga vede e comanda tutto.

Per guardarla dal tuo portatile, fai un tunnel invece di aprire la porta:

```bash
ssh -L 8002:127.0.0.1:8002 aurum@indirizzo-del-server
```

Poi apri `http://localhost:8002` sul portatile. Il traffico passa dentro ssh e
nessuna porta resta esposta.

**Non usare `--host 0.0.0.0`** su una macchina raggiungibile da internet.

---

## 6. Se qualcosa non va

| Sintomo | Da guardare |
|---|---|
| Nessun segnale | `diagnose` — dice quale cancello blocca e cosa fare |
| Portafoglio spento | manca `--payout`: senza, il denaro non e' calcolabile |
| Bridge "non connesso" | `journalctl --user -u aurum-bridge -n 50` |
| Sessione scaduta | riprendila dal browser: decade da sola dopo qualche giorno |
| Calendario "non disponibile" | `sorgenti` stampa l'errore esatto delle API |
| Il servizio muore al logout | manca `sudo loginctl enable-linger $USER` |
| Versione sbagliata | `config \| head -3` deve dire `1.5.0` |

---

## Una cosa che vale la pena ripetere

Il motore **non ha ancora dimostrato nessun vantaggio**. Tutto quello che e'
stato misurato dice che la macchina emette, entra, chiude e conta
correttamente — non che vince. Prima di dare peso a un tasso di vittoria,
guarda l'intervallo di confidenza sulla dashboard: se il suo limite inferiore
non supera il pareggio richiesto dal payout, quel numero e' compatibile con il
caso.

E i cicli: quando il saldo non copre piu' una puntata, il ciclo e' bruciato e
il motore ne riapre uno nuovo dopo aver studiato. **Ricominciare non recupera
niente** — il capitale del ciclo precedente e' perso. Quello che i cicli
misurano e' quanti ne servono e quanto durano.
