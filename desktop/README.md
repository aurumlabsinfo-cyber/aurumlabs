# Notificatore desktop

Un processo che tieni aperto sulla tua macchina. Si collega al backend e fa
partire una **notifica nativa di sistema** ogni volta che il motore cambia stato
a un segnale. Niente tab del browser aperta, niente Electron.

```bash
pip install websockets
python3 notifier.py
```

Funziona su macOS (`osascript`), Linux (`notify-send`) e Windows 10+
(toast via PowerShell). Se non trova nessuno dei tre, stampa in console e
continua a funzionare.

---

## Opzioni

```bash
# backend su un'altra macchina
python3 notifier.py --url ws://192.168.1.50:8000

# solo i segnali molto confidenti
python3 notifier.py --min-confidence 0.75

# solo quando il trigger viene raggiunto e quando finisce
python3 notifier.py --events trigger_hit,signal_settled

# suono sugli eventi urgenti (trigger raggiunto, perdita)
python3 notifier.py --sound

# nessuna notifica di sistema, solo terminale
python3 notifier.py --no-desktop
```

| Evento | Quando |
|---|---|
| `signal_created` | segnale generato, in attesa del trigger |
| `trigger_hit` | il prezzo ha toccato il trigger, countdown avviato |
| `trade_active` | countdown in corso |
| `trade_expired` | orizzonte scaduto, in liquidazione |
| `signal_settled` | risultato: WIN / LOSS / PARI |
| `signal_cancelled` | trigger mai raggiunto nella finestra di attesa |

Di default notifica `signal_created`, `trigger_hit` e `signal_settled`.
`trade_active` e `trade_expired` arrivano entro 5 secondi da `trigger_hit` e
aggiungerebbero solo rumore.

I risultati non vengono mai filtrati dalla confidence: come è finita la vuoi
sapere comunque, qualunque cosa il modello avesse dichiarato all'inizio.

---

## Perché una connessione aperta e non una chiamata al minuto

Un segnale a 5 secondi va consegnato **dentro** quei 5 secondi. Campionando una
volta al minuto, la finestra da prevedere si è chiusa 55 secondi prima della
tua occhiata successiva: staresti prevedendo un evento già passato.

L'orizzonte deve essere molto più corto dell'intervallo di campionamento, non
dodici volte più lungo. Una WebSocket tenuta aperta non costa nulla e consegna
in millisecondi, quindi è insieme la scelta più economica e l'unica che
funzioni.

## Perché non legge lo schermo

Il grafico sul monitor è una rappresentazione dei dati: pixel, arrotondata, con
ritardo di rendering. I numeri veri — bid, ask, ogni trade con timestamp al
millisecondo — sono già disponibili gratis e senza chiave dall'API pubblica.

Leggere i pixel al posto dei numeri aggiunge errore e latenza per ottenere meno
informazione, e la parte costosa (un modello che analizza uno screenshot) è
proprio quella che non serve.

---

## Cosa NON fa

* **Non decide niente.** Mostra le decisioni del motore, non ne prende.
* **Non invia ordini.** Non esiste codice di esecuzione in questo repository.
* **Non valida i segnali.** Finché `app.ml.cli search` su dati reali non
  restituisce qualcosa di diverso da `NO EDGE`, ciò che arriva sono **ipotesi
  non validate**.

Un `82%` nella notifica significa "il modello afferma 82%", non "vince l'82%
delle volte". Sono cose diverse, e la differenza si misura con
`/statistics/calibration`.

Se il backend gira sul simulatore, ogni notifica è marcata `[SIMULATO]` e
`DATI SINTETICI - non è il mercato`. Non è possibile scambiare l'output del
simulatore per una chiamata sul mercato.

---

## Tenerlo sempre attivo

**Linux (systemd utente)** — `~/.config/systemd/user/btc-notifier.service`:

```ini
[Unit]
Description=BTC 5s Quant notifier
After=network-online.target

[Service]
ExecStart=/usr/bin/python3 %h/percorso/desktop/notifier.py --sound
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now btc-notifier
```

**macOS** — `~/Library/LaunchAgents/com.btcquant.notifier.plist` con
`RunAtLoad` e `KeepAlive` a `true`.

**Windows** — Utilità di pianificazione, "all'accesso dell'utente",
azione `pythonw.exe notifier.py`.

Il notificatore si riconnette da solo con backoff esponenziale e ti avvisa
quando **perde** il backend, così un motore fermo non si confonde con un
mercato senza segnali.
