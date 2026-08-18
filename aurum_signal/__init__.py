"""AURUM SIGNAL ENGINE M60 — analisi e segnali su EUR/USD a 60 secondi.

Cosa fa: osserva il mercato, stima una probabilita' direzionale, emette un
segnale con almeno 30 secondi di anticipo, ne misura l'esito, studia i propri
errori e tiene un portafoglio virtuale.

Cosa NON fa, per costruzione e non per configurazione: non invia ordini, non
si collega a un broker, non automatizza un browser, non tocca nessun conto.
Le operazioni le decide ed esegue una persona. Il modulo `tests/selftest.py`
verifica questa proprieta' analizzando il codice sorgente a ogni esecuzione.

Non promette profitti. Su un orizzonte di 60 secondi EUR/USD e' vicino a un
cammino casuale e il payout dell'80% richiede il 55,56% di vittorie solo per
non perdere: il sistema misura quanto ci si avvicina, non assume di riuscirci.
"""

from .config import APP_NAME, VERSION

__all__ = ["APP_NAME", "VERSION"]
