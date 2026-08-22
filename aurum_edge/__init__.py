"""AURUM EDGE DISCOVERY.

Uno strumento di **previsione manuale** per BTCUSDT Perpetual su Bybit.

Cosa fa: dice se i prossimi 30 minuti hanno una direzione prevedibile, con
quale probabilita', entro quale intervallo di prezzo, per quanto tempo, e a
quale prezzo la tesi e' sbagliata.

Cosa NON fa, per costruzione e non per configurazione: non apre ordini, non
chiude ordini, non si collega a un wallet, non usa capitale, non usa leva, non
gestisce stop loss, non fa trading automatico. Non esiste in questo pacchetto
una sola chiamata autenticata a un exchange: solo endpoint pubblici in lettura.

Il principio che governa ogni scelta: **meglio WAIT che una previsione falsa**.
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__ = ["__version__"]
