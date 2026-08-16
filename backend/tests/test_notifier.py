"""Desktop notifier: message building, filtering and de-duplication.

The rendering logic is pure, so it is tested directly. What matters most is
that a simulator signal can never be mistaken for a market call.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


# The notifier lives outside the backend package: it runs on the user's desktop,
# not in the container, and depends on nothing from `app`.
_PATH = Path(__file__).resolve().parents[2] / "desktop" / "notifier.py"
_spec = importlib.util.spec_from_file_location("notifier", _PATH)
notifier = importlib.util.module_from_spec(_spec)
sys.modules["notifier"] = notifier
_spec.loader.exec_module(notifier)


def signal(**over):
    base = {
        "signal_id": "abc123",
        "direction": "DOWN",
        "status": "WAITING",
        "trigger_price": 104515.8,
        "entry_price": None,
        "expiry_price": None,
        "confidence": 0.82,
        "horizon_s": 5.0,
        "result": None,
        "is_synthetic": False,
    }
    base.update(over)
    return base


# ------------------------------------------------------------------ formatting
def test_price_uses_the_italian_convention():
    assert notifier.fmt_price(104515.8) == "104.515,80"
    assert notifier.fmt_price(None) == "—"
    assert notifier.fmt_price("nonsense") == "—"


def test_new_signal_states_that_the_countdown_has_not_started():
    note = notifier.build_notification("signal_created", signal(), synthetic=False)
    assert note.title == "NUOVO SEGNALE"
    assert "104.515,80" in note.body
    assert "countdown parte SOLO al trigger" in note.body
    assert "GIÙ" in note.body
    assert note.urgent is False


def test_trigger_hit_is_urgent_and_shows_the_entry():
    note = notifier.build_notification(
        "trigger_hit", signal(entry_price=104515.8, status="ACTIVE"), synthetic=False
    )
    assert note.urgent is True
    assert "104.515,80" in note.body
    assert "countdown avviato" in note.body


def test_settled_shows_entry_and_expiry():
    note = notifier.build_notification(
        "signal_settled",
        signal(result="WIN", entry_price=104515.8, expiry_price=104509.2),
        synthetic=False,
    )
    assert "WIN" in note.body
    assert "104.515,80" in note.body and "104.509,20" in note.body
    assert note.urgent is False


def test_a_loss_is_flagged_urgent():
    note = notifier.build_notification(
        "signal_settled", signal(result="LOSS"), synthetic=False
    )
    assert note.urgent is True


def test_up_and_down_render_distinctly():
    up = notifier.build_notification(
        "signal_created", signal(direction="UP"), synthetic=False
    )
    down = notifier.build_notification("signal_created", signal(), synthetic=False)
    assert "↑ SU" in up.body
    assert "↓ GIÙ" in down.body


def test_unknown_events_produce_nothing():
    assert notifier.build_notification("something_else", signal(), False) is None


# ------------------------------------------------------- synthetic labelling
def test_synthetic_signals_are_impossible_to_mistake_for_the_market():
    note = notifier.build_notification("trigger_hit", signal(), synthetic=True)
    assert note.title.startswith("[SIMULATO]")
    assert "DATI SINTETICI" in note.body
    assert "non è il mercato" in note.body


def test_live_signals_carry_no_simulator_wording():
    note = notifier.build_notification("trigger_hit", signal(), synthetic=False)
    assert "SIMULATO" not in note.title
    assert "SINTETICI" not in note.body


# ---------------------------------------------------------------- filtering
def test_only_requested_events_notify():
    wanted = ("signal_created",)
    assert notifier.should_notify("signal_created", signal(), wanted, 0.0) is True
    assert notifier.should_notify("trigger_hit", signal(), wanted, 0.0) is False


def test_confidence_floor_filters_weak_signals():
    wanted = notifier.ALL_EVENTS
    assert notifier.should_notify("signal_created", signal(confidence=0.62), wanted, 0.75) is False
    assert notifier.should_notify("signal_created", signal(confidence=0.81), wanted, 0.75) is True


def test_results_are_never_filtered_by_confidence():
    """You always want to know how it ended, whatever it claimed going in."""
    wanted = notifier.ALL_EVENTS
    weak = signal(confidence=0.10, result="LOSS")
    assert notifier.should_notify("signal_settled", weak, wanted, 0.95) is True
    assert notifier.should_notify("signal_cancelled", weak, wanted, 0.95) is True


def test_missing_confidence_is_treated_as_zero_not_as_pass():
    wanted = notifier.ALL_EVENTS
    no_conf = signal()
    no_conf.pop("confidence")
    assert notifier.should_notify("signal_created", no_conf, wanted, 0.5) is False


def test_default_events_skip_the_noisy_intermediate_steps():
    # trade_active and trade_expired land within 5s of trigger_hit.
    assert "trigger_hit" in notifier.DEFAULT_EVENTS
    assert "trade_active" not in notifier.DEFAULT_EVENTS
    assert set(notifier.DEFAULT_EVENTS) <= set(notifier.ALL_EVENTS)


# ------------------------------------------------------------ de-duplication
class _Recorder(notifier.DesktopNotifier):
    def __init__(self):
        self.sent = []
        self.sound = False
        self.system = "Test"
        self.backend = "console"
        self.failures = 0

    def send(self, note):
        self.sent.append(note)


def _watcher(**over):
    import argparse

    args = argparse.Namespace(
        url="ws://localhost:8000",
        events=",".join(notifier.ALL_EVENTS),
        min_confidence=0.0,
        sound=False,
        no_desktop=True,
    )
    for k, v in over.items():
        setattr(args, k, v)
    w = notifier.SignalWatcher(args)
    w.notifier = _Recorder()
    return w


def frame(event, sig):
    return {"type": "signal", "data": {"event": event, "signal": sig}}


def test_a_repeated_frame_notifies_once():
    w = _watcher()
    w.handle(frame("signal_created", signal()))
    w.handle(frame("signal_created", signal()))
    assert len(w.notifier.sent) == 1


def test_different_steps_of_the_same_signal_each_notify():
    w = _watcher()
    w.handle(frame("signal_created", signal()))
    w.handle(frame("trigger_hit", signal(entry_price=104515.8)))
    w.handle(frame("signal_settled", signal(result="WIN")))
    assert [n.title for n in w.notifier.sent] == [
        "NUOVO SEGNALE", "TRIGGER RAGGIUNTO", "RISULTATO",
    ]


def test_heartbeats_are_ignored():
    w = _watcher()
    w.handle({"type": "heartbeat", "server_ts": 1})
    assert w.notifier.sent == []


def test_the_snapshot_sets_the_synthetic_flag():
    w = _watcher()
    w.handle({"type": "snapshot", "data": {"is_synthetic": True, "counters": {}}})
    assert w.synthetic is True
    w.handle(frame("signal_created", signal()))
    assert w.notifier.sent[0].title.startswith("[SIMULATO]")


def test_a_synthetic_signal_alone_flips_the_flag():
    """Even without a snapshot, one flagged signal is enough."""
    w = _watcher()
    w.handle(frame("signal_created", signal(is_synthetic=True)))
    assert w.synthetic is True
    assert "DATI SINTETICI" in w.notifier.sent[0].body


def test_seen_set_is_bounded():
    w = _watcher()
    for i in range(6000):
        w.handle(frame("signal_created", signal(signal_id=f"s{i}")))
    assert len(w.seen) <= 5000


# --------------------------------------------------------------------- CLI
def test_parser_defaults_are_safe():
    args = notifier.build_parser().parse_args([])
    assert args.url == "ws://localhost:8000"
    assert args.min_confidence == 0.0
    assert args.sound is False
    assert args.no_desktop is False


def test_parser_accepts_overrides():
    args = notifier.build_parser().parse_args(
        ["--url", "ws://box:9000", "--min-confidence", "0.8", "--sound"]
    )
    assert args.url == "ws://box:9000"
    assert args.min_confidence == 0.8
    assert args.sound is True


def test_banner_states_no_edge_is_proven():
    """The notifier must not let a confidence number imply a validated result."""
    assert "NESSUN EDGE" in notifier.BANNER
    assert "PAPER TRADING" in notifier.BANNER


def test_watcher_targets_the_signals_stream():
    w = _watcher(url="ws://box:9000")
    assert w.url == "ws://box:9000/ws/signals"
