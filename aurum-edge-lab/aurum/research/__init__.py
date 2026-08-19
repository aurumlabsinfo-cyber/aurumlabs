"""Research: hypotheses, datasets, memory, validation and the director.

``ResearchDirector`` is exposed lazily.  It imports the agents package, whose
agents import this package's leaf modules; eagerly importing the director here
would make ``aurum.research`` and ``aurum.agents`` each require the other to be
finished first.  The leaves below have no such dependency and import normally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .dataset import Observation, ResearchView, build_observations
from .hypotheses import Hypothesis, PercentileCondition, new_hypothesis_id
from .memory import Outcome, ResearchMemory
from .validation import Stage, ValidationLab, ValidationReport

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from .director import CycleReport, ResearchDirector

_LAZY = {"ResearchDirector": "director", "CycleReport": "director"}


def __getattr__(name: str) -> Any:
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    module = import_module(f".{module_name}", __name__)
    return getattr(module, name)


__all__ = [
    "CycleReport",
    "Hypothesis",
    "Observation",
    "Outcome",
    "PercentileCondition",
    "ResearchDirector",
    "ResearchMemory",
    "ResearchView",
    "Stage",
    "ValidationLab",
    "ValidationReport",
    "build_observations",
    "new_hypothesis_id",
]
