"""Research agents.

Five families, each with real inputs, a real algorithm, state, logs and metrics,
plus the post-mortem agent which runs only on failure.
"""

from __future__ import annotations

from ..config import Config
from ..research.memory import ResearchMemory
from ..storage.repositories import ResearchRepository
from .base import AgentContext, AgentMetrics, FeatureRank, ResearchAgent, slice_view
from .cross_crypto import CrossCryptoAgent
from .mean_reversion import MeanReversionAgent
from .microstructure import MicrostructureAgent
from .momentum import MomentumAgent
from .postmortem import Cause, CyclePostMortemAgent, PostMortem
from .relative_value import RelativeValueAgent

AGENT_CLASSES: tuple[type[ResearchAgent], ...] = (
    MicrostructureAgent,
    MomentumAgent,
    MeanReversionAgent,
    CrossCryptoAgent,
    RelativeValueAgent,
)


def build_agents(
    config: Config, memory: ResearchMemory, repo: ResearchRepository
) -> list[ResearchAgent]:
    return [cls(config, memory, repo) for cls in AGENT_CLASSES]


__all__ = [
    "AGENT_CLASSES",
    "AgentContext",
    "AgentMetrics",
    "Cause",
    "CrossCryptoAgent",
    "CyclePostMortemAgent",
    "FeatureRank",
    "MeanReversionAgent",
    "MicrostructureAgent",
    "MomentumAgent",
    "PostMortem",
    "RelativeValueAgent",
    "ResearchAgent",
    "build_agents",
    "slice_view",
]
