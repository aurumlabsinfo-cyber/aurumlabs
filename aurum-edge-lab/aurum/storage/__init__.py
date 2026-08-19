"""Persistence layer."""

from .base import Database, decode_json, encode_json
from .repositories import (
    ExecutionRepository,
    MarketRepository,
    Repositories,
    ResearchRepository,
    StrategyRepository,
    SystemRepository,
    WalletRepository,
)
from .schema import TABLES, TABLES_BY_NAME, render_ddl
from .sqlite_db import SqliteDatabase, open_database

__all__ = [
    "Database",
    "SqliteDatabase",
    "open_database",
    "encode_json",
    "decode_json",
    "Repositories",
    "MarketRepository",
    "ResearchRepository",
    "StrategyRepository",
    "ExecutionRepository",
    "WalletRepository",
    "SystemRepository",
    "TABLES",
    "TABLES_BY_NAME",
    "render_ddl",
]
