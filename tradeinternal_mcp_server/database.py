"""Simple database helpers for the MCP server."""

from __future__ import annotations

import contextlib
from typing import Any, Dict, Iterable, List

import pymysql
import pymysql.cursors

from .config import DatabaseConfig


class DatabaseClient:
    """Tiny wrapper around pymysql to keep connection logic in one place."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config

    @contextlib.contextmanager
    def connection(self) -> Iterable[pymysql.connections.Connection]:
        """Context manager that yields a fresh connection."""

        conn = pymysql.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        try:
            yield conn
        finally:
            conn.close()

    def fetch_all(self, sql: str, params: Iterable[Any]) -> List[Dict[str, Any]]:
        """Execute the select query and return all rows as dicts."""

        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return list(cursor.fetchall())
