"""Data-access helpers for fetching candle data."""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import re
from typing import Any, Dict, List, Optional

from .database import DatabaseClient

ALLOWED_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


def _sanitize_identifier(value: str, default: str) -> str:
    candidate = value or default
    if not ALLOWED_IDENTIFIER.match(candidate):
        raise ValueError(f"Invalid identifier provided: {candidate}")
    return candidate


def _normalize_time_frame(value: str) -> str:
    """Coerce incoming time frame values to match stored formats."""

    if vahalue == "1":
        return "1m"
    elif value == "5":
        return "5m"
    elif value == "15":
        return "15m"
    elif value == "30":
        return "30m"
    elif value == "45": 
        return "45m"    
    elif value == "60":
        return "1H"
    
    return value


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized: Dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, dt.datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, dt.date):
            serialized[key] = value.isoformat()
        elif isinstance(value, decimal.Decimal):
            serialized[key] = float(value)
        else:
            serialized[key] = value
    return serialized


class CandleRepository:
    """Handles building SQL queries for tradingview_candle_data."""

    def __init__(
        self,
        db: DatabaseClient,
        table: str = "tradingview_candle_data",
        symbol_column: str = "symbol",
        time_frame_column: str = "time_frame",
        timestamp_column: str = "timestamp",
        exchange_column: Optional[str] = "exchange",
    ) -> None:
        self._db = db
        self._table = _sanitize_identifier(table, "tradingview_candle_data")
        self._symbol_column = _sanitize_identifier(symbol_column, "symbol")
        self._time_frame_column = _sanitize_identifier(time_frame_column, "time_frame")
        self._timestamp_column = _sanitize_identifier(timestamp_column, "timestamp")
        self._exchange_column = (
            _sanitize_identifier(exchange_column, "exchange") if exchange_column else None
        )

    @classmethod
    def from_env(cls, db: DatabaseClient) -> "CandleRepository":
        """Build repository using optional env overrides."""

        return cls(
            db=db,
            table=os.getenv("CANDLE_TABLE", "tradingview_candle_data"),
            symbol_column=os.getenv("CANDLE_SYMBOL_COLUMN", "symbol"),
            time_frame_column=os.getenv("CANDLE_TIME_FRAME_COLUMN", "time_frame"),
            timestamp_column=os.getenv("CANDLE_TIMESTAMP_COLUMN", "timestamp"),
            exchange_column=os.getenv("CANDLE_EXCHANGE_COLUMN", "exchange"),
        )

    def fetch_candles(
        self,
        symbol: str,
        time_frame: str,
        limit: int,
        exchange: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return candle rows with optional time filters."""

        limit = max(1, min(limit, 1000))
        time_frame = _normalize_time_frame(time_frame)

        sql_parts = [
            f"SELECT time_frame, symbol, {self._timestamp_column}, open, high, low, close, volume FROM {self._table}",
            "WHERE {symbol_col} = %s AND {time_frame_col} = %s".format(
                symbol_col=self._symbol_column,
                time_frame_col=self._time_frame_column,
            ),
        ]
        params: List[Any] = [symbol, time_frame]

        if exchange is not None:
            if not self._exchange_column:
                raise ValueError("Exchange filtering requested but no exchange column configured.")
            sql_parts.append(f"AND {self._exchange_column} = %s")
            params.append(exchange)

        if start_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} >= %s")
            params.append(start_timestamp)
        if end_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} <= %s")
            params.append(end_timestamp)

        sql_parts.append(f"ORDER BY {self._timestamp_column} DESC")
        sql_parts.append("LIMIT %s")
        params.append(limit)

        rows = self._db.fetch_all(" ".join(sql_parts), params)
        # Reverse to ascending chronological order for easier consumption.
        rows.reverse()
        return [_serialize_row(row) for row in rows]


class VolumeFootprintRepository:
    """Builds queries for tradingview_volume_footprint rows."""

    def __init__(
        self,
        db: DatabaseClient,
        table: str = "tradingview_volume_footprint",
        symbol_column: str = "symbol",
        time_frame_column: str = "time_frame",
        timestamp_column: str = "timestamp",
        exchange_column: Optional[str] = None,
    ) -> None:
        self._db = db
        self._table = _sanitize_identifier(table, "tradingview_volume_footprint")
        self._symbol_column = _sanitize_identifier(symbol_column, "symbol")
        self._time_frame_column = _sanitize_identifier(time_frame_column, "time_frame")
        self._timestamp_column = _sanitize_identifier(timestamp_column, "timestamp")
        self._exchange_column = (
            _sanitize_identifier(exchange_column, "exchange") if exchange_column else None
        )

    @classmethod
    def from_env(cls, db: DatabaseClient) -> "VolumeFootprintRepository":
        """Build repository using optional env overrides."""

        return cls(
            db=db,
            table=os.getenv("FOOTPRINT_TABLE", "tradingview_volume_footprint"),
            symbol_column=os.getenv("FOOTPRINT_SYMBOL_COLUMN", "symbol"),
            time_frame_column=os.getenv("FOOTPRINT_TIME_FRAME_COLUMN", "time_frame"),
            timestamp_column=os.getenv("FOOTPRINT_TIMESTAMP_COLUMN", "timestamp"),
            exchange_column=os.getenv("FOOTPRINT_EXCHANGE_COLUMN"),
        )

    def fetch_volume_footprints(
        self,
        symbol: str,
        time_frame: str,
        limit: int,
        exchange: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return volume footprint rows with optional time filters."""

        limit = max(1, min(limit, 1000))
        time_frame = _normalize_time_frame(time_frame)

        select_columns = [
            "fp_id",
            f"{self._time_frame_column} AS time_frame",
            f"{self._symbol_column} AS symbol",
            f"{self._timestamp_column} AS timestamp",
        ]
        if self._exchange_column:
            select_columns.append(f"{self._exchange_column} AS exchange")
        select_columns.extend(
            [
                "poc",
                "vah",
                "val",
                "volume_delta",
                "levels",
                "total_fp_volume",
                "volume_diff",
                "created_at",
                "updated_at",
            ]
        )

        sql_parts = [
            f"SELECT {', '.join(select_columns)} FROM {self._table}",
            "WHERE {symbol_col} = %s AND {time_frame_col} = %s".format(
                symbol_col=self._symbol_column,
                time_frame_col=self._time_frame_column,
            ),
        ]
        params: List[Any] = [symbol, time_frame]

        if exchange is not None:
            if not self._exchange_column:
                raise ValueError("Exchange filtering requested but no exchange column configured.")
            sql_parts.append(f"AND {self._exchange_column} = %s")
            params.append(exchange)

        if start_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} >= %s")
            params.append(start_timestamp)
        if end_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} <= %s")
            params.append(end_timestamp)

        sql_parts.append(f"ORDER BY {self._timestamp_column} DESC")
        sql_parts.append("LIMIT %s")
        params.append(limit)

        rows = self._db.fetch_all(" ".join(sql_parts), params)
        rows.reverse()

        serialized_rows: List[Dict[str, Any]] = []
        for row in rows:
            serialized = _serialize_row(row)
            levels_value = serialized.get("levels")
            if isinstance(levels_value, (bytes, bytearray)):
                try:
                    levels_value = levels_value.decode("utf-8")
                    serialized["levels"] = levels_value
                except UnicodeDecodeError:
                    pass
            if isinstance(levels_value, str):
                try:
                    serialized["levels"] = json.loads(levels_value)
                except ValueError:
                    # Leave as-is if not JSON encoded.
                    pass
            serialized_rows.append(serialized)

        return serialized_rows


class CandleCvdRepository:
    """Builds queries for tradingview_candle_cvd rows."""

    def __init__(
        self,
        db: DatabaseClient,
        table: str = "tradingview_candle_cvd",
        symbol_column: str = "symbol",
        time_frame_column: str = "time_frame",
        timestamp_column: str = "timestamp",
        exchange_column: Optional[str] = "exchange",
    ) -> None:
        self._db = db
        self._table = _sanitize_identifier(table, "tradingview_candle_cvd")
        self._symbol_column = _sanitize_identifier(symbol_column, "symbol")
        self._time_frame_column = _sanitize_identifier(time_frame_column, "time_frame")
        self._timestamp_column = _sanitize_identifier(timestamp_column, "timestamp")
        self._exchange_column = (
            _sanitize_identifier(exchange_column, "exchange") if exchange_column else None
        )

    @classmethod
    def from_env(cls, db: DatabaseClient) -> "CandleCvdRepository":
        """Build repository using optional env overrides."""

        return cls(
            db=db,
            table=os.getenv("CVD_TABLE", "tradingview_candle_cvd"),
            symbol_column=os.getenv("CVD_SYMBOL_COLUMN", "symbol"),
            time_frame_column=os.getenv("CVD_TIME_FRAME_COLUMN", "time_frame"),
            timestamp_column=os.getenv("CVD_TIMESTAMP_COLUMN", "timestamp"),
            exchange_column=os.getenv("CVD_EXCHANGE_COLUMN", "exchange"),
        )

    def fetch_cvd(
        self,
        symbol: str,
        time_frame: str,
        limit: int,
        exchange: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return candle CVD rows with optional time filters."""

        limit = max(1, min(limit, 1000))
        time_frame = _normalize_time_frame(time_frame)

        select_columns = [
            "cvd_id",
            f"{self._exchange_column} AS exchange" if self._exchange_column else None,
            f"{self._symbol_column} AS symbol",
            f"{self._time_frame_column} AS time_frame",
            f"{self._timestamp_column} AS timestamp",
            "open",
            "high",
            "low",
            "close",
            "ohlc_color",
            "wick_color",
            "border_color",
        ]
        # filter out optional None
        select_columns = [col for col in select_columns if col is not None]

        sql_parts = [
            f"SELECT {', '.join(select_columns)} FROM {self._table}",
            "WHERE {symbol_col} = %s AND {time_frame_col} = %s".format(
                symbol_col=self._symbol_column,
                time_frame_col=self._time_frame_column,
            ),
        ]
        params: List[Any] = [symbol, time_frame]

        if exchange is not None:
            if not self._exchange_column:
                raise ValueError("Exchange filtering requested but no exchange column configured.")
            sql_parts.append(f"AND {self._exchange_column} = %s")
            params.append(exchange)

        if start_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} >= %s")
            params.append(start_timestamp)
        if end_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} <= %s")
            params.append(end_timestamp)

        sql_parts.append(f"ORDER BY {self._timestamp_column} DESC")
        sql_parts.append("LIMIT %s")
        params.append(limit)

        rows = self._db.fetch_all(" ".join(sql_parts), params)
        rows.reverse()
        return [_serialize_row(row) for row in rows]


class EmaRepository:
    """Builds queries for tradingview_ema rows."""

    def __init__(
        self,
        db: DatabaseClient,
        table: str = "tradingview_ema",
        symbol_column: str = "symbol",
        time_frame_column: str = "time_frame",
        timestamp_column: str = "timestamp",
        exchange_column: Optional[str] = "exchange",
    ) -> None:
        self._db = db
        self._table = _sanitize_identifier(table, "tradingview_ema")
        self._symbol_column = _sanitize_identifier(symbol_column, "symbol")
        self._time_frame_column = _sanitize_identifier(time_frame_column, "time_frame")
        self._timestamp_column = _sanitize_identifier(timestamp_column, "timestamp")
        self._exchange_column = (
            _sanitize_identifier(exchange_column, "exchange") if exchange_column else None
        )

    @classmethod
    def from_env(cls, db: DatabaseClient) -> "EmaRepository":
        """Build repository using optional env overrides."""

        return cls(
            db=db,
            table=os.getenv("EMA_TABLE", "tradingview_ema"),
            symbol_column=os.getenv("EMA_SYMBOL_COLUMN", "symbol"),
            time_frame_column=os.getenv("EMA_TIME_FRAME_COLUMN", "time_frame"),
            timestamp_column=os.getenv("EMA_TIMESTAMP_COLUMN", "timestamp"),
            exchange_column=os.getenv("EMA_EXCHANGE_COLUMN", "exchange"),
        )

    def fetch_ema(
        self,
        symbol: str,
        time_frame: str,
        limit: int,
        exchange: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return EMA rows with optional time filters."""

        limit = max(1, min(limit, 1000))
        time_frame = _normalize_time_frame(time_frame)

        select_columns = [
            "e_id",
            f"{self._exchange_column} AS exchange" if self._exchange_column else None,
            f"{self._symbol_column} AS symbol",
            f"{self._time_frame_column} AS time_frame",
            f"{self._timestamp_column} AS timestamp",
            "20_ema",
            "50_ema",
            "100_ema",
            "200_ema",
            "date_time",
        ]
        select_columns = [col for col in select_columns if col is not None]

        sql_parts = [
            f"SELECT {', '.join(select_columns)} FROM {self._table}",
            "WHERE {symbol_col} = %s AND {time_frame_col} = %s".format(
                symbol_col=self._symbol_column,
                time_frame_col=self._time_frame_column,
            ),
        ]
        params: List[Any] = [symbol, time_frame]

        if exchange is not None:
            if not self._exchange_column:
                raise ValueError("Exchange filtering requested but no exchange column configured.")
            sql_parts.append(f"AND {self._exchange_column} = %s")
            params.append(exchange)

        if start_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} >= %s")
            params.append(start_timestamp)
        if end_timestamp is not None:
            sql_parts.append(f"AND {self._timestamp_column} <= %s")
            params.append(end_timestamp)

        sql_parts.append(f"ORDER BY {self._timestamp_column} DESC")
        sql_parts.append("LIMIT %s")
        params.append(limit)

        rows = self._db.fetch_all(" ".join(sql_parts), params)
        rows.reverse()
        return [_serialize_row(row) for row in rows]
