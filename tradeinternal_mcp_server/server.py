"""Entry-point for the TradingView candle MCP server."""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Any, Optional, Union

from fastmcp import FastMCP
from typing_extensions import TypedDict

from .config import DatabaseConfig
from .database import DatabaseClient
from .repository import CandleCvdRepository, CandleRepository, EmaRepository, VolumeFootprintRepository


class CandleRow(TypedDict, total=False):
    data_id: int
    exchange: str
    symbol: str
    time_frame: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: str
    date_time: str


class CandleResponse(TypedDict):
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    count: int
    candles: list[CandleRow]


class VolumeFootprintRow(TypedDict, total=False):
    fp_id: int
    exchange: str
    symbol: str
    time_frame: str
    timestamp: str
    poc: float
    vah: float
    val: float
    volume_delta: float
    total_fp_volume: float
    volume_diff: float
    levels: Union[dict[str, Any], list[Any], str, None]
    created_at: str
    updated_at: str


class VolumeFootprintResponse(TypedDict):
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    count: int
    footprints: list[VolumeFootprintRow]


class CvdRow(TypedDict, total=False):
    cvd_id: int
    exchange: str
    symbol: str
    time_frame: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    ohlc_color: int
    wick_color: int
    border_color: int


class CvdResponse(TypedDict):
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    count: int
    cvd: list[CvdRow]


class EmaRow(TypedDict, total=False):
    e_id: int
    exchange: str
    symbol: str
    time_frame: str
    timestamp: str
    date_time: str
    _20_ema: float
    _50_ema: float
    _100_ema: float
    _200_ema: float


class EmaResponse(TypedDict):
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    count: int
    ema: list[EmaRow]


server = FastMCP(
    "tradingview-candle-server",
    instructions=(
        "Retrieve historical candles from the tradingview_candle_data table. "
        "Use get_candles to pull OHLCV rows for a given symbol/time frame. "
        "Use get_volume_footprint to retrieve POC/VAH/VAL/volume delta, levels, totals, and diffs from tradingview_volume_footprint. "
        "Use get_cvd to retrieve CVD candles from tradingview_candle_cvd. "
        "Use get_ema to retrieve EMA values from tradingview_ema. "
        "Timestamps are returned as ISO-8601 strings and the data is sorted "
        "chronologically."
    ),
)

_repository: CandleRepository | None = None
_volume_repository: VolumeFootprintRepository | None = None
_cvd_repository: CandleCvdRepository | None = None
_ema_repository: EmaRepository | None = None


def get_repository() -> CandleRepository:
    global _repository
    if _repository is None:
        config = DatabaseConfig.from_env()
        db_client = DatabaseClient(config)
        _repository = CandleRepository.from_env(db_client)
    return _repository


def get_volume_repository() -> VolumeFootprintRepository:
    global _volume_repository
    if _volume_repository is None:
        config = DatabaseConfig.from_env()
        db_client = DatabaseClient(config)
        _volume_repository = VolumeFootprintRepository.from_env(db_client)
    return _volume_repository


def get_cvd_repository() -> CandleCvdRepository:
    global _cvd_repository
    if _cvd_repository is None:
        config = DatabaseConfig.from_env()
        db_client = DatabaseClient(config)
        _cvd_repository = CandleCvdRepository.from_env(db_client)
    return _cvd_repository


def get_ema_repository() -> EmaRepository:
    global _ema_repository
    if _ema_repository is None:
        config = DatabaseConfig.from_env()
        db_client = DatabaseClient(config)
        _ema_repository = EmaRepository.from_env(db_client)
    return _ema_repository


@server.tool()
def get_candles(
    symbol: Annotated[str, "Ticker or instrument identifier exactly as stored in the DB."],
    time_frame: Annotated[
        str,
        "Time frame value stored in the time_frame column (e.g. 1, 5, 60, 1D). '30' is normalized to '30m'.",
    ],
    limit: Annotated[int, "Maximum number of rows to return (1-1000)."] = 200,
    exchange: Annotated[Optional[str], "Optional exchange to filter if multiple venues store the same symbol."] = None,
    start_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp filter in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
    end_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp upper bound in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
) -> CandleResponse:
    """Fetch OHLCV candles for the given symbol/resolution."""

    repository = get_repository()
    rows = repository.fetch_candles(
        symbol=symbol,
        time_frame=time_frame,
        limit=limit,
        exchange=exchange,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return format_candle_response(rows)


@server.tool()
def get_volume_footprint(
    symbol: Annotated[str, "Ticker or instrument identifier exactly as stored in the DB."],
    time_frame: Annotated[
        str,
        "Time frame value stored in the time_frame column (e.g. 1, 5, 60, 1D). '30' is normalized to '30m'.",
    ],
    limit: Annotated[int, "Maximum number of rows to return (1-1000)."] = 200,
    exchange: Annotated[Optional[str], "Optional exchange to filter if multiple venues store the same symbol."] = None,
    start_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp filter in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
    end_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp upper bound in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
) -> VolumeFootprintResponse:
    """Fetch volume footprint data for the given symbol/resolution."""

    repository = get_volume_repository()
    rows = repository.fetch_volume_footprints(
        symbol=symbol,
        time_frame=time_frame,
        limit=limit,
        exchange=exchange,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return format_volume_footprint_response(rows)


@server.tool()
def get_cvd(
    symbol: Annotated[str, "Ticker or instrument identifier exactly as stored in the DB."],
    time_frame: Annotated[
        str,
        "Time frame value stored in the time_frame column (e.g. 1, 5, 60, 1D). '30' is normalized to '30m'.",
    ],
    limit: Annotated[int, "Maximum number of rows to return (1-1000)."] = 200,
    exchange: Annotated[Optional[str], "Optional exchange to filter if multiple venues store the same symbol."] = None,
    start_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp filter in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
    end_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp upper bound in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
) -> CvdResponse:
    """Fetch cumulative volume delta (CVD) candles for the given symbol/resolution."""

    repository = get_cvd_repository()
    rows = repository.fetch_cvd(
        symbol=symbol,
        time_frame=time_frame,
        limit=limit,
        exchange=exchange,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return format_cvd_response(rows)


@server.tool()
def get_ema(
    symbol: Annotated[str, "Ticker or instrument identifier exactly as stored in the DB."],
    time_frame: Annotated[
        str,
        "Time frame value stored in the time_frame column (e.g. 1, 5, 60, 1D). '30' is normalized to '30m'.",
    ],
    limit: Annotated[int, "Maximum number of rows to return (1-1000)."] = 200,
    exchange: Annotated[Optional[str], "Optional exchange to filter if multiple venues store the same symbol."] = None,
    start_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp filter in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
    end_timestamp: Annotated[
        Optional[str],
        "Inclusive timestamp upper bound in 'YYYY-MM-DD HH:MM:SS' format.",
    ] = None,
) -> EmaResponse:
    """Fetch EMA values for the given symbol/resolution."""

    repository = get_ema_repository()
    rows = repository.fetch_ema(
        symbol=symbol,
        time_frame=time_frame,
        limit=limit,
        exchange=exchange,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return format_ema_response(rows)


def _format_timestamp(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt_value = dt.datetime.fromisoformat(value)
    except ValueError:
        return value
    return dt_value.strftime("%Y-%m-%d %H:%M:%S")


def format_candle_response(rows: list[CandleRow]) -> CandleResponse:
    start_iso = rows[0]["timestamp"] if rows else None
    end_iso = rows[-1]["timestamp"] if rows else None

    return {
        "start_timestamp": _format_timestamp(start_iso),
        "end_timestamp": _format_timestamp(end_iso),
        "count": len(rows),
        "candles": rows,
    }


def format_volume_footprint_response(rows: list[VolumeFootprintRow]) -> VolumeFootprintResponse:
    start_iso = rows[0]["timestamp"] if rows else None
    end_iso = rows[-1]["timestamp"] if rows else None

    return {
        "start_timestamp": _format_timestamp(start_iso),
        "end_timestamp": _format_timestamp(end_iso),
        "count": len(rows),
        "footprints": rows,
    }


def format_cvd_response(rows: list[CvdRow]) -> CvdResponse:
    start_iso = rows[0]["timestamp"] if rows else None
    end_iso = rows[-1]["timestamp"] if rows else None

    return {
        "start_timestamp": _format_timestamp(start_iso),
        "end_timestamp": _format_timestamp(end_iso),
        "count": len(rows),
        "cvd": rows,
    }


def format_ema_response(rows: list[EmaRow]) -> EmaResponse:
    start_iso = rows[0]["timestamp"] if rows else None
    end_iso = rows[-1]["timestamp"] if rows else None

    # Preserve original column names, but prefix numerics to keep valid keys in JSON and TypedDict.
    normalized_rows: list[EmaRow] = []
    for row in rows:
        normalized_rows.append(
            {
                **row,
                "_20_ema": row.get("20_ema"),  # type: ignore[literal-required]
                "_50_ema": row.get("50_ema"),  # type: ignore[literal-required]
                "_100_ema": row.get("100_ema"),  # type: ignore[literal-required]
                "_200_ema": row.get("200_ema"),  # type: ignore[literal-required]
            }
        )

    return {
        "start_timestamp": _format_timestamp(start_iso),
        "end_timestamp": _format_timestamp(end_iso),
        "count": len(rows),
        "ema": normalized_rows,
    }


if __name__ == "__main__":
    server.run()
