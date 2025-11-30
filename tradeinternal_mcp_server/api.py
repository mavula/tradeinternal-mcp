"""FastAPI application exposing the candle retrieval endpoint."""

from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query

from .config import DatabaseConfig
from .database import DatabaseClient
from .repository import CandleCvdRepository, CandleRepository, EmaRepository, VolumeFootprintRepository
from .server import (
    format_candle_response,
    format_cvd_response,
    format_ema_response,
    format_volume_footprint_response,
    server as mcp_server,
)


@lru_cache
def get_repository() -> CandleRepository:
    """Return a cached repository instance backed by env configuration."""

    config = DatabaseConfig.from_env()
    db_client = DatabaseClient(config)
    return CandleRepository.from_env(db_client)


@lru_cache
def get_volume_repository() -> VolumeFootprintRepository:
    """Return a cached footprint repository instance backed by env configuration."""

    config = DatabaseConfig.from_env()
    db_client = DatabaseClient(config)
    return VolumeFootprintRepository.from_env(db_client)


@lru_cache
def get_cvd_repository() -> CandleCvdRepository:
    """Return a cached CVD repository instance backed by env configuration."""

    config = DatabaseConfig.from_env()
    db_client = DatabaseClient(config)
    return CandleCvdRepository.from_env(db_client)


@lru_cache
def get_ema_repository() -> EmaRepository:
    """Return a cached EMA repository instance backed by env configuration."""

    config = DatabaseConfig.from_env()
    db_client = DatabaseClient(config)
    return EmaRepository.from_env(db_client)


app = FastAPI(
    title="TradingView Candle, Footprint & EMA API",
    description=(
        "HTTP wrapper around the tradingview_candle_data, tradingview_volume_footprint, "
        "tradingview_candle_cvd, and tradingview_ema MCP backend."
    ),
    version="1.0.0",
)

# Mount the FastMCP SSE transport under /mcp so Langflow or other HTTP clients
# can use the SSE-based MCP connection without launching a separate process.
app.mount("/mcp", mcp_server.http_app(path="/sse", transport="sse"), name="mcp")


@app.get("/candles")
def get_candles(
    symbol: str = Query(..., description="Instrument identifier stored in the symbol column."),
    time_frame: str = Query(
        ...,
        description="Time frame stored in the time_frame column (e.g., 1, 5, 1D). '30' is normalized to '30m'.",
    ),
    limit: int = Query(200, ge=1, le=1000, description="Number of rows to return (1-1000)."),
    exchange: Optional[str] = Query(None, description="Optional exchange filter when multiple venues exist."),
    start_timestamp: Optional[str] = Query(
        None,
        description="Inclusive lower bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    end_timestamp: Optional[str] = Query(
        None,
        description="Inclusive upper bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    repository: CandleRepository = Depends(get_repository),
) -> List[dict]:
    """HTTP endpoint for fetching candle data."""

    try:
        candles = repository.fetch_candles(
            symbol=symbol,
            time_frame=time_frame,
            limit=limit,
            exchange=exchange,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        return format_candle_response(candles)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/cvd")
def get_cvd(
    symbol: str = Query(..., description="Instrument identifier stored in the symbol column."),
    time_frame: str = Query(
        ...,
        description="Time frame stored in the time_frame column (e.g., 1, 5, 1D). '30' is normalized to '30m'.",
    ),
    limit: int = Query(200, ge=1, le=1000, description="Number of rows to return (1-1000)."),
    exchange: Optional[str] = Query(None, description="Optional exchange filter when multiple venues exist."),
    start_timestamp: Optional[str] = Query(
        None,
        description="Inclusive lower bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    end_timestamp: Optional[str] = Query(
        None,
        description="Inclusive upper bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    repository: CandleCvdRepository = Depends(get_cvd_repository),
) -> List[dict]:
    """HTTP endpoint for fetching CVD data."""

    try:
        cvd_rows = repository.fetch_cvd(
            symbol=symbol,
            time_frame=time_frame,
            limit=limit,
            exchange=exchange,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        return format_cvd_response(cvd_rows)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/ema")
def get_ema(
    symbol: str = Query(..., description="Instrument identifier stored in the symbol column."),
    time_frame: str = Query(
        ...,
        description="Time frame stored in the time_frame column (e.g., 1, 5, 1D). '30' is normalized to '30m'.",
    ),
    limit: int = Query(200, ge=1, le=1000, description="Number of rows to return (1-1000)."),
    exchange: Optional[str] = Query(None, description="Optional exchange filter when multiple venues exist."),
    start_timestamp: Optional[str] = Query(
        None,
        description="Inclusive lower bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    end_timestamp: Optional[str] = Query(
        None,
        description="Inclusive upper bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    repository: EmaRepository = Depends(get_ema_repository),
) -> List[dict]:
    """HTTP endpoint for fetching EMA data."""

    try:
        ema_rows = repository.fetch_ema(
            symbol=symbol,
            time_frame=time_frame,
            limit=limit,
            exchange=exchange,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        return format_ema_response(ema_rows)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/volume-footprint")
def get_volume_footprint(
    symbol: str = Query(..., description="Instrument identifier stored in the symbol column."),
    time_frame: str = Query(
        ...,
        description="Time frame stored in the time_frame column (e.g., 1, 5, 1D). '30' is normalized to '30m'.",
    ),
    limit: int = Query(200, ge=1, le=1000, description="Number of rows to return (1-1000)."),
    exchange: Optional[str] = Query(None, description="Optional exchange filter when multiple venues exist."),
    start_timestamp: Optional[str] = Query(
        None,
        description="Inclusive lower bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    end_timestamp: Optional[str] = Query(
        None,
        description="Inclusive upper bound in 'YYYY-MM-DD HH:mm:ss'.",
    ),
    repository: VolumeFootprintRepository = Depends(get_volume_repository),
) -> List[dict]:
    """HTTP endpoint for fetching volume footprint data."""

    try:
        footprints = repository.fetch_volume_footprints(
            symbol=symbol,
            time_frame=time_frame,
            limit=limit,
            exchange=exchange,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        return format_volume_footprint_response(footprints)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
