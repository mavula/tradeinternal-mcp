from typing import List
from tradeinternal_mcp_server.server import get_candles

def run(symbol: str, time_frame: str, limit: int = 200, exchange: str | None = None) -> List[dict]:
    return get_candles(symbol=symbol, time_frame=time_frame, limit=limit, exchange=exchange)
