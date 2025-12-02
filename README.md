## Tradeinternal Candle MCP Server

This repository exposes the `tradingview_candle_data` MySQL table through a [Model Context Protocol](https://modelcontextprotocol.io) server implemented in Python. The server defines a single `get_candles` tool that can be invoked from any MCP-compatible client to pull historical OHLCV rows.

### Prerequisites

- Python 3.11+
- Access to the MySQL instance described in `.env`
- (Optional) Override columns/table through the `CANDLE_*` environment variables listed below

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

The `.env` file already contains credentials:

| Variable | Description |
| --- | --- |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | Standard MySQL connection arguments |
| `CANDLE_TABLE` | Override table name (defaults to `tradingview_candle_data`) |
| `CANDLE_SYMBOL_COLUMN` | Override symbol column name |
| `CANDLE_TIME_FRAME_COLUMN` | Override `time_frame` column name |
| `CANDLE_TIMESTAMP_COLUMN` | Override timestamp column used for filtering and ordering |
| `CANDLE_EXCHANGE_COLUMN` | Override exchange column name or set blank to disable exchange filtering |
| `FOOTPRINT_TABLE` | Override table name (defaults to `tradingview_volume_footprint`) |
| `FOOTPRINT_SYMBOL_COLUMN` | Override symbol column name for footprints |
| `FOOTPRINT_TIME_FRAME_COLUMN` | Override `time_frame` column name for footprints |
| `FOOTPRINT_TIMESTAMP_COLUMN` | Override timestamp column used for footprints |
| `FOOTPRINT_EXCHANGE_COLUMN` | Override exchange column name for footprints (leave unset/blank to disable exchange filtering) |
| `CVD_TABLE` | Override table name (defaults to `tradingview_candle_cvd`) |
| `CVD_SYMBOL_COLUMN` | Override symbol column name for CVD |
| `CVD_TIME_FRAME_COLUMN` | Override `time_frame` column name for CVD |
| `CVD_TIMESTAMP_COLUMN` | Override timestamp column used for CVD |
| `CVD_EXCHANGE_COLUMN` | Override exchange column name for CVD or set blank to disable exchange filtering |
| `EMA_TABLE` | Override table name (defaults to `tradingview_ema`) |
| `EMA_SYMBOL_COLUMN` | Override symbol column name for EMA |
| `EMA_TIME_FRAME_COLUMN` | Override `time_frame` column name for EMA |
| `EMA_TIMESTAMP_COLUMN` | Override timestamp column used for EMA |
| `EMA_EXCHANGE_COLUMN` | Override exchange column name for EMA or set blank to disable exchange filtering |

### Running the server (STDIO MCP)

```bash
python -m tradeinternal_mcp_server.server
```

`fastmcp` spins up a stdin/stdout MCP server. Point your MCP-compatible client (e.g., Claude Desktop) at the resulting executable entry point.

### Unified FastAPI + MCP (SSE)

If you prefer HTTP/SSE transport (e.g., Langflow running in Docker), use the bundled FastAPI app:

```bash
uvicorn tradeinternal_mcp_server.api:app --reload
```

- `GET /candles` matches the MCP tool arguments (`symbol`, `time_frame`, optional `exchange`, `limit`, `start_timestamp`, `end_timestamp`). Any HTTP client (Langflow HTTP block, curl, etc.) can consume candles directly.
- The app also mounts the MCP SSE transport at `/mcp/sse`. In Langflow’s **Add MCP Server** dialog choose **SSE**, set the URL to `http://<host>:9000/mcp/sse`, and ensure the environment variables (`DB_HOST`, etc.) are configured for that server entry or on the container itself.
- Running this single `uvicorn` process gives you both REST and MCP transports simultaneously.

### Tool: `get_candles`

| Argument | Required | Description |
| --- | --- | --- |
| `symbol` | ✅ | Instrument identifier stored in `CANDLE_SYMBOL_COLUMN` |
| `time_frame` | ✅ | Time frame stored in `CANDLE_TIME_FRAME_COLUMN` (e.g. `1`, `5`, `1D`) |
| `exchange` | optional | Exchange/venue filter if duplicates exist |
| `limit` | optional | Number of rows to return (1–1000, default 200) |
| `start_timestamp` | optional | Inclusive lower bound in `YYYY-MM-DD HH:MM:SS` |
| `end_timestamp` | optional | Inclusive upper bound in `YYYY-MM-DD HH:MM:SS` |

Responses include `start_timestamp` and `end_timestamp` metadata formatted as `YYYY-MM-DD HH:MM:SS`, along with a `count` and the `candles` array. Individual candles remain sorted chronologically (oldest to newest), with datetime columns converted to ISO-8601 strings and decimals converted to floats.

### Tool: `get_volume_footprint`

Fetches per-candle volume footprint metrics from the `tradingview_volume_footprint` table.

| Argument | Required | Description |
| --- | --- | --- |
| `symbol` | ✅ | Instrument identifier stored in `FOOTPRINT_SYMBOL_COLUMN` |
| `time_frame` | ✅ | Time frame stored in `FOOTPRINT_TIME_FRAME_COLUMN` (e.g. `1`, `5`, `1D`) |
| `exchange` | optional | Exchange/venue filter if duplicates exist |
| `limit` | optional | Number of rows to return (1–1000, default 200) |
| `start_timestamp` | optional | Inclusive lower bound in `YYYY-MM-DD HH:MM:SS` |
| `end_timestamp` | optional | Inclusive upper bound in `YYYY-MM-DD HH:MM:SS` |

Responses mirror `get_candles`: `start_timestamp`, `end_timestamp`, `count`, and a `footprints` array containing `poc`, `vah`, `val`, `volume_delta`, `levels`, and the timestamped symbol/time frame identifiers.

The FastAPI wrapper exposes this under `GET /volume-footprint` with matching query parameters. Responses include primary key (`fp_id`), `total_fp_volume`, `volume_diff`, and timestamps (`created_at`, `updated_at`) when present in the table.

`levels` is stored as JSON in the DB and is decoded to a Python object when possible; if decoding fails, the raw value is returned.

### Tool: `get_cvd`

Fetches cumulative volume delta (CVD) candle rows from the `tradingview_candle_cvd` table.

| Argument | Required | Description |
| --- | --- | --- |
| `symbol` | ✅ | Instrument identifier stored in `CVD_SYMBOL_COLUMN` |
| `time_frame` | ✅ | Time frame stored in `CVD_TIME_FRAME_COLUMN` (e.g. `1`, `5`, `1D`) |
| `exchange` | optional | Exchange/venue filter if duplicates exist |
| `limit` | optional | Number of rows to return (1–1000, default 200) |
| `start_timestamp` | optional | Inclusive lower bound in `YYYY-MM-DD HH:MM:SS` |
| `end_timestamp` | optional | Inclusive upper bound in `YYYY-MM-DD HH:MM:SS` |

Responses include `start_timestamp`, `end_timestamp`, `count`, and a `cvd` array containing `cvd_id`, `symbol`, `time_frame`, `timestamp`, OHLC values, and color fields (`ohlc_color`, `wick_color`, `border_color`). The FastAPI wrapper exposes this under `GET /cvd` with matching query parameters.

### Tool: `get_ema`

Fetches EMA values from the `tradingview_ema` table.

| Argument | Required | Description |
| --- | --- | --- |
| `symbol` | ✅ | Instrument identifier stored in `EMA_SYMBOL_COLUMN` |
| `time_frame` | ✅ | Time frame stored in `EMA_TIME_FRAME_COLUMN` (e.g. `1`, `5`, `1D`) |
| `exchange` | optional | Exchange/venue filter if duplicates exist |
| `limit` | optional | Number of rows to return (1–1000, default 200) |
| `start_timestamp` | optional | Inclusive lower bound in `YYYY-MM-DD HH:MM:SS` |
| `end_timestamp` | optional | Inclusive upper bound in `YYYY-MM-DD HH:MM:SS` |

Responses include `start_timestamp`, `end_timestamp`, `count`, and an `ema` array with `e_id`, `symbol`, `time_frame`, `timestamp`, `date_time`, and EMA columns (20/50/100/200). The FastAPI wrapper exposes this under `GET /ema` with matching query parameters.

### Tool: `get_current_date`

Returns the current server date in `YYYY-MM-DD` format. Useful for dynamically building time filters when calling the other tools.

### Tool: `compare_dates`

Compare a requested date to a provided current date and return whether the requested date is `past`, `today`, or `future`. Both dates must be supplied in `YYYY-MM-DD` format.
