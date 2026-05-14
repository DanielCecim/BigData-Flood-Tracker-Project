"""
FastAPI application for the UK Flood Warning Simulation.

Start:
    uvicorn simulation.app:app --port 8000

Then open http://localhost:8000
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "models"))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from config import Config
from simulation.engine import SimulationEngine

_engine = SimulationEngine()
_STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = Config()
    loop = asyncio.get_event_loop()
    # Load is CPU/IO-bound; run in executor so the event loop isn't blocked
    await loop.run_in_executor(None, _engine.load, config)
    yield


app = FastAPI(title="UK Flood Warning Simulation", lifespan=lifespan)


# ------------------------------------------------------------------ #
#  REST endpoints
# ------------------------------------------------------------------ #

@app.get("/stations")
async def get_stations():
    """All station metadata (lat/lon/label/threshold) for map initialisation."""
    return JSONResponse(_engine.get_station_metadata())


@app.get("/stats")
async def get_stats():
    """Pre-computed summary statistics for the full simulation period."""
    return JSONResponse(_engine.get_stats())


@app.get("/dates")
async def get_dates():
    """Sorted list of available simulation dates."""
    return JSONResponse({"dates": _engine.dates, "count": len(_engine.dates)})


# ------------------------------------------------------------------ #
#  WebSocket simulation stream
# ------------------------------------------------------------------ #

@app.websocket("/ws/simulate")
async def ws_simulate(websocket: WebSocket):
    """
    Streams day-by-day flood predictions to the browser.

    Client → Server control messages:
        {"action": "play"}
        {"action": "pause"}
        {"action": "speed", "value": 5.0}      # 0.25x – 20x
        {"action": "seek",  "date": "2023-06-01"}

    Server → Client frame messages:
        {"type": "frame", "date": "...", "day_index": N, "total_days": M, "stations": [...]}
        {"type": "end"}
        {"type": "error", "message": "..."}
    """
    await websocket.accept()

    playing = False
    speed = 1.0          # simulated days per real second
    BASE_DELAY = 1.0
    current_index = 0
    date_set = set(_engine.dates)
    total = len(_engine.dates)

    try:
        while True:
            # Non-blocking poll for control messages from the client
            try:
                msg = await asyncio.wait_for(websocket.receive_json(), timeout=0.05)
                action = msg.get("action", "")
                if action == "play":
                    playing = True
                elif action == "pause":
                    playing = False
                elif action == "speed":
                    speed = max(0.25, min(float(msg.get("value", 1.0)), 20.0))
                elif action == "seek":
                    date_str = msg.get("date", "")
                    if date_str in date_set:
                        current_index = _engine.dates.index(date_str)
            except asyncio.TimeoutError:
                pass  # no message — continue loop

            if not playing:
                await asyncio.sleep(0.1)
                continue

            if current_index >= total:
                await websocket.send_json({"type": "end"})
                playing = False
                continue

            date_str = _engine.dates[current_index]

            # Run CPU-bound LightGBM inference in a thread so asyncio isn't blocked
            loop = asyncio.get_event_loop()
            predictions = await loop.run_in_executor(
                None, _engine.get_day_predictions, date_str
            )

            await websocket.send_json({
                "type": "frame",
                "date": date_str,
                "day_index": current_index,
                "total_days": total,
                "stations": predictions,
            })

            current_index += 1
            await asyncio.sleep(BASE_DELAY / speed)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await websocket.send_json({"type": "error", "message": str(exc)})
        except Exception:
            pass


# ------------------------------------------------------------------ #
#  Static file serving
# ------------------------------------------------------------------ #

app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(_STATIC_DIR / "index.html")
