from __future__ import annotations

import os
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from fastapi import FastAPI

from app.api.handlers import router as api_router


def create_app() -> FastAPI:
    app = FastAPI(title="Video Downloader API", version="1.1.0")
    app.include_router(api_router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.getenv("UVICORN_HOST", "0.0.0.0"),
        port=int(os.getenv("UVICORN_PORT", "8000")),
        reload=os.getenv("UVICORN_RELOAD", "false").lower() == "true",
    )
