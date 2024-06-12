from pathlib import Path

import uvicorn


def deploy(port: int, log_level: str = "INFO"):
    Path("./dbt-server-volume").mkdir(parents=True, exist_ok=True)
    uvicorn.run(
        "server.main:app",
        host="0.0.0.0",
        port=port,
        workers=1,
        reload=False, # TODO: figure out how to relaod while ignoring dbt-server-volume
    )
