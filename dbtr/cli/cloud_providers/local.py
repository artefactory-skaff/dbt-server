import os
from pathlib import Path
import pkg_resources

import uvicorn


def deploy(port: int, adapter: str, log_level: str = "INFO"):
    Path("./dbt-server-volume").mkdir(parents=True, exist_ok=True)
    os.environ["LOG_LEVEL"] = log_level
    os.environ["DBT_ADAPTER"] = "dbt-bigquery"
    os.environ["PROVIDER"] = "local"

    installed_packages = {pkg.key for pkg in pkg_resources.working_set}
    if adapter not in installed_packages:
        os.system(f"pip install {adapter}")

    uvicorn.run(
        "dbtr.server.main:app",
        host="0.0.0.0",
        port=port,
        workers=1,
        reload=False, # TODO: figure out how to relaod while ignoring dbt-server-volume
    )
