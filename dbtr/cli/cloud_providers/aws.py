from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import List
from uuid import uuid4
from subprocess import check_output
import click

from rich.table import Table
from rich.console import Console

from dbtr.common.exceptions import MissingExtraPackage, MissingLocation, ServerNotFound
from dbtr.common.remote_server import DbtServer


def deploy(image: str, service_name: str, region: str, adapter: str, cpu: int = 1, memory: str = "1Gi", log_level: str = "INFO", auto_approve: bool = False):
    print("cpu", type(cpu), cpu)
    console = Console()
    table = Table(title="Deploying dbt server on AWS with the following configuration")

    table.add_column("Configuration", justify="right", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    table.add_row("Service Name", service_name)
    table.add_row("Region", region)
    table.add_row("Adapter", adapter)
    table.add_row("CPU", str(cpu))
    table.add_row("Memory", memory)
    table.add_row("Image", image)
    table.add_row("Log Level", log_level)

    console.print(table)
    if not auto_approve:
        click.confirm("Confirm deployment?", abort=True)

    console.print(f"[green]Deployed dbt server at [/green]")
    console.print(f"You can now run dbt jobs remotely with [cyan]dbtr remote debug --cloud-provider google --server-url [/cyan]")
