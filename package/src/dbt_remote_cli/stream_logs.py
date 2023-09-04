import requests
import time
from datetime import datetime, timezone
import click

from dbt_remote_cli.server_response_classes import DbtResponseLogs, DbtResponseRunStatus


def stream_logs(server_url: str, uuid: str):
    run_status = get_run_status(server_url, uuid).run_status

    while run_status == "running":
        time.sleep(1)
        run_status = get_run_status(server_url, uuid).run_status
        stop = show_last_logs(server_url, uuid)

    if run_status == "success":
        while not stop:
            time.sleep(1)
            stop = show_last_logs(server_url, uuid)
    else:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException("Job failed")


def get_run_status(server_url: str, uuid: str) -> DbtResponseRunStatus:
    url = server_url + "job/" + uuid
    res = requests.get(url=url)

    results = DbtResponseRunStatus.parse_raw(res.text)
    results.status_code = res.status_code
    return results


def show_last_logs(server_url: str, uuid: str) -> bool:

    logs = get_last_logs(server_url, uuid).run_logs

    for log in logs:
        show_log(log)
        if "END JOB" in log:
            return True
    return False


def get_last_logs(server_url: str, uuid: str) -> DbtResponseLogs:
    url = server_url + "job/" + uuid + '/last_logs'
    res = requests.get(url=url)

    results = DbtResponseLogs.parse_raw(res.text)
    results.status_code = res.status_code
    return results


def show_log(log: str) -> ():

    parsed_log = parse_log(log)
    if parsed_log is None:
        return

    log_level, log_content = parsed_log

    if log_content == '':
        click.echo('')
        return

    match (log_level):
        case 'INFO':
            log_color = 'green'
        case 'WARN':
            log_color = 'yellow'
        case 'ERROR':
            log_color = 'red'
        case _:
            log_color = 'black'

    click.echo(click.style(log_level, fg=log_color) + '\t' + log_content)


def parse_log(log: str) -> (tuple[str, str] | None):
    if log == '':
        click.echo('')
        return

    parsed_log = log.split('\t')

    if len(parsed_log) < 3:
        click.echo(click.style("ERROR", fg="red") + '\t' + "Error in log parsing:")
        click.echo(log)
        return

    log_level = parsed_log[1]
    log_content = '  '.join(parsed_log[2:])

    return log_level, log_content


def current_time() -> str:
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt_string
