import requests
import time
from datetime import datetime, timezone
import click
import traceback
from typing import List

from dbt_remote.src.dbt_remote.server_response_classes import DbtResponseLogs, DbtResponseRunStatus, FollowUpLink


def stream_logs(links: List[FollowUpLink], auth_session: requests.Session) -> ():
    run_status_link = get_link_from_action_name(links, "run_status")
    last_logs_link = get_link_from_action_name(links, "last_logs")
    run_status = get_run_status(run_status_link, auth_session).run_status

    stop = False
    while run_status == "running":
        time.sleep(1)
        run_status = get_run_status(run_status_link, auth_session).run_status
        stop = show_last_logs(last_logs_link, auth_session)
        if stop:
            return

    if run_status == "success":
        while not stop:
            time.sleep(1)
            stop = show_last_logs(last_logs_link, auth_session)
    else:
        show_last_logs(last_logs_link, auth_session)
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException("Job failed")
    return


def get_link_from_action_name(links: List[FollowUpLink], action_name: str) -> str:
    for follow_up_link in links:
        if follow_up_link.action_name == action_name:
            return follow_up_link.link
    raise click.ClickException('Error in parsing server response: no link for action name {action_name}')


def get_run_status(run_status_link: str, auth_session: requests.Session) -> DbtResponseRunStatus:
    res = auth_session.get(url=run_status_link)

    try:
        results = DbtResponseRunStatus.parse_raw(res.text)
        results.status_code = res.status_code
        return results
    except Exception:
        traceback_str = traceback.format_exc()
        raise click.ClickException("Error in parsing: " + traceback_str + "\n Original message: " + res.text)


def show_last_logs(last_logs_link: str, auth_session: requests.Session) -> bool:

    logs = get_last_logs(last_logs_link, auth_session).run_logs

    for log in logs:
        show_log(log)
        if "END JOB" in log:
            return True
    return False


def get_last_logs(last_logs_link: str, auth_session: requests.Session) -> DbtResponseLogs:
    res = auth_session.get(url=last_logs_link)

    try:
        results = DbtResponseLogs.parse_raw(res.text)
        results.status_code = res.status_code
        return results
    except Exception:
        traceback_str = traceback.format_exc()
        raise click.ClickException("Error in parsing: " + traceback_str + "\n Original message: " + res.text)


def show_log(log: str) -> ():

    parsed_log = parse_log(log)
    if parsed_log is None:
        return

    log_level, log_content = parsed_log

    if log_content == '':
        click.echo('')
        return

    log_content_color = 'reset'
    match (log_level):
        case 'INFO':
            log_level_color = 'green'
            match(log_content[:5]):
                case '[dbt]':
                    log_content_color = 'reset'
                case '[job]':
                    log_content_color = 'blue'
                case _:
                    log_content_color = (128, 128, 128)  # gray
        case 'WARN':
            log_level_color = 'yellow'
        case 'ERROR':
            log_level_color = 'red'
        case _:
            log_level_color = 'black'

    click.echo(click.style(log_level, fg=log_level_color) + '\t' + click.style(log_content, fg=log_content_color))


def parse_log(log: str) -> (tuple[str, str] | None):
    if log == '':
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
