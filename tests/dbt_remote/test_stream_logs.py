from dbt_remote.server_response_classes import FollowUpLink
from dbt_remote.stream_logs import (
    get_last_logs,
    get_link_from_action_name,
    get_run_status,
    parse_log,
    show_last_logs,
)


def test_parse_log():
    log_dict = {
        "": None,
        "timestamp": None,
        "timstamp\tlog content": None,
        "timstamp\tSEVERITY\tlog content": ("SEVERITY", "log content"),
        "timstamp\tSEVERITY\tlog content\t+1": ("SEVERITY", "log content  +1"),
    }
    for log in log_dict.keys():
        computed_log = parse_log(log)
        expected_log = log_dict[log]
        assert computed_log == expected_log


def test_get_link_from_action_name():
    links = [
        FollowUpLink(action_name="action", link="link-url"),
        FollowUpLink(action_name="other-action", link="other-link-url"),
    ]
    link_result = get_link_from_action_name(links, "action")
    assert link_result == links[0].link
    link_result = get_link_from_action_name(links, "other-action")
    assert link_result == links[1].link


def test_get_run_status(requests_mock):
    run_status_url = "https://test-server.test/run_status"
    run_status_json = {"run_status": "started"}
    request_mock = requests_mock.get(run_status_url, status_code=200, json=run_status_json)

    run_status_results = get_run_status(run_status_url)

    assert request_mock.last_request.method == "GET"
    assert request_mock.last_request.url == run_status_url
    assert run_status_results.status_code == 200
    assert run_status_results.run_status == run_status_json["run_status"]


def test_get_last_logs(requests_mock):
    logs_url = "https://test-server.test/last_logs"
    logs = {"run_logs": ["log1", "log2", "log3"]}
    request_mock = requests_mock.get(logs_url, status_code=200, json=logs)

    logs_result = get_last_logs(logs_url)

    assert request_mock.last_request.method == "GET"
    assert request_mock.last_request.url == logs_url
    assert logs_result.status_code == 200
    assert logs_result.run_logs == logs["run_logs"]


def test_show_last_logs(requests_mock):
    logs_url = "https://test-server.test/last_logs"
    logs = {"run_logs": ["log1", "log2", "log3"]}
    requests_mock.get(logs_url, status_code=200, json=logs)
    assert not show_last_logs(logs_url)

    logs_url = "https://test-server.test/empty_logs"
    logs = {"run_logs": []}
    requests_mock.get(logs_url, status_code=200, json=logs)
    assert not show_last_logs(logs_url)

    logs_url = "https://test-server.test/end_job"
    logs = {"run_logs": ["log1", "log2", "log3", "END JOB"]}
    requests_mock.get(logs_url, status_code=200, json=logs)
    assert show_last_logs(logs_url)
