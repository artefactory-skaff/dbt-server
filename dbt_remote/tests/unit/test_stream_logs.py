import requests
from src.dbt_remote.stream_logs import parse_log, get_last_logs, show_last_logs
from src.dbt_remote.stream_logs import get_run_status


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


def test_get_run_status(requests_mock):

    run_status_url = "https://test-server.test/run_status"
    run_status_json = {"run_status": "started"}
    request_mock = requests_mock.get(run_status_url, status_code=200, json=run_status_json)

    session = requests.Session()
    auth_headers = {"Authorization": "Bearer 1234"}
    session.headers.update(auth_headers)

    run_status_results = get_run_status(run_status_url, session)

    assert request_mock.last_request.method == 'GET'
    assert request_mock.last_request.url == run_status_url
    assert request_mock.last_request.headers['Authorization'] == auth_headers['Authorization']
    assert run_status_results.status_code == 200
    assert run_status_results.run_status == run_status_json["run_status"]


def test_get_last_logs(requests_mock):

    logs_url = "https://test-server.test/last_logs"
    logs = {"run_logs": ["log1", "log2", "log3"]}
    request_mock = requests_mock.get(logs_url, status_code=200, json=logs)

    session = requests.Session()
    auth_headers = {"Authorization": "Bearer 1234"}
    session.headers.update(auth_headers)

    logs_result = get_last_logs(logs_url, session)

    assert request_mock.last_request.method == 'GET'
    assert request_mock.last_request.url == logs_url
    assert request_mock.last_request.headers['Authorization'] == auth_headers['Authorization']
    assert logs_result.status_code == 200
    assert logs_result.run_logs == logs["run_logs"]


def test_show_last_logs(requests_mock):

    logs_url = "https://test-server.test/last_logs"
    logs = {"run_logs": ["log1", "log2", "log3"]}
    requests_mock.get(logs_url, status_code=200, json=logs)
    session = requests.Session()
    auth_headers = {"Authorization": "Bearer 1234"}
    session.headers.update(auth_headers)

    assert not show_last_logs(logs_url, session)

    logs_url = "https://test-server.test/empty_logs"
    logs = {"run_logs": []}
    requests_mock.get(logs_url, status_code=200, json=logs)
    assert not show_last_logs(logs_url, session)

    logs_url = "https://test-server.test/end_job"
    logs = {"run_logs": ["log1", "log2", "log3", "dbt-remote job finished"]}
    requests_mock.get(logs_url, status_code=200, json=logs)
    assert show_last_logs(logs_url, session)
