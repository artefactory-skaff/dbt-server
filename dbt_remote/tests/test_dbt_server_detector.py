import requests
from src.dbt_remote.dbt_server_detector import check_if_server_is_dbt_server
from unittest.mock import Mock


def test_check_if_server_is_dbt_server(requests_mock):
    server_url = "https://test-server.test"
    service_mock = Mock(name="service_mock")
    service_mock.uri = server_url

    check_list = [
        {
            "url": server_url+'/check',
            "status_code": 200,
            "json": {'response': 'Running dbt-server on port 8001'},
            "is_dbt_server": True
        },
        {
            "url": server_url+'/check',
            "status_code": 201,
            "json": {'response': 'Running dbt-server on port 8001'},
            "is_dbt_server": False
        },
        {
            "url": server_url+'/check',
            "status_code": 200,
            "json": {'response': 'other msg'},
            "is_dbt_server": False
        },
        {
            "url": server_url+'/check',
            "status_code": 200,
            "json": {'other key': 'other msg'},
            "is_dbt_server": False
        },
        {
            "url": server_url+'/check',
            "status_code": 200,
            "json": "not a json",
            "is_dbt_server": False
        },
    ]
    session = requests.Session()
    auth_headers = {"Authorization": "Bearer 1234"}
    session.headers.update(auth_headers)

    for check in check_list:
        request_mock = requests_mock.get(check["url"], status_code=check["status_code"], json=check["json"])
        assert check_if_server_is_dbt_server(service_mock, session) == check["is_dbt_server"]
        assert request_mock.last_request.method == 'GET'
        assert request_mock.last_request.url == check["url"]
        assert request_mock.last_request.headers['Authorization'] == auth_headers['Authorization']
