from src.dbt_remote.dbt_server_detector import deduce_target_from_profiles, get_metadata_from_profiles_dict
from src.dbt_remote.dbt_server_detector import get_selected_sub_command_conf_from_user_command
from src.dbt_remote.dbt_server_detector import check_if_server_is_dbt_server
from unittest.mock import Mock


def test_deduce_target_from_profiles():

    profile_dict = {'target': 'dev', 'other': 'something else'}
    assert deduce_target_from_profiles(profile_dict) == "dev"

    outputs_dict = {'default': 'default config', 'prod': 'prod config'}
    profile_dict = {'target': 'dev', 'outputs': outputs_dict}
    assert deduce_target_from_profiles(profile_dict) == "dev"

    outputs_dict = {'default': 'default config', 'prod': 'prod config'}
    profile_dict = {'outputs': outputs_dict}
    assert deduce_target_from_profiles(profile_dict) == "default"

    outputs_dict = {'dev': 'dev config'}
    profile_dict = {'outputs': outputs_dict}
    assert deduce_target_from_profiles(profile_dict) == "dev"


def test_get_metadata_from_profiles_dict():

    profiles_dict = {
        "profile1": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "key": "value",
                    "project": "projectid",
                    "location": "location"
                }
            }
        },
        "profile2": {
            "target": "prod",
            "outputs": {
                "dev": {
                    "key": "value",
                    "project": "projectid3",
                    "location": "location3"
                },
                "prod": {
                    "key": "value",
                    "project": "projectid2",
                    "location": "location2"
                }
            }
        }
    }
    profile, target = "profile1", "dev"
    project_id = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'project')
    location = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'location')
    assert project_id == "projectid"
    assert location == "location"

    profile, target = "profile2", "prod"
    project_id = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'project')
    location = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'location')
    assert project_id == "projectid2"
    assert location == "location2"


def test_get_selected_target_and_profile(MockDbtFileSystem):
    MockDbtFileSystem
    project_dir = " --project-dir /home/runner/work/dbt-server/dbt-server"
    commands_dict = {
        "run": {
            "target": None,
            "profile": None
        },
        "run --target mytarget": {
            "target": "mytarget",
            "profile": None
        },
        "run -t mytarget": {
            "target": "mytarget",
            "profile": None
        },
        "run --profile myprofile": {
            "target": None,
            "profile": "myprofile"
        },
        "run -t mytarget --profile myprofile": {
            "target": "mytarget",
            "profile": "myprofile"
        },
    }

    for command in commands_dict.keys():
        expected_target, expected_profile = commands_dict[command]["target"], commands_dict[command]["profile"]
        computed_target = get_selected_sub_command_conf_from_user_command(command+project_dir, 'target')
        computed_profile = get_selected_sub_command_conf_from_user_command(command+project_dir, 'profile')
        assert computed_target == expected_target
        assert computed_profile == expected_profile


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
    auth_headers = {"Authorization": "Bearer 1234"}

    for check in check_list:
        request_mock = requests_mock.get(check["url"], status_code=check["status_code"], json=check["json"])
        assert check_if_server_is_dbt_server(service_mock, auth_headers) == check["is_dbt_server"]
        assert request_mock.last_request.method == 'GET'
        assert request_mock.last_request.url == check["url"]
        assert request_mock.last_request.headers['Authorization'] == auth_headers['Authorization']
