import sys

sys.path.insert(1, './api')
sys.path.insert(2, './api/lib')
from dbt_run_job import get_user_request_log_configuration


def test_get_user_request_log_configuration():

    user_commands_dict = {
        "--log-level info --log-format json": {
            "log_format": "json",
            "log_level": "info"
        },
        "--log-format json --log-level info": {
            "log_format": "json",
            "log_level": "info"
        },
        "--log-level debug --log-format text": {
            "log_format": "text",
            "log_level": "debug"
        },
        "--log-level error": {
            "log_format": "default",
            "log_level": "error"
        },
        "--log-format text": {
            "log_format": "text",
            "log_level": "info"
        },
        "--quiet --log-format json": {
            "log_format": "json",
            "log_level": "error"
        },
    }
    for command in user_commands_dict.keys():
        results = get_user_request_log_configuration(command)
        expected_results = user_commands_dict[command]
        assert results["log_format"] == expected_results["log_format"]
        assert results["log_level"] == expected_results["log_level"]
