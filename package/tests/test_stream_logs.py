from src.dbt_remote_cli.stream_logs import parse_log


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
