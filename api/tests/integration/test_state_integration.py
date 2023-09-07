import sys

sys.path.insert(1, './api/lib')
from state import State


def test_init_state():

    state = State("0000")
    state.init_state()

    uuid = state.uuid
    assert uuid == "0000"

    run_status = state.run_status
    assert run_status == "created"

    user_command = state.user_command
    assert user_command == ""

    cloud_storage_folder = state.storage_folder
    assert cloud_storage_folder == ""

    log_starting_byte = state.log_starting_byte
    assert log_starting_byte == 0


def test_get_all_logs():

    state = State("0000")
    state.init_state()
    state.log('INFO', 'log1')

    logs = state.get_all_logs()
    assert len(logs) == 2
    log1 = logs[1]
    assert log1.split('\t')[1] == 'INFO'
    assert log1.split('\t')[2] == 'log1'


def test_get_last_logs():

    state = State("0000")
    state.init_state()
    state.log('INFO', 'log1')

    logs = state.get_last_logs()
    bytes_length0 = len(''.join(logs)) + len(logs)  # at the end of each log, we add '\n' so +1 byte/log
    assert len(logs) == 2
    log1 = logs[1]
    assert log1.split('\t')[1] == 'INFO'
    assert log1.split('\t')[2] == 'log1'
    assert state.log_starting_byte == bytes_length0

    logs = state.get_last_logs()
    bytes_length = len(''.join(logs)) + len(logs)
    assert len(logs) == 0
    assert bytes_length == 0
    assert state.log_starting_byte == bytes_length0

    state.log('WARN', 'log2')

    logs = state.get_last_logs()
    bytes_length1 = len(''.join(logs)) + len(logs)
    assert len(logs) == 1
    log2 = logs[0]
    assert log2.split('\t')[1] == 'WARN'
    assert log2.split('\t')[2] == 'log2'
    assert state.log_starting_byte == bytes_length0 + bytes_length1
