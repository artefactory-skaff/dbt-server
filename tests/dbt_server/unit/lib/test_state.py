import pytest
from unittest.mock import Mock, patch
from dbt_server.lib.state import State, DbtRunLogs
from dbt_server.lib.metadata_document import MetadataDocument
from dbt_server.lib.dbt_classes import DbtCommand


def test_state_init():
    metadata_document = Mock()
    state = State("test_uuid", metadata_document)
    assert state.uuid == "test_uuid"
    assert isinstance(state.run_logs, DbtRunLogs)
    assert state.metadata_document == metadata_document
    metadata_document.create.assert_called_once()


def test_state_properties():
    metadata_document = Mock()
    metadata_document.get().to_dict.return_value = {
        "run_status": "created",
        "user_command": "test_command",
        "log_starting_byte": 0,
        "cloud_storage_folder": "test_folder",
    }
    state = State("test_uuid", metadata_document)
    assert state.run_status == "created"
    assert state.user_command == "test_command"
    assert state.log_starting_byte == 0
    assert state.cloud_storage_folder == "test_folder"


def test_dbt_run_logs_init():
    run_logs = DbtRunLogs("test_uuid")
    assert run_logs._uuid == "test_uuid"
    assert run_logs.log_file == "logs/test_uuid.txt"


def test_dbt_run_logs_init_log_file():
    run_logs = DbtRunLogs("test_uuid")
    with patch("dbt_server.lib.state.CLOUD_STORAGE_INSTANCE.write_file") as mock_write_file:
        run_logs.init_log_file()
        mock_write_file.assert_called_once()


def test_dbt_run_logs_get():
    run_logs = DbtRunLogs("test_uuid")
    with patch("dbt_server.lib.state.CLOUD_STORAGE_INSTANCE.get_file") as mock_get_file:
        mock_get_file.return_value = b"test_log\n"
        logs, byte_length = run_logs.get(0)
        mock_get_file.assert_called_once()
        assert logs == ["test_log"]
        assert byte_length == 9
