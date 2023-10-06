from unittest.mock import Mock, patch

import pytest
from dbt_server.lib.logger import DbtLogger, get_log_level, init_logger
from dbt_server.lib.state import State


def test_dbt_logger_init():
    logger = DbtLogger(service="Local", uuid="test_uuid")
    assert logger.uuid == "test_uuid"
    assert isinstance(logger.state, State)


def test_dbt_logger_log():
    logger = DbtLogger(service="Local", uuid="test_uuid")
    logger.log(severity="INFO", new_log="test log")
    assert any(["INFO" in buffer for buffer in logger.state.run_logs_buffer])
    assert any(["test log" in buffer for buffer in logger.state.run_logs_buffer])


@patch("dbt_server.lib.logger.CloudLoggingHandler")
@patch("dbt_server.lib.logger.Client")
@patch("dbt_server.lib.logger.logging.getLogger")
def test_init_logger_google_cloud(mock_get_logger, *args):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    logger = init_logger(service="GoogleCloudLogging")
    assert logger == mock_logger
    mock_logger.addHandler.assert_called_once()


@patch("dbt_server.lib.logger.AzureLogHandler")
@patch("dbt_server.lib.logger.logging.getLogger")
def test_init_logger_azure_monitor(mock_get_logger, *args):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    logger = init_logger(service="AzureMonitor")
    assert logger == mock_logger
    mock_logger.addHandler.assert_called_once()


def test_get_log_level():
    assert get_log_level("INFO") == 20
    assert get_log_level("ERROR") == 40
    with pytest.raises(Exception):
        get_log_level("UNKNOWN")
