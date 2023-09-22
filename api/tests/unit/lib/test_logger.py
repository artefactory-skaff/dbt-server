import pytest
from unittest.mock import Mock, patch
from api.lib.logger import DbtLogger, init_logger, get_log_level
from api.lib.state import State


def test_dbt_logger_init():
    logger = DbtLogger(service="Local", uuid="test_uuid")
    assert logger.uuid == "test_uuid"
    assert isinstance(logger.state, State)


def test_dbt_logger_log():
    logger = DbtLogger(service="Local", uuid="test_uuid")
    logger.log(severity="INFO", new_log="test log")
    assert any(["INFO" in buffer for buffer in logger.state.run_logs_buffer])
    assert any(["test log" in buffer for buffer in logger.state.run_logs_buffer])


@patch("api.lib.logger.CloudLoggingHandler")
@patch("api.lib.logger.Client")
@patch("api.lib.logger.logging.getLogger")
def test_init_logger_google_cloud(mock_get_logger, *args):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    logger = init_logger(service="GoogleCloudLogging")
    assert logger == mock_logger
    mock_logger.addHandler.assert_called_once()


@patch("api.lib.logger.AzureLogHandler")
@patch("api.lib.logger.logging.getLogger")
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
