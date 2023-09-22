import os
import pytest
import unittest
from unittest.mock import patch


@pytest.fixture
def MockSendCommandRequest(requests_mock):
    server_url = "https://test-server.test"
    mock = requests_mock.post(f"{server_url}/dbt", json={"name": "awesome-mock"})
    return mock


@pytest.fixture
def PatchBuiltInOpen():
    return patch("builtins.open", unittest.mock.mock_open(read_data="data..."))


def pytest_configure(config):
    os.system("mkdir ./data")
    os.system("mkdir ./data/document")
    os.system("mkdir ./data/storage")


def pytest_unconfigure(config):
    os.system("rm -rf ./data")
