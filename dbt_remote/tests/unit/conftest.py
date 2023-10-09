import pytest
import unittest
from unittest.mock import patch
from pathlib import Path


@pytest.fixture
def MockSendCommandRequest(requests_mock):
    server_url = "https://test-server.test/"
    mock = requests_mock.post(server_url+'dbt', json={'name': 'awesome-mock'})
    return mock


@pytest.fixture
def PatchBuiltInOpen():
    return patch('builtins.open', unittest.mock.mock_open(read_data='data...'))


@pytest.fixture
def MockDbtFileSystem(fs):
    fs.create_file('seeds_path/my_seed.csv')
    fs.create_file('.dbt')
    fs.create_file('/home/runner/.dbt')
    fs.create_dir('/home/runner/work/dbt-server/dbt-server')

    dbt_home_dir = str(Path.home()) + '/.dbt'
    fs.create_dir(dbt_home_dir)
