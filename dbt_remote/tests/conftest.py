import pytest
import unittest
from unittest.mock import patch


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
    fs.create_file('project_dir/seeds_path/my_seed.csv')
    fs.create_file('.dbt')
    # fs.create_file('/home/runner/.dbt')
    fs.create_file('/Users/emma.galliere/.dbt')
