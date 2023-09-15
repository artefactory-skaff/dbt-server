from unittest.mock import call

from api.lib.state import State, current_date_time, generate_folder_name
from api.lib.dbt_classes import DbtCommand


def test_init_state(MockCloudStorage, MockState):
    mock_gcs_client, _, _, _ = MockCloudStorage
    mock_dbt_collection, mock_document = MockState
    uuid = 'test_uuid'

    state = State(uuid, mock_gcs_client, mock_dbt_collection)
    state.init_state()
    dt_time = current_date_time()

    initial_state = {
            "uuid": uuid,
            "run_status": "created",
            "user_command": "",
            "cloud_storage_folder": "",
            "log_starting_byte": 0
        }
    mock_dbt_collection.document.assert_called_once_with(uuid)
    mock_document.set.assert_called_once_with(initial_state)
    mock_gcs_client.write_to_bucket.assert_called_once_with('dbt-stc-test-eu',
                                                            f'logs/{uuid}.txt', dt_time+"\tINFO\tInit")


def test_load_context(MockCloudStorage, MockState):

    uuid = 'test_uuid'
    cloud_storage_folder = generate_folder_name(uuid)
    load_context_list = [
        {
            "command": DbtCommand(
                server_url='http://localhost:8080',
                user_command='dbt run',
                processed_command='dbt run',
                manifest='manifest',
                dbt_project='dbt_project',
            ),
            "calls": [call('dbt-stc-test-eu', f'{cloud_storage_folder}/manifest.json', 'manifest'),
                      call('dbt-stc-test-eu', f'{cloud_storage_folder}/dbt_project.yml', 'dbt_project')]
        },
        {
            "command": DbtCommand(
                server_url='http://localhost:8080',
                user_command='dbt run',
                processed_command='dbt run',
                manifest='manifest',
                dbt_project='dbt_project',
                packages='packages',
                seeds={'seed1': 'seed1', 'seed2': 'seed2'},
            ),
            "calls": [call('dbt-stc-test-eu', f'{cloud_storage_folder}/manifest.json', 'manifest'),
                      call('dbt-stc-test-eu', f'{cloud_storage_folder}/dbt_project.yml', 'dbt_project'),
                      call('dbt-stc-test-eu', f'{cloud_storage_folder}/packages.yml', 'packages'),
                      call('dbt-stc-test-eu', f'{cloud_storage_folder}/seed1', 'seed1'),
                      call('dbt-stc-test-eu', f'{cloud_storage_folder}/seed2', 'seed2')]
        },
    ]

    for i in range(len(load_context_list)):
        mock_gcs_client, _, _, _ = MockCloudStorage
        mock_dbt_collection, _ = MockState
        state = State(uuid, mock_gcs_client, mock_dbt_collection)

        dbt_command = load_context_list[i]["command"]
        state.load_context(dbt_command)

        calls = load_context_list[i]["calls"]
        mock_gcs_client.write_to_bucket.assert_has_calls(calls)


def test_get_last_logs(MockCloudStorage, MockState):
    uuid = 'test_uuid'

    mock_gcs_client, _, _, _ = MockCloudStorage
    mock_gcs_client.get_blob_from_bucket.return_value = b'hello world'
    mock_dbt_collection, _ = MockState

    state = State(uuid, mock_gcs_client, mock_dbt_collection)
    state.get_last_logs()

    mock_gcs_client.get_blob_from_bucket.assert_called_once_with('dbt-stc-test-eu', f'logs/{uuid}.txt', 0)


def test_log(MockCloudStorage, MockState):
    uuid = 'test_uuid'
    mock_gcs_client, _, _, _ = MockCloudStorage
    mock_gcs_client.get_blob_from_bucket.return_value = b'hello world'
    mock_dbt_collection, _ = MockState

    state = State(uuid, mock_gcs_client, mock_dbt_collection)
    state.log('INFO', 'test log')

    dt_time = current_date_time()
    new_log = 'hello world\n' + (dt_time + "\tINFO\ttest log")

    mock_gcs_client.write_to_bucket.assert_called_once_with('dbt-stc-test-eu', f'logs/{uuid}.txt', new_log)
