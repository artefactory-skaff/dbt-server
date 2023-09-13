from src.dbt_remote.dbt_server_detector import deduce_target_from_profiles, get_metadata_from_profiles_dict
from src.dbt_remote.dbt_server_detector import get_selected_sub_command_conf_from_user_command


def test_deduce_target_from_profiles():

    profile_dict = {'target': 'dev', 'other': 'something else'}
    assert deduce_target_from_profiles(profile_dict) == "dev"

    outputs_dict = {'default': 'default config', 'prod': 'prod config'}
    profile_dict = {'target': 'dev', 'outputs': outputs_dict}
    assert deduce_target_from_profiles(profile_dict) == "dev"

    outputs_dict = {'default': 'default config', 'prod': 'prod config'}
    profile_dict = {'outputs': outputs_dict}
    assert deduce_target_from_profiles(profile_dict) == "default"

    outputs_dict = {'dev': 'dev config'}
    profile_dict = {'outputs': outputs_dict}
    assert deduce_target_from_profiles(profile_dict) == "dev"


def test_get_metadata_from_profiles_dict():

    profiles_dict = {
        "profile1": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "key": "value",
                    "project": "projectid",
                    "location": "location"
                }
            }
        },
        "profile2": {
            "target": "prod",
            "outputs": {
                "dev": {
                    "key": "value",
                    "project": "projectid3",
                    "location": "location3"
                },
                "prod": {
                    "key": "value",
                    "project": "projectid2",
                    "location": "location2"
                }
            }
        }
    }
    profile, target = "profile1", "dev"
    project_id = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'project')
    location = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'location')
    assert project_id == "projectid"
    assert location == "location"

    profile, target = "profile2", "prod"
    project_id = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'project')
    location = get_metadata_from_profiles_dict(profiles_dict, profile, target, 'location')
    assert project_id == "projectid2"
    assert location == "location2"


def test_get_selected_target_and_profile():
    project_dir = " --project-dir /home/runner/work/dbt-server/dbt-server"
    commands_dict = {
        "run": {
            "target": None,
            "profile": None
        },
        "run --target mytarget": {
            "target": "mytarget",
            "profile": None
        },
        "run -t mytarget": {
            "target": "mytarget",
            "profile": None
        },
        "run --profile myprofile": {
            "target": None,
            "profile": "myprofile"
        },
        "run -t mytarget --profile myprofile": {
            "target": "mytarget",
            "profile": "myprofile"
        },
    }

    for command in commands_dict.keys():
        expected_target, expected_profile = commands_dict[command]["target"], commands_dict[command]["profile"]
        computed_target = get_selected_sub_command_conf_from_user_command(command+project_dir, 'target')
        computed_profile = get_selected_sub_command_conf_from_user_command(command+project_dir, 'profile')
        assert computed_target == expected_target
        assert computed_profile == expected_profile
