import base64
from pathlib import Path
from subprocess import check_output
from time import sleep
from dbt_server.dbt_server import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_check_endpoint():
    response = client.get("/check")
    assert response.status_code == 200
    assert "response" in response.json()
    assert "dbt-server" in response.json()["response"]

def test_dbt_endpoint():
    profiles_path = Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dbt"
    dbt_project_path = Path(__file__).parent / 'dbt_project'
    check_output(f"cd {dbt_project_path} && dbt compile", shell=True)

    manifest = read_file_as_b64(dbt_project_path / "target" / "manifest.json")
    dbt_project_str = read_file_as_b64(dbt_project_path / "dbt_project.yml")
    profiles_str = read_file_as_b64(profiles_path / "profiles.yml")
    seeds_str = read_file_as_b64(dbt_project_path / "seeds" / "test_seed.csv")

    dbt_command = {
        "server_url": "http://does_not_matter:8000/",
        "user_command": "run",
        "manifest": manifest,
        "dbt_project": dbt_project_str,
        "profiles": profiles_str,
        "seeds": {"seed": seeds_str},
    }
    response = client.post("/dbt", json=dbt_command)

    assert response.status_code == 202
    assert "uuid" in response.json()
    assert "links" in response.json()

    uuid = response.json()['uuid']

    status = client.get(f"/job/{uuid}").json()['run_status']
    while status in ["pending", "running"]:
        response = client.get(f"/job/{uuid}/last_logs")
        assert response.status_code == 200
        assert "run_logs" in response.json()
        assert isinstance(response.json()['run_logs'], list)
        status = client.get(f"/job/{uuid}").json()['run_status']
        sleep(3)


    response = client.get(f"/job/{uuid}/logs")
    assert response.status_code == 200
    assert "run_logs" in response.json()
    assert isinstance(response.json()['run_logs'], list)
    assert "Command successfully executed" in " ".join(response.json()['run_logs'])

def read_file_as_b64(filename) -> str:
    with open(filename, 'r') as f:
        file_str = f.read()
    file_bytes = base64.b64encode(bytes(file_str, 'ascii'))
    file_str = file_bytes.decode('ascii')
    return file_str
