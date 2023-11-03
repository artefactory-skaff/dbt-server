import subprocess
from pathlib import Path


class DbtServerImage:
    def __init__(self, location: str, artifact_registry: str):
        self.location = location
        self.artifact_registry = artifact_registry

    def submit(self):
        dbt_server_dir = Path(__file__).parents[2] / "dbt_server"

        command = f"gcloud builds submit {dbt_server_dir} --region={self.location} --tag {self.artifact_registry}/server-image"
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logs = ""
        while True:
            output = process.stdout.readline()
            if output:
                log = output.decode("utf8")
                logs += log
                print(log.strip())
            if process.poll() is not None:
                break


if __name__ == "__main__":
    dbt_server_image = DbtServerImage("europe-west1", "europe-west1-docker.pkg.dev/dbt-server-alexis2-2a22/dbt-server-repository")
    print(dbt_server_image.submit())
