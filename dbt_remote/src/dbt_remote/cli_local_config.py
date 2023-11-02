from pathlib import Path
import click
import yaml


class LocalCliConfig:
    CONFIG_FILE = "dbt_remote.yml"
    DEFAULT_CONFIG = {
        'manifest': None,
        'project_dir': '.',
        'dbt_project': 'dbt_project.yml',
        'profiles_dir': '.',
        'extra_packages': None,
        'seeds_path': 'seeds/',
        'server_url': None,
        'location': None,
    }

    def __init__(self):
        if not Path(self.CONFIG_FILE).exists():
            click.echo("No config file found. Creating config...")
            self.init()

        self._config = self.load()

    @property
    def config(self):
        return self._config

    def init(self):
        with open(self.CONFIG_FILE, 'w') as f:
            yaml.dump(self.DEFAULT_CONFIG, f)

    def load(self):
        with open(self.CONFIG_FILE, 'r') as f:
            return yaml.safe_load(f)

    def set(self, key: str, value) -> None:
        config = self.load()
        config[key] = value
        with open(self.CONFIG_FILE, 'w') as f:
            yaml.dump(config, f)

    def get(self, key: str) -> str:
        config = self.load()
        return config[key]

    def delete(self, key: str) -> None:
        config = self.load()
        del config[key]
        with open(self.CONFIG_FILE, 'w') as f:
            yaml.dump(config, f)

    def __repr__(self):
        return f"LocalCliConfig({self.__dict__})"
