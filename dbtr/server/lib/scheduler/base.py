import abc


class BaseScheduler(abc.ABC):
    @abc.abstractmethod
    def create_or_update_job(self, name: str, cron_expression: str, trigger_url: str, description: str = ""):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, name: str):
        raise NotImplementedError

    @abc.abstractmethod
    def list(self):
        raise NotImplementedError


def get_scheduler(cloud_provider: str) -> BaseScheduler:
    if cloud_provider == "local":
        from dbtr.server.lib.scheduler.local import LocalScheduler
        return LocalScheduler()
    elif cloud_provider == "google":
        from dbtr.server.lib.scheduler.gcp import GCPScheduler
        return GCPScheduler()
    elif cloud_provider == "azure":
        from dbtr.server.lib.scheduler.azure import AzureScheduler
        return AzureScheduler()
    else:
        raise ValueError(f"Invalid cloud provider: {cloud_provider}")
