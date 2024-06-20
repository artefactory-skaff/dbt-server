import abc


class BaseScheduler(abc.ABC):
    @abc.abstractmethod
    def create_or_update_job(self, name: str, cron_expression: str, server_url: str, description: str = ""):
        raise NotImplementedError
