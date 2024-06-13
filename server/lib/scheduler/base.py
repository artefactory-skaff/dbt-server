import abc


class BaseScheduler(abc.ABC):
    @abc.abstractmethod
    def create_or_update_job(self, job_name: str, schedule: str, server_url: str, description: str = ""):
        raise NotImplementedError
