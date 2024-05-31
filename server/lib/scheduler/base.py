import abc


class Scheduler:
    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def create(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass
