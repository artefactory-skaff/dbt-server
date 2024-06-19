from pathlib import Path
import time
import json
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import humanize


@dataclass
class LockData:
    holder: str = None
    created_at: float = None
    updated_at: float = None
    run_id: str = None

    def __repr__(self):
        created_at_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.created_at)) if self.created_at else "None"
        updated_at_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.updated_at)) if self.updated_at else "None"
        time_ago = humanize.naturaltime(time.time() - self.updated_at) if self.updated_at else "Never"
        return (
            f"Lock info:\n"
            f"    Holder: {self.holder}\n"
            f"    Run ID: {self.run_id}\n"
            f"    Created at: {created_at_str}\n"
            f"    Updated at: {updated_at_str}\n"
            f"    Last updated {time_ago}\n"
        )

class LockException(Exception):
    def __init__(self, lock_data: LockData):
        self.lock_data = lock_data
        super().__init__(f"LockException: {lock_data}")


class Lock:
    def __init__(self, lock_path: Path, update_cooldown=10):
        self.lock_path = lock_path
        self.lock_data = LockData()
        self.update_cooldown = update_cooldown
        self.last_update_time = None

    def acquire(self, holder, run_id) -> "Lock":
        if self.lock_path.exists():
            raise LockException(Lock.from_file(self.lock_path).lock_data)
        self.lock_data.holder = holder
        self.lock_data.created_at = time.time()
        self.lock_data.updated_at = self.lock_data.created_at
        self.lock_data.run_id = run_id
        with self.lock_path.open("w") as f:
            json.dump(asdict(self.lock_data), f)
        self.last_update_time = self.lock_data.created_at
        return self

    def release(self):
        if self.lock_path.exists():
            self.lock_path.unlink()

    def refresh(self):
        current_time = time.time()
        if self.last_update_time is None or (current_time - self.last_update_time) >= self.update_cooldown:
            self.lock_data.updated_at = current_time
            with self.lock_path.open("w") as f:
                json.dump(asdict(self.lock_data), f)
            self.last_update_time = current_time

    @contextmanager
    def lock(self, holder, run_id):
        try:
            self.acquire(holder, run_id)
            yield
        finally:
            self.release()

    @classmethod
    def from_file(cls, path: Path, update_cooldown=10) -> "Lock":
        lock = cls(path, update_cooldown)
        if lock.lock_path.exists():
            with lock.lock_path.open("r") as f:
                data = json.load(f)
            lock.lock_data = LockData(**data)
            lock.last_update_time = lock.lock_data.updated_at
        else:
            raise FileNotFoundError(f"Lock file {lock.lock_path} not found.")
        return lock
