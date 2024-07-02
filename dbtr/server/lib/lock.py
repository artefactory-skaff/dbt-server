from pathlib import Path
import time
import json
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import humanize
from dbtr.server.lib.database import Database

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

class Lock:
    def __init__(self, db: Database, update_cooldown=10):
        self.db = db
        self.lock_data = LockData()
        self.update_cooldown = update_cooldown
        self.last_update_time = None

    def acquire(self, holder, run_id) -> "Lock":
        with self.db as db:
            result = db.fetchone("SELECT * FROM Lock WHERE holder = ?", (holder,))
            if result:
                raise LockException(LockData(**result))
            self.lock_data.holder = holder
            self.lock_data.created_at = time.time()
            self.lock_data.updated_at = self.lock_data.created_at
            self.lock_data.run_id = run_id
            db.execute(
                "INSERT INTO Lock (holder, created_at, updated_at, run_id) VALUES (?, ?, ?, ?)",
                (self.lock_data.holder, self.lock_data.created_at, self.lock_data.updated_at, self.lock_data.run_id)
            )
            self.last_update_time = self.lock_data.created_at
        return self

    def release(self):
        with self.db as db:
            db.execute("DELETE FROM Lock WHERE holder = ?", (self.lock_data.holder,))

    def refresh(self):
        current_time = time.time()
        if self.last_update_time is None or (current_time - self.last_update_time) >= self.update_cooldown:
            self.lock_data.updated_at = current_time
            with self.db as db:
                db.execute(
                    "UPDATE Lock SET updated_at = ? WHERE holder = ?",
                    (self.lock_data.updated_at, self.lock_data.holder)
                )
            self.last_update_time = current_time

    @contextmanager
    def lock(self, holder, run_id):
        try:
            self.acquire(holder, run_id)
            yield
        finally:
            self.release()

    @classmethod
    def from_db(cls, db: Database, update_cooldown=10) -> "Lock":
        lock = cls(db, update_cooldown)
        with db as db_conn:
            result = db_conn.fetchone("SELECT * FROM Lock")
            if result:
                lock.lock_data = LockData(**result)
                lock.last_update_time = lock.lock_data.updated_at
            else:
                raise LockNotFound()
        return lock



class LockException(Exception):
    def __init__(self, lock_data: LockData):
        self.lock_data = lock_data
        super().__init__(f"LockException: {lock_data}")

class LockNotFound(Exception):
    pass
