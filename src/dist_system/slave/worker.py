import asyncio
import string
import random
from ..library import SingletonMeta
import subprocess


class WorkerIdentity(object):
    def __init__(self, addr = None):
        self._addr = addr
        if addr is None:
            self._valid = False
        else:
            self._valid = True

    # I fire you if you override this.
    def __eq__(self, other):
        if self._valid and other._valid:
            return self._addr == other._addr
        else:
            return False

    def get_lazy_identity(self, worker_identity):
        self._addr = worker_identity.addr
        self._valid = worker_identity.valid

    @property
    def addr(self):
        return self._addr

    @addr.setter
    def addr(self, addr):
        self._addr = addr

    @property
    def valid(self):
        return self._valid


class Worker(WorkerIdentity):
    def __init__(self, proc : subprocess.Pipen, task, addr = None):
        super().__init__(addr)
        self._proc = proc
        self._task = task

    @property
    def task(self):
        return self._task

    @property
    def proc(self):
        return self._proc


class WorkerManager(metaclass=SingletonMeta):

    def __init__(self):
        self._workers = []
        self._dic_task_worker = {}

    @property
    def count(self):
        return len(self._workers)

    def add_worker(self, worker):
        if self.check_worker_existence(worker):
            raise ValueError("Duplicated Worker.")
        else:
            self._workers.append(worker)

    def del_worker(self, worker_identity):
        worker = self._from_generic_to_worker(worker_identity)
        self._workers.remove(worker)
        del self._dic_task_worker[worker.task]

    def _from_generic_to_worker(self, identity_or_worker):
        if type(identity_or_worker) == WorkerIdentity:
            worker = self.find_worker(identity_or_worker)
        else:
            worker = identity_or_worker
        return worker

    def check_worker_existence(self, worker_identity, find_flag = False):
        targets = [worker for worker in self._workers if worker == worker_identity]
        ret = len(targets) > 0
        if find_flag:
            return (ret, targets)
        else:
            return ret

    def find_worker(self, worker_identity):
        exists, targets = self.check_worker_existence(worker_identity, find_flag=True)
        if exists:
            if len(targets) > 1:
                raise ValueError("Same Workers exist.")
            return targets[0]
        else:
            raise ValueError("Non-existent Worker.")

    def find_worker_having_task(self, task):
        try:
            self._dic_task_worker[task]
        except KeyError:
            raise ValueError("Non-existent Worker.")

    def purge(self):
        expired_workers = []
        leak_tasks = []
        for worker in self._workers:
            if worker.proc.poll() is not None:
                expired_workers.append(worker)
                self.del_worker(worker)
                leak_tasks.append(worker.task)
        return expired_workers, leak_tasks