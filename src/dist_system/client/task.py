from dist_system.logger import Logger
from dist_system.library import (AutoIncrementEnum, SingletonMeta)
from dist_system.task import *


class TaskSyncManager(metaclass=SingletonMeta):
    def __index__(self):
        self._pending_queue = list()
        self._cancel_queue = list()

    def pend_experiment(self, exp_id):
        self._pending_queue.append(exp_id)

    def unpend_experiment(self, exp_id):
        if exp_id in self._pending_queue:
            self._pending_queue.remove(exp_id)
        else:
            Logger().log(" * TaskSyncManager::Unpending error - There is no experiment_id")

    def reserve_cancel(self, exp_id):
        self._cancel_queue.append(exp_id)

    def remove_from_cancel_queue(self, exp_id):
        if exp_id in self._cancel_queue:
            self._cancel_queue.remove(exp_id)
        else:
            Logger().log(" * TaskSyncManager::Canceling error - There is no experiment_id")

    def check_cancel_exp(self, exp_id):
        return exp_id in self._cancel_queue

    def check_pending_exp_id(self, exp_id):
        return exp_id in self._pending_queue


class TaskManager(CommonTaskManager, metaclass=SingletonMeta):

    def __init__(self):
        super().__init__()

    def check_task_existence_by_exp_id(self, exp_id):
        for task in self._all_tasks:
            if task.exp_id == exp_id:
                return True
        else:
            return False

    def find_task_by_exp_id(self, exp_id):
        if self.check_task_existence_by_exp_id(exp_id):
            targets = [task for task in self._all_tasks if task.exp_id == exp_id]

            if len(targets) == 1:
                return targets[0]
            elif len(targets) > 1:
                raise TaskValueError("Same Tasks exist.")
            else:
                raise TaskValueError("Exist, but not found.")
        else:
            raise TaskValueError("Non-existent Task.")

