from ..library import (AutoIncrementEnum, SingletonMeta)
from ..task.task import *


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

