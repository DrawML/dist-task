from ..library import (AutoIncrementEnum, SingletonMeta)
from ..task.task import *


class TaskStatusValueError(ValueError):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return "TaskStatusValueError : %s" % self._msg

class TaskStatus(AutoIncrementEnum):
    STATUS_PENDING_ACK = ()
    STATUS_WAITING = ()
    STATUS_PROCESSING = ()
    STATUS_COMPLETE = ()


class TaskManager(CommonTaskManager, metaclass=SingletonMeta):

    def __init__(self):
        self._pending_ack_tasks = []
        self._waiting_tasks = []
        self._processing_tasks = []
        self._complete_tasks = []
        self._dic_status_queue = {
            TaskStatus.STATUS_PENDING_ACK : self._pending_ack_tasks,
            TaskStatus.STATUS_WAITING : self._waiting_tasks,
            TaskStatus.STATUS_PROCESSING : self._processing_tasks,
            TaskStatus.STATUS_COMPLETE : self._complete_tasks
        }
        super().__init__(self._dic_status_queue, TaskStatus.STATUS_PENDING_ACK)

    def redo_leak_task(self, task_token_or_task_or_list):
        if type(task_token_or_task_or_list) == list or type(task_token_or_task_or_list) == tuple:
            l = task_token_or_task_or_list
        else:
            l = [task_token_or_task_or_list]
        for task_token_or_task in l:
            task = self._from_generic_to_task(task_token_or_task)
            #self.cancel_task(task)
            #self.add_task(task)
            self.change_task_status(task, TaskStatus.STATUS_WAITING)

    @property
    def waiting_tasks(self):
        return tuple(self._waiting_tasks)

    @property
    def complete_tasks(self):
        return tuple(self._complete_tasks)