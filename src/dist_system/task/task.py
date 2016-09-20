#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import random
import string
from abc import *
from ..library import AutoIncrementEnum
from ..result_receiver import ResultReceiverAddress
from ..library import SingletonMeta


class TaskTypeValueError(ValueError):
    def __init__(self, msg = ''):
        self._msg = msg

    def __str__(self):
        return "TaskTypeValueError : %s" % self._msg


class TaskValueError(ValueError):
    def __init__(self, msg = ''):
        self._msg = msg

    def __str__(self):
        return "TaskValueError : %s" % self._msg


class TaskType(AutoIncrementEnum):
    TYPE_SLEEP_TASK = ()
    TYPE_DATA_PROCESSING_TASK = ()
    TYPE_TENSORFLOW_TASK = ()

    @staticmethod
    def from_str(task_type_str : str):
        if task_type_str == 'sleep_task':
            return TaskType.TYPE_SLEEP_TASK
        elif task_type_str == 'data_processing_task':
            return TaskType.TYPE_DATA_PROCESSING_TASK
        elif task_type_str == 'tensorflow_task':
            return TaskType.TYPE_TENSORFLOW_TASK
        else:
            return TaskTypeValueError(task_type_str + ' is invalid task type.')

    def to_str(self):
        if self == TaskType.TYPE_SLEEP_TASK:
            return 'sleep_task'
        elif self == TaskType.TYPE_DATA_PROCESSING_TASK:
            return 'data_processing_task'
        elif self == TaskType.TYPE_TENSORFLOW_TASK:
            return 'tensorflow_task'
        else:
            return TaskTypeValueError(self + ' is invalid task type.')


class TaskJob(metaclass = ABCMeta):
    @abstractmethod
    def _to_dict(self):
        pass

    @classmethod
    @abstractmethod
    def _from_dict(cls, dict_ : dict):
        pass

    def to_dict(self):
        try:
            return self._to_dict()
        except Exception as e:
            raise TaskValueError(str(e))

    @classmethod
    def from_dict(cls, dict_: dict):
        try:
            return cls._from_dict(dict_)
        except Exception as e:
            raise TaskValueError(str(e))


class TaskResult(metaclass = ABCMeta):
    @abstractmethod
    def _to_dict(self):
        pass

    @classmethod
    @abstractmethod
    def _from_dict(cls, dict_ : dict):
        pass

    def to_dict(self):
        try:
            return self._to_dict()
        except Exception as e:
            raise TaskValueError(str(e))

    @classmethod
    def from_dict(cls, dict_: dict):
        try:
            return cls._from_dict(dict_)
        except Exception as e:
            raise TaskValueError(str(e))


class TaskToken(object):
    def __init__(self, token : bytes):
        self._token = token

    def __eq__(self, other : 'TaskToken'):
        return self._token == other._token

    @staticmethod
    def generate_random_token(bytes_size : int = 512//8) -> 'TaskToken':
        return TaskToken(bytes(random.getrandbits(8) for _ in range(bytes_size)))

    def to_bytes(self):
        return self._token

    @staticmethod
    def from_bytes(bytes_ : bytes) -> 'TaskToken':
        return TaskToken(bytes_)


class Task(metaclass = ABCMeta):
    def __init__(self, task_token : TaskToken, result_receiver_address : ResultReceiverAddress, job : TaskJob):
        self._task_token = task_token
        self._result_receiver_address = result_receiver_address
        self._job = job

    def __eq__(self, other : 'Task'):
        return self._task_token == other._task_token

    def __hash__(self):
        return id(self)

    @property
    def result_receiver_address(self):
        return self._result_receiver_address

    @property
    def task_token(self):
        return self._task_token

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    @property
    def job(self) -> TaskJob:
        return self._job

    @job.setter
    def job(self, job):
        self._job = job

    @property
    def result(self) -> TaskResult:
        return self._result

    @result.setter
    def result(self, result : TaskResult):
        self._result = result


class CommonTaskManager(object):

    """ Example of dic_status_queue
    dic_status_queue = {
            TaskStatus.STATUS_PENDING_ACK : self._pending_ack_tasks,
            TaskStatus.STATUS_WAITING : self._waiting_tasks,
            TaskStatus.STATUS_PROCESSING : self._processing_tasks,
            TaskStatus.STATUS_COMPLETE : self._complete_tasks
        }
    """

    def __init__(self, dic_status_queue = {}, initial_status = None):
        self._all_tasks = []
        self._dic_status_queue = dic_status_queue
        self._initial_status = initial_status
        if not None in self._dic_status_queue:
            self._none_status_tasks = []
            self._dic_status_queue[None] = self._none_status_tasks

    @property
    def all_tasks(self):
        return tuple(self._all_tasks)

    def add_task(self, task, status = None):
        if status is None: status = self._initial_status
        if self.check_task_existence(task.task_token):
            raise TaskValueError("Duplicated Task.")
        else:
            self._dic_status_queue[status].append(task)
            task.status = status
            self._all_tasks.append(task)

    def del_task(self, task_token_or_task):
        task = self._from_generic_to_task(task_token_or_task)
        self._dic_status_queue[task.status].remove(task)
        self._all_tasks.remove(task)

    def _from_generic_to_task(self, task_token_or_task):
        if isinstance(task_token_or_task, TaskToken):
            task = self.find_task(task_token_or_task)
        else:
            task = task_token_or_task
        return task

    def change_task_status(self, task_token_or_task, new_status):
        task = self._from_generic_to_task(task_token_or_task)
        cur_status = task.status
        if cur_status != new_status:
            # the statements' order is important because of strong guarantee for exception.
            self._dic_status_queue[new_status].append(task)
            self._dic_status_queue[cur_status].remove(task)
            task.status = new_status

    def check_task_existence(self, task_token, find_flag = False):
        targets = [task for task in self._all_tasks if task.task_token == task_token]
        ret = len(targets) > 0
        if find_flag:
            return (ret, targets)
        else:
            return ret

    def find_task(self, task_token):
        exists, targets = self.check_task_existence(task_token, find_flag=True)
        if exists:
            if len(targets) > 1:
                raise TaskValueError("Same Tasks exist.")
            return targets[0]
        else:
            raise TaskValueError("Non-existent Task.")
