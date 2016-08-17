#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import random
import string
from abc import *
from ..library import AutoIncrementEnum
from ..protocol import ResultReceiverAddress
from ..library import SingletonMeta


class TaskType(AutoIncrementEnum):
    TYPE_SLEEP_TASK = ()
    #TYPE_TENSORFLOW_LEARNING = ()
    #TYPE_TENSORFLOW_TEST = ()


class TaskToken(object):
    def __init__(self, token : bytes):
        self._token = token

    def __eq__(self, other : 'TaskToken'):
        return self._token == other._token

    @staticmethod
    def generate_random_token(size : int = 512, chars : str = string.ascii_uppercase + string.digits) -> 'TaskToken':
        # this must be modified!!
        raise NotImplementedError("This must be implemented!")
        return TaskToken(''.join(random.choice(chars) for _ in range(size)))


class Task(metaclass = ABCMeta):
    def __init__(self, task_token : TaskToken, result_receiver_address : ResultReceiverAddress):
        self._task_token = task_token
        self._result_receiver_address = result_receiver_address

    def __eq__(self, other : 'Task'):
        return self._task_token == other._task_token


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
    @abstractmethod
    def job(self):
        pass

    @property
    @abstractmethod
    def result(self):
        pass

    @result.setter
    @abstractmethod
    def result(self, result):
        pass


class TaskJob(metaclass = ABCMeta):
    @abstractmethod
    def to_bytes(self):
        pass

    @staticmethod
    @abstractmethod
    def from_bytes(bytes_ : bytes) -> 'TaskJob':
        pass


class TaskResult(metaclass = ABCMeta):
    @abstractmethod
    def to_bytes(self):
        pass

    @staticmethod
    @abstractmethod
    def from_bytes(bytes_: bytes) -> 'TaskResult':
        pass


class CommonTaskManager(metaclass=SingletonMeta):

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

    def add_task(self, task, status = None):
        if status is None: status = self._initial_status
        if self.check_task_existence(task.task_token):
            raise ValueError("Duplicated Task.")
        else:
            task.status = status
            self._dic_status_queue[status].append(task)
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
        self._dic_status_queue[cur_status].remove(task)
        self._dic_status_queue[new_status].append(task)
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
                raise ValueError("Same Tasks exist.")
            return targets[0]
        else:
            raise ValueError("Non-existent Task.")