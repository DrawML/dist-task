from queue import Queue
from threading import Thread

from dist_system.library import SingletonMeta
from dist_system.task import TaskType


class RequestMessage():
    def __init__(self, experiment_id, task_type, task_job_dict, callback):
        self._experiment_id = experiment_id
        self._task_type = task_type
        self._task_job_dict = task_job_dict
        self._callback = callback

    @property
    def experiment_id(self):
        return self._experiment_id

    @property
    def task_type(self):
        return self._task_type

    @property
    def task_job_dict(self):
        return self._task_job_dict

    @property
    def callback(self):
        return self._callback


class CancelMessage():
    def __init__(self, experiment_id):
        self._experiment_id = experiment_id

    @property
    def experiment_id(self):
        return self._experiment_id


class Client(metaclass=SingletonMeta):
    TaskType = TaskType

    def __init__(self, master_addr, result_router_addr, result_receiver_address):
        self._master_addr = master_addr
        self._result_router_addr = result_router_addr
        self._result_receiver_address = result_receiver_address

        self._msg_queue = Queue()

        from .main import main
        self._client_thd = Thread(target=main,
                                  args=(self._master_addr,
                                        self._result_router_addr,
                                        self._result_receiver_address,
                                        self._msg_queue)
                                  )
        self._is_running = False

    def start(self):
        if self._is_running:
            print('[Client] ', 'already started ')
            pass
        else:
            print('[Client] ', 'started ')
            self._is_running = True
            self._client_thd.start()

    def stop(self, timeout):
        if self._is_running:
            print('[Client] ', 'stopped ')
            self._is_running = False
            self._client_thd.join(timeout=timeout)
        else:
            print('[Client] ', 'already stopped ')
            pass

    def request_task(self, experiment_id, task_type: TaskType, task_job_dict: dict, callback=None):
        if self._is_running:
            print('[Client] ', 'request message before')

            msg = RequestMessage(experiment_id, task_type, task_job_dict, callback)
            self._msg_queue.put(msg)

            print('[Client] ', 'request message after')
        else:
            print('[Client] ', 'is not running')
            pass

    def request_cancel(self, experiment_id):
        if self._is_running:
            print('[Client] ', 'request cancel message before')

            msg = CancelMessage(experiment_id)
            self._msg_queue.put(msg)

            print('[Client] ', 'request cancel message after')
        else:
            print('[Client] ', 'is not running')
            pass

    @property
    def is_running(self):
        return self._is_running
