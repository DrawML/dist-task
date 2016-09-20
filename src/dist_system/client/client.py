from ..task.task import TaskType
from ..library import SingletonMeta
from threading import Thread
from queue import Queue

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
            pass
        else:
            self._client_thd.start()

    def stop(self, timeout):
        if self._is_running:
            self._client_thd.join(timeout=timeout)
        else:
            pass

    def request(self, task_id, task_type: TaskType, task_job_dict: dict, callback=None):
        if self._is_running:
            msg = (task_id, task_type, task_job_dict, callback)
            self._msg_queue.put(msg)
        else:
            pass

    @property
    def is_running(self):
        return self._is_running

