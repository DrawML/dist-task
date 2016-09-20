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

    def request(self, task_type: TaskType, task_job_dict: dict, callback=None):
        if self._is_running:
            print('[Client] ', 'request message before')

            msg = (task_type, task_job_dict, callback)
            self._msg_queue.put(msg)

            print('[Client] ', 'request message after')
        else:
            print('[Client] ', 'is not running')
            pass

    @property
    def is_running(self):
        return self._is_running

