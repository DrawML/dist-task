import sys
from os.path import dirname

src_root_path = dirname(dirname(dirname(__file__)))

if not src_root_path in sys.path:
    sys.path.append(src_root_path)

from queue import Queue
from threading import Thread

from dist_system.library import SingletonMeta
from dist_system.task import TaskType
from dist_system.client.message import RequestMessage, CancelMessage


class Client(metaclass=SingletonMeta):
    TaskType = TaskType

    def __init__(self, master_addr, result_router_addr, result_receiver_address):
        self._master_addr = master_addr
        self._result_router_addr = result_router_addr
        self._result_receiver_address = result_receiver_address

        self._msg_queue = Queue()

        from dist_system.client.main import main
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
