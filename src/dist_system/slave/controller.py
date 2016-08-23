import asyncio
import subprocess
from ..result_receiver import *
from .result_receiver import *
from .task import TaskManager
from ..library import SingletonMeta
from .worker import WorkerManager
from .msg_dispatcher import *
from ..protocol.slave_worker import *


class WorkerCreator(metaclass=SingletonMeta):
    def __init__(self, worker_file_name):
        self._worker_file_name = worker_file_name

    def create(self, result_receiver_address, task_token, task_type, task):
        serialized_data = make_msg_data('task_register', {
            'result_receiver_address' : result_receiver_address.to_dict(),
            'task_token' : task_token.to_bytes(),
            'task_type' : task_type.to_str(),
            'task' : task.job.to_dict()
        })
        str_data = serialized_data.decode(encoding='utf-8')
        proc = subprocess.Popen([self._worker_file_name, str_data])
        return proc

        #issue!!
        #decode encoding type what?
    ############################### SEE THIS!@@@


async def run_polling_workers(result_re):
    POLLING_WORKERS_INTERVAL = 3

    await asyncio.sleep(POLLING_WORKERS_INTERVAL)
    expired_workers, leak_tasks = WorkerManager().purge()
    for expired_worker in expired_workers:
        WorkerManager().del_worker(expired_worker)
    for leak_task in leak_tasks:

        TaskManager().del_task(leak_task)

        header, body = ResultReceiverCommunicatorWithSlave().communicate('task_finish_req', {
            'status' : 'fail',
            'task_token' : leak_task.task_token.to_bytes()
        })
        # nothing to do using response message...