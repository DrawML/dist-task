import asyncio
import subprocess
from ..protocol import ResultReceiverAddress
from .task import TaskManager
from ..library import SingletonMeta
from .worker import WorkerManager


class WorkerCreator(metaclass=SingletonMeta):
    def __init__(self, worker_file_name):
        self._worker_file_name = worker_file_name

    def create(self, result_receiver_address, task_token, task_type, task):
        serialized_data = b"asdfasdfasdf"
        str_data = serialized_data.decode(encoding='utf-8')
        proc = subprocess.Popen([self._worker_file_name, str_data])
        return proc

        #issue!!
        #decode encoding type what?


async def run_polling_workers():
    POLLING_WORKERS_INTERVAL = 3

    await asyncio.sleep(POLLING_WORKERS_INTERVAL)
    expired_workers, leak_tasks = WorkerManager().purge()
    for expired_worker in expired_workers:
        WorkerManager().del_worker(expired_worker)
    for leak_task in leak_tasks:
        TaskManager().del_task(leak_task)

        # send "Task Finish Res" (fail) to master using protocol.