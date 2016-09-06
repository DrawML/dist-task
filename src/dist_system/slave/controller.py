import asyncio
import subprocess
from ..result_receiver import *
from .result_receiver import *
from .task import TaskManager
from ..library import SingletonMeta
from ..task.functions import get_task_type_of_task
from ..task.tensorflow_task import *
from ..task.data_processing_task import *
from .worker import WorkerManager
from .msg_dispatcher import *
from ..protocol.slave_worker import *
import binascii
from .monitor.monitor import monitor


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
        hex_data = serialized_data.hex()
        proc = subprocess.Popen([self._worker_file_name, hex_data])
        return proc


def preprocess_task(task):
    task_type = get_task_type_of_task(task)
    if task_type == TaskType.TYPE_SLEEP_TASK:
        pass
    elif task_type == TaskType.TYPE_DATA_PROCESSING_TASK:
        data_filename = "data_filename"
        executable_code_filename = "executable_code_filename"
        task.job = DataProcessingTaskWorkerJob(data_filename, executable_code_filename)
    elif task_type == TaskType.TYPE_TENSORFLOW_TASK:
        data_filename = "data_filename"
        executable_code_filename = "executable_code_filename"
        task.job = TensorflowTaskWorkerJob(data_filename, executable_code_filename)
    else:
        raise NotImplementedError


async def run_polling_workers():
    POLLING_WORKERS_INTERVAL = 3

    while True:
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


async def monitor_information():
    MONITORING_INTERVAL = 3

    while True:
        slave_information = await monitor()
        await asyncio.sleep(MONITORING_INTERVAL)

        MasterMessageDispatcher().dispatch_msg('slave_information', slave_information.to_dict())
