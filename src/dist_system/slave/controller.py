import asyncio
import subprocess

from dist_system.library import SingletonMeta
from dist_system.protocol.slave_worker import make_msg_data
from dist_system.slave.file import FileManager, FileType
from dist_system.slave.monitor import monitor
from dist_system.slave.msg_dispatcher import MasterMessageDispatcher
from dist_system.slave.result_receiver import ResultReceiverCommunicatorWithSlave
from dist_system.slave.task import TaskManager
from dist_system.slave.worker import WorkerManager
from dist_system.task import TaskType
from dist_system.task.data_processing_task import DataProcessingTaskWorkerJob
from dist_system.task.functions import get_task_type_of_task
from dist_system.task.tensorflow_train_task import TensorflowTrainTaskWorkerJob
from dist_system.task.tensorflow_test_task import TensorflowTestTaskWorkerJob
from dist_system.address import SlaveAddress
from dist_system.cloud_dfs import CloudDFSConnector, CloudDFSAddress
import dist_system.cloud_dfs as cloud_dfs


class WorkerCreator(metaclass=SingletonMeta):
    def __init__(self, worker_file_name, slave_address: SlaveAddress, cloud_dfs_address: CloudDFSAddress):
        self._worker_file_name = worker_file_name
        self._slave_address = slave_address
        self._cloud_dfs_address = cloud_dfs_address

    def create(self, result_receiver_address, task_token, task_type, task):
        serialized_data = make_msg_data('task_register_cmd', {
            'result_receiver_address': result_receiver_address.to_dict(),
            'task_token': task_token.to_bytes(),
            'task_type': task_type.to_str(),
            'task': task.job.to_dict(),
            'slave_address': self._slave_address.to_dict(),
            'cloud_dfs_address': self._cloud_dfs_address.to_dict()
        })
        hex_data = serialized_data.hex()
        proc = subprocess.Popen(['python3', self._worker_file_name, hex_data])
        return proc


def preprocess_task(task):
    task_type = get_task_type_of_task(task)

    if task_type == TaskType.TYPE_SLEEP_TASK:
        pass

    elif task_type == TaskType.TYPE_DATA_PROCESSING_TASK:
        data_file_num = task.job.data_file_num
        data_filename_list = []
        for data_file_token in task.job.data_file_tokens:
            _, file_data = CloudDFSConnector().get_data_file(data_file_token)
            data_filename = FileManager().store(task, FileType.TYPE_DATA_FILE, file_data)
            data_filename_list.append(data_filename)

        executable_code_filename = FileManager().store(task, FileType.TYPE_EXECUTABLE_CODE_FILE,
                                                       task.job.executable_code)

        task.job = DataProcessingTaskWorkerJob(data_file_num, data_filename_list, executable_code_filename)

    elif task_type == TaskType.TYPE_TENSORFLOW_TRAIN_TASK:
        _, file_data = CloudDFSConnector().get_data_file(task.job.data_file_token)
        data_filename = FileManager().store(task, FileType.TYPE_DATA_FILE, file_data)
        executable_code_filename = FileManager().store(task, FileType.TYPE_EXECUTABLE_CODE_FILE,
                                                       task.job.executable_code)

        session_filename = FileManager().reserve(task, FileType.TYPE_SESSION_FILE)
        result_filename = FileManager().reserve(task, FileType.TYPE_RESULT_FILE)

        task.job = TensorflowTrainTaskWorkerJob(data_filename, executable_code_filename,
                                                session_filename, result_filename)

    elif task_type == TaskType.TYPE_TENSORFLOW_TEST_TASK:
        _, file_data = CloudDFSConnector().get_data_file(task.job.data_file_token)
        data_filename = FileManager().store(task, FileType.TYPE_DATA_FILE, file_data)
        executable_code_filename = FileManager().store(task, FileType.TYPE_EXECUTABLE_CODE_FILE,
                                                       task.job.executable_code)

        _, file_data = CloudDFSConnector().get_data_file(task.job.session_file_token)
        session_filename = FileManager().store(task, FileType.TYPE_SESSION_FILE, task.job.file_data)
        result_filename = FileManager().reserve(task, FileType.TYPE_RESULT_FILE)

        task.job = TensorflowTestTaskWorkerJob(data_filename, executable_code_filename,
                                               session_filename, result_filename)

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
            FileManager().remove_files_using_key(leak_task)

            header, body = ResultReceiverCommunicatorWithSlave().communicate('task_finish_req', {
                'status': 'fail',
                'task_token': leak_task.task_token.to_bytes()
            })
            # nothing to do using response message...


async def monitor_information():
    MONITORING_INTERVAL = 3

    while True:
        slave_information = await monitor()
        await asyncio.sleep(MONITORING_INTERVAL)

        MasterMessageDispatcher().dispatch_msg('slave_information_req', slave_information.to_dict())
