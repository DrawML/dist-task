import asyncio
import random
import traceback

import zmq
from zmq.asyncio import Context

from dist_system.logger import Logger
from dist_system.result_receiver import ResultReceiverAddress
from dist_system.task import Task, TaskType, TaskToken, TaskTypeValueError
from dist_system.task.functions import make_task_with_task_type
from dist_system.task.sleep_task import SleepTask, SleepTaskResult
from dist_system.task.data_processing_task import DataProcessingTask, DataProcessingTaskResult
from dist_system.task.tensorflow_train_task import TensorflowTrainTask, TensorflowTrainTaskResult
from dist_system.task.tensorflow_test_task import TensorflowTestTask, TensorflowTestTaskResult
from dist_system.worker.msg_dispatcher import SlaveMessageDispatcher
from dist_system.worker.result_receiver import ResultReceiverCommunicatorWithWorker
from dist_system.address import SlaveAddress
from dist_system.cloud_dfs import CloudDFSConnector, CloudDFSAddress


class TaskInformation(object):
    def __init__(self, result_receiver_address: ResultReceiverAddress,
                 task_token: TaskToken, task_type: TaskType, task: Task,
                 slave_address: SlaveAddress, cloud_dfs_address: CloudDFSAddress):
        self._result_receiver_address = result_receiver_address
        self._task_token = task_token
        self._task_type = task_type
        self._task = task
        self._slave_address = slave_address
        self._cloud_dfs_address = cloud_dfs_address

    @property
    def result_receiver_address(self):
        return self._result_receiver_address

    @property
    def task_token(self):
        return self._task_token

    @property
    def task_type(self):
        return self._task_type

    @property
    def task(self):
        return self._task

    @property
    def slave_address(self):
        return self._slave_address

    @property
    def cloud_dfs_address(self):
        return self._cloud_dfs_address

    @classmethod
    def from_dict(dict_: dict) -> 'TaskInformation':
        result_receiver_address = ResultReceiverAddress.from_dict(dict_['result_receiver_address'])
        task_token = TaskToken.from_bytes(dict_['task_token'])
        task_type = TaskType.from_str(dict_['task_type'])
        task = make_task_with_task_type(task_type, dict_['task'], 'worker', task_token, result_receiver_address)
        slave_address = SlaveAddress.from_dict(dict_['slave_address'])
        cloud_dfs_address = CloudDFSAddress.from_dict(dict_['cloud_dfs_address'])

        return TaskInformation(result_receiver_address, task_token, task_type, task, slave_address, cloud_dfs_address)


async def _do_sleep_task(sleep_task):
    Logger().log("-------before sleep--------")
    await asyncio.sleep(sleep_task.job.seconds)
    Logger().log("+++++++after sleep++++++++")
    sleep_task.result = SleepTaskResult('sleep{0}..'.format(random.randint(1, 1000000)))


async def _do_data_processing_task(data_processing_task):
    job = data_processing_task.job

    Logger().log("-------before data processing task--------")
    proc = await asyncio.create_subprocess_exec('python3', job.executable_code_filename, job.data_file_num,
                                                *job.data_files, job.result_filename,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    Logger().log("-------after data processing task--------")

    # exception handling is needed about cloud_dfs??

    # file open mode what???
    try:
        with open(job.result_filename, 'rt') as f:
            result_file_data = f.read()
    except OSError as e:
        Logger().log("There is no result file :", job.result_filename)
        # handling if there is no file.
        result_file_data = ''

    result_file_token = CloudDFSConnector().put_data_file(job.result_filename, result_file_data, 'text')

    data_processing_task.result = TensorflowTestTaskResult(stdout.decode(), stderr.decode(), result_file_token)


async def _do_tensorflow_train_task(tensorflow_train_task):
    job = tensorflow_train_task.job

    Logger().log("-------before tensorflow TRAIN task--------")
    proc = await asyncio.create_subprocess_exec('python3', job.executable_code_filename, job.data_filename,
                                                job.session_filename, job.result_filename,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    Logger().log("-------after tensorflow TRAIN task--------")

    # exception handling is needed about cloud_dfs??

    # file open mode what???
    try:
        with open(job.result_filename, 'rt') as f:
            result_file_data = f.read()
    except OSError as e:
        Logger().log("There is no result file :", job.result_filename)
        # handling if there is no file.
        result_file_data = ''

    result_file_token = CloudDFSConnector().put_data_file(job.result_filename, result_file_data, 'text')

    # file open mode what???
    try:
        with open(job.session_filename, 'rb') as f:
            session_file_data = f.read()
    except OSError as e:
        Logger().log("There is no session file :", job.result_filename)
        # handling if there is no file.
        session_file_data = b''

    session_file_token = CloudDFSConnector().put_data_file(job.session_filename, session_file_data, 'binary')

    tensorflow_train_task.result = TensorflowTrainTaskResult(stdout.decode(), stderr.decode(),
                                                       session_file_token,  result_file_token)


async def _do_tensorflow_test_task(tensorflow_test_task):
    job = tensorflow_test_task.job

    Logger().log("-------before tensorflow TEST task--------")
    proc = await asyncio.create_subprocess_exec('python3', job.executable_code_filename, job.data_filename,
                                                job.session_filename, job.result_filename,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    Logger().log("-------after tensorflow TEST task--------")

    # exception handling is needed about cloud_dfs??

    # file open mode what???
    with open(job.result_filename, 'rt') as f:
        result_file_data = f.read()
    result_file_token = CloudDFSConnector().put_data_file(job.result_filename, result_file_data, 'text')

    tensorflow_test_task.result = TensorflowTestTaskResult(stdout.decode(), stderr.decode(), result_file_token)


async def _report_task_result(context: Context, task_info: TaskInformation):
    sock = context.socket(zmq.DEALER)
    sock.connect(task_info.result_receiver_address.to_zeromq_addr())
    header, body = ResultReceiverCommunicatorWithWorker().communicate(
        task_info.result_receiver_address, 'task_result_req', {
            'status': 'complete',
            'task_type': task_info.task_type.to_str(),
            'task_token': task_info.task_token.to_bytes(),
            'result': task_info.task.result.to_dict()
        })
    # nothing to do using response message...

    SlaveMessageDispatcher().dispatch_msg('task_finish_req', {})


    # send task_result_req to result receiver. (wait)
    # receive task_result_res (wait)
    # send task_finish_req to slave. (what?! there is no sync problem with 'recv task_cancel_req'?!)


async def do_task(context: Context, task_info: TaskInformation):
    try:
        if task_info.task_type == TaskType.TYPE_SLEEP_TASK:
            await _do_sleep_task(task_info.task)
        elif task_info.task_type == TaskType.TYPE_DATA_PROCESSING_TASK:
            await _do_data_processing_task(task_info.task)
        elif task_info.task_type == TaskType.TYPE_TENSORFLOW_TRAIN_TASK:
            await _do_tensorflow_train_task(task_info.task)
        elif task_info.task_type == TaskType.TYPE_TENSORFLOW_TEST_TASK:
            await _do_tensorflow_test_task(task_info.task)
        else:
            raise TaskTypeValueError("Invalid Task Type.")

        await _report_task_result(context, task_info)
    except Exception as e:
        Logger().log("Unknown Exception occurs!\n" + traceback.format_exc())
        raise
