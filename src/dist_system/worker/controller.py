import zmq
from zmq.asyncio import Context
import asyncio
from ..result_receiver import ResultReceiverAddress
from .result_receiver import *
from ..task.task import *
from ..task.sleep_task import *
from .msg_dispatcher import *


class TaskInformation(object):

    def __init__(self, result_receiver_address : ResultReceiverAddress,
                 task_token : TaskToken, task_type : TaskType, task : Task):
        self._result_receiver_address = result_receiver_address
        self._task_token = task_token
        self._task_type = task_type
        self._task = task

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

    @staticmethod
    def from_dict(dict_ : dict) -> 'TaskInformation':
        result_receiver_address = ResultReceiverAddress.from_dict(dict_['result_receiver_address'])
        task_token = TaskToken.from_bytes(dict_['task_token'])
        task_type = TaskType.from_str(dict_['task_type'])
        if task_type == TaskType.TYPE_SLEEP_TASK:
            task = SleepTask(task_token, result_receiver_address, SleepTaskJob.from_dict(dict_['task']))
        else:
            raise NotImplementedError("Not implemented Task Type.")

        return TaskInformation(result_receiver_address, task_token, task_type, task)


async def _do_sleep_task(sleep_task : SleepTask):
    await asyncio.sleep(sleep_task.job.seconds)


async def _report_task_result(context : Context, task_info : TaskInformation):

    sock = context.socket(zmq.DEALER)
    sock.connect(task_info.result_receiver_address.to_zeromq_addr())

    header, body = ResultReceiverCommunicatorWithWorker().communicate(task_info.result_receiver_address, 'task_result_req', {
        'status' : 'complete',
        'task_token' : task_info.task_token.to_bytes(),
        'result' : task_info.task.result.to_dict()
    })
    # nothing to do using response message...

    SlaveMessageDispatcher().dispatch_msg('task_finish_req', '')


    # send task_result_req to result receiver. (wait)
    # receive task_result_res (wait)
    # send task_finish_req to slave. (what?! there is no sync problem with 'recv task_cancel_req'?!)


async def do_task(context : Context, task_info : TaskInformation):

    if task_info.task_type == TaskType.TYPE_SLEEP_TASK:
        await _do_sleep_task(task_info.task)
    else:
        raise ValueError("Invalid Task Type.")

    await _report_task_result(context, task_info)

    """
    tensorflow 관련 task 실행시킬 땐,
    asyncio.create_subprocess_exec()
    subprocess.wait()
    활용하자.
    """