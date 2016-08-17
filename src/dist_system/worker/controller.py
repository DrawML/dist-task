import zmq
from zmq.asyncio import Context
import asyncio
from ..protocol import ResultReceiverAddress
from ..task.task import *
from ..task.sleep_task import *


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
    def from_bytes(bytes_ : bytes) -> 'TaskInformation':
        raise NotImplementedError("will be implemented.")
        return TaskInformation("""will be filled...""")


async def _do_sleep_task(sleep_task : SleepTask):
    await asyncio.sleep(sleep_task.job.seconds)


async def _report_task_result(context : Context, task_info : TaskInformation):

    sock = context.socket(zmq.DEALER)
    sock.connect(task_info.result_receiver_address.to_zeromq_addr())

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