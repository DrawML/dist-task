
from ..task.sleep_task import *
from ..protocol import ResultReceiverAddress
from ..library import SingletonMeta
from .worker import *
from .task import *
from .controller import *


class MasterMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, header, body):
        msg_name = header
        MasterMessageHandler.__handler_dict[msg_name](self, body)

    def _h_heart_beat_req(self, body):
        # send "Heart Beat Res" to master using protocol.
        pass

    def _h_slave_register_res(self, body):
        # extract some data from body using protocol.
        status = 'success'
        error_code = None

        if status == 'success':
            pass
        elif status == 'fail':
            import sys
            # cannot register itself to master.
            sys.exit(1)
        else:
            pass

    def _h_task_register_req(self, body):
        # extract some data from body using protocol.
        result_receiver_address = ResultReceiverAddress('tcp', 'ip', 12345)
        task_token = TaskToken(b"__THIS_IS_TASK_TOKEN__")
        task_type = TaskType.TYPE_SLEEP_TASK
        task = SleepTask(task_token, result_receiver_address, SleepTaskJob(10))

        TaskManager().add_task(task)
        proc = WorkerCreator().create(result_receiver_address, task_token, task_type, task)
        WorkerManager().add_worker(Worker(proc, task))


        # send "Task Register Res" to master using protocol.

    def _h_task_cancel_req(self, body):
        # extract some data from body using protocol.
        task_token = b"__THIS_IS_TASK_TOKEN__"

        task = TaskManager().find_task(task_token)
        TaskManager().del_task(task)
        try:
            worker = WorkerManager().find_worker_having_task(task)
            WorkerManager().del_worker(worker)
            # send "Task Cancel Req" to worker using protocol
            # 여기서 다 지우므로 Task Cancel Res는 받을 수 없다.
        except ValueError:
            pass

    def _h_task_finish_res(self, body):
        # what I do??!
        pass


    __handler_dict = {
        "heart_beat_req": _h_heart_beat_req,
        "slave_register_res": _h_slave_register_res,
        "task_register_req": _h_task_register_req,
        "task_cancel_req": _h_task_cancel_req,
        "task_finish_res": _h_task_finish_res
    }


class WorkerMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        worker_identity = WorkerIdentity(addr)
        msg_name = header
        WorkerMessageHandler.__handler_dict[msg_name](self, worker_identity, body)

    def _h_worker_register_req(self, worker_identity, body):
        # extract some data from body using protocol.
        task_token = b"__THIS_IS_TASK_TOKEN__"

        task = TaskManager().find_task(task_token)
        worker = WorkerManager().find_worker_having_task(task)
        if worker.valid:
            pass
        else:
            worker.get_lazy_identity(worker_identity)
            # send "Worker Register Res" to worker using protocol


    def _h_task_cancel_res(self, worker_identity, body):
        # 현재 흐름상 이 message는 절대 수신될 수 없음!!
        pass

    def _h_task_finish_req(self, worker_identity, body):
        worker = WorkerManager().find_worker(worker_identity)
        WorkerManager().del_worker(worker)
        TaskManager().del_task(worker.task)

        # send "Task Finish Req" to master using protocol
        # send "Task Finish Res" to worker using protocol
        pass

    __handler_dict = {
        "worker_register_req": _h_worker_register_req,
        "task_cancel_res": _h_task_cancel_res,
        "task_finish_req": _h_task_finish_req
    }
