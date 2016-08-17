from .client import *
from .slave import *
from .task import *
from ..task.sleep_task import *
from .controller import Scheduler
from ..protocol import ResultReceiverAddress
from ..library import SingletonMeta


class ClientMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        session_identity = ClientSessionIdentity(addr)
        msg_name = header
        ClientMessageHandler.__handler_dict[msg_name](self, session_identity, body)

    def _h_task_register_req(self, session_identity, body):
        # extract some data from body using protocol.
        result_receiver_address = ResultReceiverAddress('tcp', 'ip', 12345)
        task_token = TaskToken(b"__THIS_IS_TASK_TOKEN__")
        task_type = TaskType.TYPE_SLEEP_TASK
        task = SleepTask(task_token, result_receiver_address, SleepTaskJob(10))

        TaskManager().add_task(task)
        session = ClientSession.make_session_from_identity(session_identity, task)
        ClientSessionManager().add_session(session)

        # send "Task Register Res" to client using protocol.

    def _h_task_register_ack(self, session_identity, body):
        session = ClientSessionManager().find_session(session_identity)
        task = session.task
        TaskManager().change_task_status(task, TaskStatus.STATUS_WAITING)
        ClientSessionManager().del_session(session)

        Scheduler().invoke()

    def _h_task_cancel_req(self, session_identity, body):
        # extract some data from body using protocol.
        task_token = b"__THIS_IS_TASK_TOKEN__"

        task = TaskManager().find_task(task_token)
        TaskManager().del_task(task)
        try:
            slave = SlaveManager().find_slave_having_task(task)
            slave.delete_task(task)
            # send "Task Cancel Req" to slave using protocol
        except ValueError:
            pass

        # send "Task Cancel Res" to client using protocol.

    __handler_dict = {
        "task_register_req": _h_task_register_req,
        "task_register_ack": _h_task_register_ack,
        "task_cancel_req": _h_task_cancel_req
    }


class SlaveMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        slave_identity = SlaveIdentity(addr)
        msg_name = header
        SlaveMessageHandler.__handler_dict[msg_name](self, slave_identity, body)

    def _h_heart_beat_res(self, slave_identity, body):
        SlaveManager().find_slave(slave_identity).heartbeat()

    def _h_slave_register_req(self, slave_identity, body):
        SlaveManager().add_slave(Slave.make_slave_from_identity(slave_identity))

        Scheduler().invoke()

        # send "Slave Register Res" to slave using protocol.

    def _h_task_register_res(self, slave_identity, body):
        slave = SlaveManager().find_slave(slave_identity)

        # extract some data from body using protocol.
        task_token = b"__THIS_IS_TASK_TOKEN__"
        status = 'success'
        error_code = None

        task = TaskManager().find_task(task_token)
        if status == 'success':
            # check if task's status == TaskStatus.STATUS_WAITING
            # or(and)
            # check task register req가 갔었는지 올바른 res인지 check가 필요.
            # 여기부분외에도 여러부분에서 이런 처리가 필요할 것이다.
            # 그러나 일단 이부분은 후순위로 두고 일단 빠른 구현을 목표로 한다.
            # 추후에 구현완료 후 보완하도록 하자.
            pass
        elif status == 'fail':
            slave.delete_task(task)
            TaskManager().redo_leak_task(task)
            Scheduler().invoke()
        else:
            pass


    def _h_task_cancel_res(self, slave_identity, body):
        # 뭘 해야하지... 흠.. 그냥 프로토콜에서 뺄까?
        pass

    def _h_task_finish_req(self, slave_identity, body):
        slave = SlaveManager().find_slave(slave_identity)

        # extract some data from body using protocol.
        task_token = b"__THIS_IS_TASK_TOKEN__"

        task = TaskManager().find_task(task_token)
        slave.delete_task(task)
        TaskManager().change_task_status(task, TaskStatus.STATUS_COMPLETE)  # yes, there is no need of this code.
        TaskManager().del_task(task)

        # send "Task Finish Res" to slave using protocol. 근데 이거 굳히 필요한가..? 흠..

    __handler_dict = {
        "heart_beat_res": _h_heart_beat_res,
        "slave_register_req": _h_slave_register_req,
        "task_register_res": _h_task_register_res,
        "task_cancel_res": _h_task_cancel_res,
        "task_finish_req": _h_task_finish_req
    }
