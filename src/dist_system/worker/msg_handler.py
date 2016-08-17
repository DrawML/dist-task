
from ..task.sleep_task import *
from ..protocol import ResultReceiverAddress
from ..library import SingletonMeta
from .main import *
from .task import *
from .controller import *


class SlaveMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, header, body):
        msg_name = header
        SlaveMessageHandler.__handler_dict[msg_name](self, body)

    def _h_worker_register_res(self, body):
        import sys
        sys.exit(0)

    def _h_task_cancel_req(self, body):
        import sys
        sys.exit(0)

    def _h_task_finish_res(self, body):
        import sys
        sys.exit(0)

    __handler_dict = {
        "worker_register_res": _h_worker_register_res,
        "task_cancel_req": _h_task_cancel_req,
        "task_finish_res": _h_task_finish_res
    }