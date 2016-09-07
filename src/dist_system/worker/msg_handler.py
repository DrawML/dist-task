
from ..task.sleep_task import *
from ..result_receiver import ResultReceiverAddress
from ..library import SingletonMeta
from .main import *
from ..task.task import *
from .controller import *
from ..logger import Logger
import traceback


class SlaveMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, header, body):
        msg_name = header
        Logger().log("from slave, header={0}, body={1}".format(header, body))
        try:
            SlaveMessageHandler.__handler_dict[msg_name](self, body)
        except Exception as e:
            print(traceback.format_exc())
        Logger().log("finish of handling slave message")

    def _h_worker_register_res(self, body):
        status = body['status']

        if status == 'fail':
            import sys
            sys.exit(0)

    def _h_task_cancel_req(self, body):
        import sys
        sys.exit(0)

    def _h_task_finish_res(self, body):
        # nothing to do using response message...
        import sys
        sys.exit(0)

    __handler_dict = {
        "worker_register_res": _h_worker_register_res,
        "task_cancel_req": _h_task_cancel_req,
        "task_finish_res": _h_task_finish_res
    }