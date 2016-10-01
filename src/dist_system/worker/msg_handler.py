import traceback

from dist_system.library import SingletonMeta
from dist_system.logger import Logger


class SlaveMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, header, body):
        msg_name = header
        Logger().log("from slave, header={0}, body={1}".format(header, body), level=2)
        try:
            SlaveMessageHandler.__handler_dict[msg_name](self, body)
        except Exception as e:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n" + traceback.format_exc())
        Logger().log("finish of handling slave message", level=2)

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
