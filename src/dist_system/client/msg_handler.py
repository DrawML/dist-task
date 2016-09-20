from ..task.sleep_task import *
from ..result_receiver import ResultReceiverAddress
from ..library import SingletonMeta
from .task import *
from .controller import *
from ..task.functions import *
from . import main
from ..logger import Logger
import traceback


class ResultMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        msg_name = header
        Logger().log("result addr={0} header={1}, body={2}".format(addr, header, body))
        try:
            ResultMessageHandler.__handler_dict[msg_name](self, addr, body)
        except Exception as e:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n" + traceback.format_exc())
        Logger().log("finish of handling result message")

    def _h_task_result_req(self, addr, body):
        try:
            status = body['status']
            task_token = TaskToken.from_bytes(body['task_token'])

            callback_args = dict()
            callback_args['result'] = status

            if status == 'complete':
                task = TaskManager().find_task(task_token)

                callback = task.callback

                set_result_dict_to_task(task, body['result'])
                Logger().log("[*] Task Result : {0}".format(str(task.result)))
                TaskManager().del_task(task)

                res_body = {
                    'status': 'success'
                }
            elif status == 'cancel':
                # nothing to do use request.
                res_body = {
                    'status': 'success'
                }
            elif status == 'fail':
                error_code = body['error_code']
                task = TaskManager().del_task(task_token)

                callback = task.callback
                callback_args['error_code'] = error_code

                res_body = {
                    'status': 'success'
                }
            else:
                # invalid message
                res_body = {
                    'status': 'fail',
                    'error_code': 'unknown'
                }
        except TaskValueError:
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }

        main.ResultReceiverCommunicationRouter().dispatch_msg(addr, 'task_result_res', res_body,
                                                              callback, callback_args)

    __handler_dict = {
        'task_result_req': _h_task_result_req
    }
