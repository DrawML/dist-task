from ..task.sleep_task import *
from ..result_receiver import ResultReceiverAddress
from ..library import SingletonMeta
from .task import *
from .controller import *
from ..task.functions import *
from . import main


class ResultMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        msg_name = header
        ResultMessageHandler.__handler_dict[msg_name](self, addr, body)

    def _h_task_result_req(self, addr, body):
        try:
            status = body['status']
            task_token = TaskToken.from_bytes(body['task_token'])

            if status == 'complete':
                task = TaskManager().find_task(task_token)
                set_result_dict_to_task(task, body['result'])
                print("[*] Task Result : {0}".format(str(task.result)))
                TaskManager().del_task(task)
                res_body = {
                    'status': 'success'
                }
            elif status == 'cancel':
                # nothing to do use request.
                res_body = {
                    'status' : 'success'
                }
            elif status == 'fail':
                error_code = body['error_code']
                task = TaskManager().del_task(task_token)
                res_body = {
                    'status': 'success'
                }
            else:
                # invalid message
                res_body = {
                    'status' : 'fail',
                    'error_code' : 'unknown'
                }
        except TaskValueError:
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }

        main.ResultReceiverCommunicationRouter().dispatch_msg(addr, 'task_result_res', res_body)

    __handler_dict = {
        'task_result_req' : _h_task_result_req
    }
