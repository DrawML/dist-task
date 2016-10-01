from dist_system.client.task import TaskManager
from dist_system.task import TaskToken, TaskValueError
from dist_system.task.functions import make_task_with_task_type, set_result_dict_to_task
from dist_system.client import main
from dist_system.logger import Logger
from dist_system.library import SingletonMeta
import traceback


class ResultMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        msg_name = header
        # Logger().log("result addr={0} header={1}, body={2}".format(addr, header, body))
        try:
            ResultMessageHandler.__handler_dict[msg_name](self, addr, body)
        except Exception as e:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n" + traceback.format_exc())
        Logger().log("finish of handling result message")

    def _h_task_result_req(self, addr, body):
        try:
            Logger().log("[*] Task Result in _h_task_result_req")

            status = body['status']
            task_token = TaskToken.from_bytes(body['task_token'])

            callback = None
            callback_args = dict()

            if status == 'complete':
                task = TaskManager().find_task(task_token)

                set_result_dict_to_task(task, body['result'])
                Logger().log("[*] Task Result : {0}".format(str(task.result)))
                TaskManager().del_task(task)

                callback = task.callback
                callback_args['status'] = 'success'
                callback_args['body'] = task.result.to_dict()

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
                callback_args['status'] = 'error'
                callback_args['body'] = error_code

                res_body = {
                    'status': 'success'
                }
            else:
                # invalid message
                res_body = {
                    'status': 'fail',
                    'error_code': 'unknown'
                }
        except TaskValueError as e:
            print(e, traceback.format_exc())
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }

        main.ResultReceiverCommunicationRouter().dispatch_msg(addr, 'task_result_res', res_body,
                                                              callback, callback_args)

    __handler_dict = {
        'task_result_req': _h_task_result_req
    }
