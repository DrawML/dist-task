
from ..task.sleep_task import *
from ..result_receiver import ResultReceiverAddress
from ..library import SingletonMeta
from .worker import *
from .task import *
from .controller import *
from .msg_dispatcher import *


class MasterMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, header, body):
        msg_name = header
        MasterMessageHandler.__handler_dict[msg_name](self, body)

    def _h_heart_beat_req(self, body):
        MasterMessageDispatcher().dispatch_msg('heart_beat_res', '')

    def _h_slave_register_res(self, body):
        import sys
        try:
            status = body['status']
            error_code = body['error_code']

            if status == 'success':
                print('[*] Slave Register Success.')
            elif status == 'fail':
                # cannot register itself to master.
                print("[!] Can't register itself to master. error_code =", error_code)
                sys.exit(1)
            else:
                # invalid message.
                sys.exit(1)
        except Exception as e:
            # invalid message
            print('[!]', e)
            sys.exit(1)

    def _h_task_register_req(self, body):
        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            result_receiver_address = ResultReceiverAddress.from_dict(body['result_receiver_address'])
            task_type = TaskType.from_str(body['task_type'])

            if task_type == TaskType.TYPE_SLEEP_TASK:
                task = SleepTask(task_token, result_receiver_address, SleepTaskJob.from_dict(body['task']))
            else:
                raise NotImplementedError("Not implemented Task Type.")

            TaskManager().add_task(task)
            proc = WorkerCreator().create(result_receiver_address, task_token, task_type, task)
            WorkerManager().add_worker(Worker(proc, task))

            res_body = {
                'task_token': task_token,
                'status': 'success'
            }
        except Exception as e:
            # invalid message
            print('[!]', e)
            res_body = {
                'task_token': task_token,
                'status': 'fail',
                'error_code': 'unknown'
            }

        # send "Task Register Res" to master using protocol.
        MasterMessageDispatcher().dispatch_msg('task_register_res', res_body)

    def _h_task_cancel_req(self, body):
        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            task = TaskManager().find_task(task_token)
            try:
                worker = WorkerManager().find_worker_having_task(task)
                WorkerManager().del_worker(worker)

                WorkerMessageDispatcher().dispatch_msg(worker, 'task_cancel_req', '')
                # 여기서 worker를 지우므로 worker로 부터 Task Cancel Res는 받을 수 없다.
            except WorkerValueError as e:
                pass

            TaskManager().del_task(task)
        except TaskValueError as e:
            # invalid message
            print('[!]', e)
            res_body = {
                'task_token': task_token,
                'status': 'fail',
                'error_code': 'invalid_token'
            }
        except Exception as e:
            # invalid message
            print('[!]', e)
            res_body = {
                'task_token': task_token,
                'status': 'fail',
                'error_code': 'unknown'
            }

    def _h_task_finish_res(self, body):
        # no specific handling.
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
        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            task = TaskManager().find_task(task_token)
            worker = WorkerManager().find_worker_having_task(task)
            if worker.valid:
                raise WorkerValueError('The Worker is already registered.')

            worker.get_lazy_identity(worker_identity)
            res_body = {
                'status': 'success',
            }
        except TaskValueError as e:
            # invalid message
            print('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_token'
            }
        except Exception as e:
            # invalid message
            print('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }

        WorkerMessageDispatcher().dispatch_msg(worker_identity, 'worker_register_res', res_body)


    def _h_task_cancel_res(self, worker_identity, body):
        # 현재 흐름상 이 message는 절대 수신될 수 없음!!
        # So, ignore this message.
        pass

    def _h_task_finish_req(self, worker_identity, body):
        try:
            worker = WorkerManager().find_worker(worker_identity)
            WorkerManager().del_worker(worker)
            TaskManager().del_task(worker.task)

            res_body = {
                'status': 'success',
            }

            MasterMessageDispatcher().dispatch_msg('task_finish_req', {
                                                       'task_token': worker.task.task_token.to_bytes()
                                                   })
        except Exception as e:
            # invalid message
            print('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }

        WorkerMessageDispatcher().dispatch_msg(worker_identity, 'task_finish_res', res_body)

    __handler_dict = {
        "worker_register_req": _h_worker_register_req,
        "task_cancel_res": _h_task_cancel_res,
        "task_finish_req": _h_task_finish_req
    }
