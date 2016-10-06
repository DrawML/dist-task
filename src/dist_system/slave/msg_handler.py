import traceback

from dist_system.library import SingletonMeta
from dist_system.logger import Logger
from dist_system.result_receiver import ResultReceiverAddress
from dist_system.slave.controller import WorkerCreator, preprocess_task
from dist_system.slave.file import FileManager
from dist_system.slave.msg_dispatcher import MasterMessageDispatcher, WorkerMessageDispatcher
from dist_system.slave.task import TaskManager, TaskStatus
from dist_system.slave.worker import Worker, WorkerIdentity, WorkerManager, WorkerValueError
from dist_system.task import TaskType, TaskValueError, TaskToken
from dist_system.task.functions import make_task_with_task_type


class MasterMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, header, body):
        msg_name = header
        Logger().log("from master, header={0}, body={1}".format(header, body), level=2)
        try:
            MasterMessageHandler.__handler_dict[msg_name](self, body)
        except:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n{0}".
                format(traceback.format_exc()))
        Logger().log("finish of handling master message", level=2)

    def _h_heart_beat_req(self, body):
        MasterMessageDispatcher().dispatch_msg('heart_beat_res', {})

    def _h_slave_register_res(self, body):
        import sys
        try:
            status = body['status']

            if status == 'success':
                Logger().log('[*] Slave Register Success.')
            elif status == 'fail':
                # cannot register itself to master.
                error_code = body['error_code']
                Logger().log("[!] Can't register itself to master. error_code =", error_code)
                sys.exit(1)
            else:
                # invalid message.
                Logger().log('{0} is invalid status'.format(status))
                sys.exit(1)
        except:
            Logger().log('\n', traceback.format_exc())
            sys.exit(1)

    def _h_task_register_req(self, body):
        Logger().log("* TASK REGISTER")
        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            result_receiver_address = ResultReceiverAddress.from_dict(body['result_receiver_address'])
            task_type = TaskType.from_str(body['task_type'])
            task = make_task_with_task_type(task_type, body['task'], 'slave',
                                            task_token, result_receiver_address)

            Logger().log("* task : {0}".format(task))

            TaskManager().add_task(task)
            try:
                TaskManager().change_task_status(task, TaskStatus.STATUS_PREPROCESSING)
                preprocess_task(task)
                TaskManager().change_task_status(task, TaskStatus.STATUS_PROCESSING)
                proc = WorkerCreator().create(result_receiver_address, task_token, task_type, task)
                WorkerManager().add_worker(Worker(proc, task))

                res_body = {
                    'task_token': task_token.to_bytes(),
                    'status': 'success'
                }
            except:
                # for consistency of task manager
                TaskManager().del_task(task)
                raise
        except:
            # invalid message
            res_body = {
                'task_token': task_token.to_bytes(),
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            # send "Task Register Res" to master using protocol.
            MasterMessageDispatcher().dispatch_msg('task_register_res', res_body)

    def _h_task_cancel_req(self, body):
        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            try:
                task = TaskManager().find_task(task_token)
                worker = WorkerManager().find_worker_having_task(task)
                try:
                    WorkerManager().del_worker(worker)
                finally:
                    try:
                        FileManager().remove_files_using_key(task)
                    finally:
                        try:
                            TaskManager().del_task(task)
                        finally:
                            # 여기서 worker를 지우므로 worker로 부터 Task Cancel Res는 받을 수 없다.
                            WorkerMessageDispatcher().dispatch_msg(worker, 'task_cancel_req', {})
            except TaskValueError:
                res_body = {
                    'task_token': task_token,
                    'status': 'fail',
                    'error_code': 'invalid_token'
                }
                raise
        except:
            res_body = {
                'task_token': task_token,
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            MasterMessageDispatcher().dispatch_msg('task_cancel_res', res_body)

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
        Logger().log("worker identity={0} header={1}, body={2}".format(worker_identity, header, body), level=2)
        try:
            WorkerMessageHandler.__handler_dict[msg_name](self, worker_identity, body)
        except:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n{0}".
                         format(traceback.format_exc()))
        Logger().log("finish of handling worker message", level=2)

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
        except TaskValueError:
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_token'
            }
            raise
        except:
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            WorkerMessageDispatcher().dispatch_msg(worker_identity, 'worker_register_res', res_body)

    def _h_task_cancel_res(self, worker_identity, body):
        # 현재 흐름상 이 message는 절대 수신될 수 없음!!
        # So, ignore this message.
        pass

    def _h_task_finish_req(self, worker_identity, body):
        Logger().log('task finish.')
        try:
            worker = WorkerManager().find_worker(worker_identity)

            try:
                WorkerManager().del_worker(worker)
            finally:
                try:
                    TaskManager().del_task(worker.task)
                finally:
                    FileManager().remove_files_using_key(worker.task)

            res_body = {
                'status': 'success',
            }
            MasterMessageDispatcher().dispatch_msg('task_finish_req', {
                'task_token': worker.task.task_token.to_bytes()
            })
        except:
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            WorkerMessageDispatcher().dispatch_msg(worker_identity, 'task_finish_res', res_body)

    __handler_dict = {
        "worker_register_req": _h_worker_register_req,
        "task_cancel_res": _h_task_cancel_res,
        "task_finish_req": _h_task_finish_req
    }
