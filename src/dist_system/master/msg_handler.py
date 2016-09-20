from .client import *
from .slave import *
from .task import *
from ..task.sleep_task import *
from .controller import *
from ..result_receiver import ResultReceiverAddress
from ..library import SingletonMeta
from .msg_dispatcher import *
from ..task.functions import *
from ..logger import Logger
import traceback
from ..information.information import *


class ClientMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        session_identity = ClientSessionIdentity(addr)
        msg_name = header
        Logger().log("client identity={0}, header={1}, body={2}".format(session_identity, header, body))
        try:
            ClientMessageHandler.__handler_dict[msg_name](self, session_identity, body)
        except Exception as e:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n" + traceback.format_exc())
        Logger().log("finish of handling client message.")

    def _h_task_register_req(self, session_identity, body):
        try:
            result_receiver_address = ResultReceiverAddress.from_dict(body['result_receiver_address'])
            task_token = TaskToken.generate_random_token()
            task_type = TaskType.from_str(body['task_type'])
            task = make_task_with_task_type(task_type, body['task'], 'master',
                                            task_token, result_receiver_address)
            try:
                session = ClientSession.make_session_from_identity(session_identity, task)
                ClientSessionManager().add_session(session)
                try:
                    TaskManager().add_task(task)
                except TaskValueError as e:
                    ClientSessionManager().del_session(session)
                    raise
            except ClientSessionValueError as e:
                raise

            res_body = {
                'status' : 'success',
                'task_token' : task_token.to_bytes()
            }
        except TaskTypeValueError as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_task'
            }
        except TaskValueError as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_task'
            }
        except Exception as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'status' : 'fail',
                'error_code' : 'unknown'
            }

        ClientMessageDispatcher().dispatch_msg(session_identity, 'task_register_res', res_body)

    def _h_task_register_ack(self, session_identity, body):
        try:
            session = ClientSessionManager().find_session(session_identity)
            task = session.task
            TaskManager().change_task_status(task, TaskStatus.STATUS_WAITING)
            ClientSessionManager().del_session(session)
        except ClientSessionValueError as e:
            # invalid message
            Logger().log('[!]', e)

        Scheduler().invoke()

    def _h_task_cancel_req(self, session_identity, body):
        try:
            task_token = TaskToken.from_bytes(body['task_token'])

            task = TaskManager().find_task(task_token)
            TaskManager().del_task(task)
            try:
                slave = SlaveManager().find_slave_having_task(task)
                slave.delete_task(task)

                SlaveMessageDispatcher().dispatch_msg(slave, 'task_cancel_req', {
                    'task_token' : task_token.to_bytes()
                })
            except SlaveValueError as e:
                pass

            res_body = {
                'status' : 'success'
            }
        except TaskValueError as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_token'
            }
        except Exception as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }

        ClientMessageDispatcher().dispatch_msg(session_identity, 'task_cancel_res', res_body)

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
        Logger().log("slave identity={0}, header={1}, body={2}".format(slave_identity, header, body))
        try:
            SlaveMessageHandler.__handler_dict[msg_name](self, slave_identity, body)
        except Exception as e:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n" + traceback.format_exc())
        Logger().log("finish of handling slave message")

    def _h_heart_beat_res(self, slave_identity, body):
        try:
            SlaveManager().find_slave(slave_identity).heartbeat()
        except SlaveValueError as e:
            # invalid message
            Logger().log('[!]', e)

    def _h_slave_register_req(self, slave_identity, body):
        try:
            SlaveManager().add_slave(Slave.make_slave_from_identity(slave_identity))
            res_body = {
                'status' : 'success'
            }
        except Exception as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'status': 'fail',
                'error_code' : 'unknown'
            }

        SlaveMessageDispatcher().dispatch_msg(slave_identity, 'slave_register_res', res_body)
        Scheduler().invoke()

    def _h_task_register_res(self, slave_identity, body):

        try:
            slave = SlaveManager().find_slave(slave_identity)
            task_token = TaskToken.from_bytes(body['task_token'])
            status = body['status']
            task = TaskManager().find_task(task_token)

            # check if task's status == TaskStatus.STATUS_WAITING
            # or(and)
            # check task register req가 갔었는지 올바른 res인지 check가 필요.
            # 여기부분외에도 여러부분에서 이런 처리가 필요할 것이다.
            # 그러나 일단 이부분은 후순위로 두고 일단 빠른 구현을 목표로 한다.
            # 추후에 구현완료 후 보완하도록 하자.
            # ...
            # status 검사정도면 충분할 것 같다. (그 외의 경우는 보안상 이슈가 없다.)
            if task.status != TaskStatus.STATUS_PROCESSING:
                raise TaskStatusValueError('Invalid Task Status')

            if status == 'success':
                pass
            elif status == 'fail':
                error_code = body['error_code']
                slave.delete_task(task)
                TaskManager().redo_leak_task(task)
                Scheduler().invoke()
            else:
                # invalid message
                pass
        except Exception as e:
            # invalid message
            Logger().log('[!]', e)


    def _h_task_cancel_res(self, slave_identity, body):
        # no specific handling.
        pass

    def _h_task_finish_req(self, slave_identity, body):

        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            slave = SlaveManager().find_slave(slave_identity)
            task = TaskManager().find_task(task_token)

            TaskManager().change_task_status(task, TaskStatus.STATUS_COMPLETE)  # yes, there is no need of this code.
            TaskManager().del_task(task)
            slave.delete_task(task)

            res_body = {
                'task_token': task_token.to_bytes(),
                'status': 'success'
            }
        except TaskValueError as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'task_token': task_token.to_bytes(),
                'status': 'fail',
                'error_code': 'invalid_token'
            }
        except Exception as e:
            # invalid message
            Logger().log('[!]', e)
            res_body = {
                'task_token' : task_token.to_bytes(),
                'status' : 'fail',
                'error_code' : 'unknown'
            }

        SlaveMessageDispatcher().dispatch_msg(slave_identity, 'task_finish_res', res_body)

    # not considered about exception guarantee
    def _h_slave_information_req(self, slave_identity, body):
        try:
            slave = SlaveManager().find_slave(slave_identity)
            slave_info = SlaveInformation.from_dict(body)
            slave.slave_info = slave_info
            if slave.alloc_info is None:
                slave.alloc_info = AllocationInformation(
                    slave_info.cpu_info.cpu_count,
                    slave_info.cpu_info.cpu_count,
                    [AllocationTensorflowGpuInformation(True, tf_gpu_info)
                     for tf_gpu_info in slave_info.tf_gpu_info_list]
                )
            else:
                assert slave_info.cpu_info.cpu_count == slave.alloc_info.all_cpu_count
                for alloc_tf_gpu_info in slave.alloc_info.alloc_tf_gpu_info_list:
                    targets = [x for x in slave_info.tf_gpu_info_list if alloc_tf_gpu_info.tf_device == x.tf_device]
                    assert len(targets) == 1
                    alloc_tf_gpu_info.tf_gpu_info = targets[0]

            Scheduler().invoke()

        except Exception as e:
            Logger().log('!!!!!!!!!!!!!!' + traceback.format_exc())
            pass

    __handler_dict = {
        "heart_beat_res": _h_heart_beat_res,
        "slave_register_req": _h_slave_register_req,
        "task_register_res": _h_task_register_res,
        "task_cancel_res": _h_task_cancel_res,
        "task_finish_req": _h_task_finish_req,
        "slave_information_req": _h_slave_information_req
    }
