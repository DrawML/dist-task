import traceback

from dist_system.information import AllocationInformation, AllocatedResource, SlaveInformation, \
    AllocationTensorflowGpuInformation
from dist_system.library import SingletonMeta
from dist_system.logger import Logger
from dist_system.master.client import ClientSessionIdentity, ClientSession, ClientSessionManager, \
    ClientSessionValueError
from dist_system.master.controller import Scheduler, delete_task
from dist_system.master.msg_dispatcher import ClientMessageDispatcher, SlaveMessageDispatcher
from dist_system.master.slave import SlaveManager, SlaveValueError, SlaveIdentity, Slave
from dist_system.master.task import TaskManager, TaskStatus, TaskStatusValueError
from dist_system.address import ResultReceiverAddress
from dist_system.task import TaskToken, TaskType, TaskValueError, TaskTypeValueError
from dist_system.task.functions import make_task_with_task_type


class ClientMessageHandler(metaclass=SingletonMeta):
    def __init__(self):
        pass

    def handle_msg(self, addr, header, body):
        session_identity = ClientSessionIdentity(addr)
        msg_name = header
        Logger().log("client identity={0}, header={1}, body={2}".format(session_identity, header, body), level=2)
        try:
            ClientMessageHandler.__handler_dict[msg_name](self, session_identity, body)
        except Exception:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n{0}".
                         format(traceback.format_exc()))
        Logger().log("finish of handling client message.", level=2)

    def _h_task_register_req(self, session_identity, body):
        try:
            result_receiver_address = ResultReceiverAddress.from_dict(body['result_receiver_address'])
            task_token = TaskToken.get_avail_token()
            task_type = TaskType.from_str(body['task_type'])
            task = make_task_with_task_type(task_type, body['task'], 'master',
                                            task_token, result_receiver_address)
            try:
                session = ClientSession.make_session_from_identity(session_identity, task)
                ClientSessionManager().add_session(session)
                try:
                    TaskManager().add_task(task)
                    try:
                        res_body = {
                            'status': 'success',
                            'task_token': task_token.to_bytes()
                        }
                    except:
                        delete_task(task)
                        raise
                except:
                    ClientSessionManager().del_session(session)
                    raise
            except:
                raise
        except TaskTypeValueError:
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_task'
            }
            raise
        except TaskValueError:
            res_body = {
                'status': 'fail',
                'error_code': 'invalid_task'
            }
            raise
        except:
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            ClientMessageDispatcher().dispatch_msg(session_identity, 'task_register_res', res_body)

    def _h_task_register_ack(self, session_identity, body):
        session = ClientSessionManager().find_session(session_identity)
        try:
            task = session.task
            TaskManager().change_task_status(task, TaskStatus.STATUS_WAITING)
        finally:
            ClientSessionManager().del_session(session)

        Scheduler().invoke()

    def _h_task_cancel_req(self, session_identity, body):
        try:
            task_token = TaskToken.from_bytes(body['task_token'])

            task = TaskManager().find_task(task_token)

            try:
                delete_task(task)
            finally:
                slave = SlaveManager().find_slave_having_task(task)
                slave.delete_task(task)

                SlaveMessageDispatcher().dispatch_msg(slave, 'task_cancel_req', {
                    'task_token': task_token.to_bytes()
                })
            res_body = {
                'status': 'success'
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
        Logger().log("slave identity={0}, header={1}, body={2}".format(slave_identity, header, body), level=2)
        try:
            SlaveMessageHandler.__handler_dict[msg_name](self, slave_identity, body)
        except Exception:
            Logger().log("Unknown Exception occurs! Pass it for continuous running.\n{0}".
                         format(traceback.format_exc()))
        Logger().log("finish of handling slave message", level=2)

    def _h_heart_beat_res(self, slave_identity, body):
        SlaveManager().find_slave(slave_identity).heartbeat()

    def _h_slave_register_req(self, slave_identity, body):
        try:
            SlaveManager().add_slave(Slave.make_slave_from_identity(slave_identity))
            res_body = {
                'status': 'success'
            }
        except:
            res_body = {
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            SlaveMessageDispatcher().dispatch_msg(slave_identity, 'slave_register_res', res_body)

        Scheduler().invoke()

    def _h_task_register_res(self, slave_identity, body):

        slave = SlaveManager().find_slave(slave_identity)
        status = body['status']
        task_token = TaskToken.from_bytes(body['task_token'])
        task = TaskManager().find_task(task_token)

        if task.status != TaskStatus.STATUS_PROCESSING:
            raise TaskStatusValueError('Invalid Task Status')

        if status == 'success':
            pass
        elif status == 'fail':
            error_code = body['error_code']
            # if a exception occurs in here, that means a invalid message.
            TaskManager().change_task_status(task, TaskStatus.STATUS_WAITING)
            slave.delete_task(task)
            slave.mark_failed_task(task)
            try:
                self._free_resource(slave.alloc_info, task.allocated_resource)
            finally:
                try:
                    TaskManager().redo_leak_task(task)
                finally:
                    Scheduler().invoke()
        else:
            # invalid message
            raise ValueError('Invalid Task Register Status.')

    def _h_task_cancel_res(self, slave_identity, body):
        # no specific handling.
        pass

    def _h_task_finish_req(self, slave_identity, body):

        Logger().log('* TASK FINISH.')

        task_token = TaskToken.from_bytes(body['task_token'])
        try:
            task = TaskManager().find_task(task_token)

            if task.status != TaskStatus.STATUS_PROCESSING:
                raise TaskStatusValueError('Invalid Task Status')

            TaskManager().change_task_status(task, TaskStatus.STATUS_COMPLETE)  # yes, there is no need of this code.

            try:
                delete_task(task)
            finally:
                slave = SlaveManager().find_slave(slave_identity)
                slave.delete_task(task)
                self._free_resource(slave.alloc_info, task.allocated_resource)

            res_body = {
                'task_token': task_token.to_bytes(),
                'status': 'success'
            }
        except TaskValueError:
            res_body = {
                'task_token': task_token.to_bytes(),
                'status': 'fail',
                'error_code': 'invalid_token'
            }
            raise
        except:
            res_body = {
                'task_token': task_token.to_bytes(),
                'status': 'fail',
                'error_code': 'unknown'
            }
            raise
        finally:
            SlaveMessageDispatcher().dispatch_msg(slave_identity, 'task_finish_res', res_body)

    def _h_slave_information_req(self, slave_identity, body):

        slave = SlaveManager().find_slave(slave_identity)
        slave_info = SlaveInformation.from_dict(body)

        if slave.alloc_info is None:
            slave.alloc_info = AllocationInformation(
                0,
                slave_info.cpu_info.cpu_count,
                [AllocationTensorflowGpuInformation(True, tf_gpu_info)
                 for tf_gpu_info in slave_info.tf_gpu_info_list]
            )
        else:
            assert slave_info.cpu_info.cpu_count == slave.alloc_info.all_cpu_count

            alloc_tf_gpu_info_list = slave.alloc_info.alloc_tf_gpu_info_list
            new_tf_gpu_info_list = []

            for alloc_tf_gpu_info in alloc_tf_gpu_info_list:
                targets = [x for x in slave_info.tf_gpu_info_list if alloc_tf_gpu_info.tf_device == x.tf_device]
                assert len(targets) == 1
                new_tf_gpu_info_list.append(targets[0])

            # for strong guarantee
            for alloc_tf_gpu_info, new_tf_gpu_info in zip(alloc_tf_gpu_info_list, new_tf_gpu_info_list):
                alloc_tf_gpu_info.tf_gpu_info = new_tf_gpu_info

            # for strong guarantee
            slave.slave_info = slave_info
            Scheduler().invoke(invoke_log=False)

    __handler_dict = {
        "heart_beat_res": _h_heart_beat_res,
        "slave_register_req": _h_slave_register_req,
        "task_register_res": _h_task_register_res,
        "task_cancel_res": _h_task_cancel_res,
        "task_finish_req": _h_task_finish_req,
        "slave_information_req": _h_slave_information_req
    }

    def _free_resource(self, alloc_info: AllocationInformation, allocated_resource: AllocatedResource):

        Logger().log("Free Resource :", allocated_resource)

        if alloc_info.alloc_cpu_count < allocated_resource.alloc_cpu_count:
            raise ValueError('Invalid allocated resource. (alloc_cpu_count)')

        alloc_info.alloc_cpu_count -= allocated_resource.alloc_cpu_count
        if allocated_resource.alloc_tf_gpu_info is not None:
            allocated_resource.alloc_tf_gpu_info.available = True
