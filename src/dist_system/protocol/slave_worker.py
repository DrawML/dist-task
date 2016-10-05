from dist_system.protocol.base_protocol import BaseProtocol
from dist_system.protocol.pb import slave_worker_pb2 as sw_proto

message_table = {
    'task_register_cmd': {
        'this': sw_proto.TaskRegisterCMD,
        'result_receiver_address': sw_proto.TaskRegisterCMD.ResultReceiverAddress,
        'task': {
            'sleep_task': sw_proto.TaskRegisterCMD.SleepTask,
            'data_processing_task': sw_proto.TaskRegisterCMD.DataProcessingTask,
            'tensorflow_train_task': sw_proto.TaskRegisterCMD.TensorflowTrainTask,
            'tensorflow_test_task': sw_proto.TaskRegisterCMD.TensorflowTestTask,
        },
    },
    'worker_register_req': {
        'this': sw_proto.WorkerRegisterRequest,
    },
    'worker_register_res': {
        'this': sw_proto.WorkerRegisterResponse,
    },
    'task_cancel_req': {
        'this': sw_proto.TaskCancelRequest,
    },
    'task_cancel_res': {
        'this': sw_proto.TaskCancelResponse,
    },
    'task_finish_req': {
        'this': sw_proto.TaskFinishRequest,
    },
    'task_finish_res': {
        'this': sw_proto.TaskFinishResponse,
    },
}

protocol = BaseProtocol(sw_proto, message_table)


# input : string, dict
# output : bytes
def make_msg_data(header, body):
    return protocol.make_msg_data(header, body)


# input : bytes
# output : (string, dict)
def parse_msg_data(msg_data):
    return protocol.parse_msg_data(msg_data)
