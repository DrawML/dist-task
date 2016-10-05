from dist_system.protocol.base_protocol import BaseProtocol
from dist_system.protocol.pb import client_master_pb2 as cm_proto

message_table = {
    'task_register_req': {
        'this': cm_proto.TaskRegisterRequest,
        'result_receiver_address': cm_proto.TaskRegisterRequest.ResultReceiverAddress,
        'task': {
            'sleep_task': cm_proto.TaskRegisterRequest.SleepTask,
            'data_processing_task': cm_proto.TaskRegisterRequest.DataProcessingTask,
            'tensorflow_train_task': cm_proto.TaskRegisterRequest.TensorflowTrainTask,
            'tensorflow_test_task': cm_proto.TaskRegisterRequest.TensorflowTestTask,
        }
    },
    'task_register_res': {
        'this': cm_proto.TaskRegisterResponse,
    },
    'task_register_ack': {
        'this': cm_proto.TaskRegisterACK,
    },
    'task_cancel_req': {
        'this': cm_proto.TaskCancelRequest,
    },
    'task_cancel_res': {
        'this': cm_proto.TaskCancelResponse,
    },
}

protocol = BaseProtocol(cm_proto, message_table)


# input : string, dict
# output : bytes
def make_msg_data(header, body):
    return protocol.make_msg_data(header, body)


# input : bytes
# output : (string, dict)
def parse_msg_data(msg_data):
    return protocol.parse_msg_data(msg_data)
