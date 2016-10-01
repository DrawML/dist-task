from dist_system.protocol.base_protocol import BaseProtocol
from dist_system.protocol.pb import any_result_receiver_pb2 as arr_proto

message_table = {
    'task_result_req': {
        'this': arr_proto.TaskResultRequest,
        'result': {
            'sleep_task': arr_proto.TaskResultRequest.SleepTask,
            'data_processing_task': arr_proto.TaskResultRequest.DataProcessingTask,
            'tensorflow_task': arr_proto.TaskResultRequest.TensorflowTask
        }
    },
    'task_result_res': {
        'this': arr_proto.TaskResultResponse,
    },
}

protocol = BaseProtocol(arr_proto, message_table)


# input : string, dict
# output : bytes
def make_msg_data(header, body):
    return protocol.make_msg_data(header, body)


# input : bytes
# output : (string, dict)
def parse_msg_data(msg_data):
    return protocol.parse_msg_data(msg_data)
