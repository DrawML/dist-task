from dist_system.protocol.base_protocol import BaseProtocol
from dist_system.protocol.pb import master_slave_pb2 as ms_proto

message_table = {
    'heart_beat_req': {
        'this': ms_proto.HeartBeatRequest,
    },
    'heart_beat_res': {
        'this': ms_proto.HeartBeatResponse,
    },
    'slave_register_req': {
        'this': ms_proto.SlaveRegisterRequest,
    },
    'slave_register_res': {
        'this': ms_proto.SlaveRegisterResponse,
    },
    'task_register_req': {
        'this': ms_proto.TaskRegisterRequest,
        'result_receiver_address': ms_proto.TaskRegisterRequest.ResultReceiverAddress,
        'task': {
            'sleep_task': ms_proto.TaskRegisterRequest.SleepTask,
            'data_processing_task': ms_proto.TaskRegisterRequest.DataProcessingTask,
            'tensorflow_train_task': ms_proto.TaskRegisterRequest.TensorflowTrainTask,
            'tensorflow_test_task': ms_proto.TaskRegisterRequest.TensorflowTestTask,
        }
    },
    'task_register_res': {
        'this': ms_proto.TaskRegisterResponse,
    },
    'task_cancel_req': {
        'this': ms_proto.TaskCancelRequest,
    },
    'task_cancel_res': {
        'this': ms_proto.TaskCancelResponse,
    },
    'task_finish_req': {
        'this': ms_proto.TaskFinishRequest,
    },
    'task_finish_res': {
        'this': ms_proto.TaskFinishResponse,
    },
    'slave_information_req': {
        'this': ms_proto.SlaveInformationRequest,
        'cpu_info': ms_proto.SlaveInformationRequest.CpuInfo,
        'mem_info': ms_proto.SlaveInformationRequest.MemInfo,
        'tf_gpu_info_list': [ms_proto.SlaveInformationRequest.TfGpuInfo],
    },
}

protocol = BaseProtocol(ms_proto, message_table)


# input : string, dict
# output : bytes
def make_msg_data(header, body):
    return protocol.make_msg_data(header, body)


# input : bytes
# output : (string, dict)
def parse_msg_data(msg_data):
    return protocol.parse_msg_data(msg_data)
