from .proto import master_slave_pb2 as ms_proto
from .exceptions import *


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
        'task_register_req' : {
            'this': ms_proto.TaskRegisterRequest,
            'result_receiver_address': ms_proto.TaskRegisterRequest.ResultReceiverAddress,
            'task': {
                'sleep_task': ms_proto.TaskRegisterRequest.SleepTask,
                'tensorflow_learning_task': ms_proto.TaskRegisterRequest.TensorflowLearningTask,
                'tensorflow_test_task': ms_proto.TaskRegisterRequest.TensorflowTestTask
            }
        },
        'task_register_res' : {
            'this': ms_proto.TaskRegisterResponse,
        },
        'task_cancel_req' : {
            'this': ms_proto.TaskCancelRequest,
        },
        'task_cancel_res' : {
            'this': ms_proto.TaskCancelResponse,
        },
        'task_finish_req' : {
            'this': ms_proto.TaskFinishRequest,
        },
        'task_finish_res' : {
            'this': ms_proto.TaskFinishResponse,
        },
    }


def handle_extra(data, key, cls):
    if key == 'task':
        data[data[key + '_type']] = cls[data[key + '_type']](**data[key])

        data.pop(key + '_type')
        data.pop(key)
    else:
        raise HandleError('%s is not valid key' % key)


def dictify_from_body(body):
    field_list = body.ListFields()
    field_dict = dict()

    for desc, val in field_list:
        name, value = desc.name, val

        if hasattr(val, 'ListFields'):
            value = dictify_from_body(val)
            if desc.containing_oneof:
                name = desc.containing_oneof.name
                field_dict[name + '_type'] = desc.name

        field_dict[name] = value
        
    return field_dict


# input : string, dict
# output : bytes
def make_msg_data(header, body):
    import copy
    data = copy.deepcopy(body) 

    msg_meta = message_table.get(header)

    if msg_meta == None:
        raise HeaderError('%s is not valid header' % header)
    
    for key, cls in msg_meta.items():
        if key == 'this':
            msg_type = cls
        else:
            if type(cls) is dict:
                handle_extra(data, key, cls)
            else:
                data[key] = cls(**data[key])


    msg = { header: message_table[header]['this'](**data) }
    return ms_proto.Message(**msg).SerializeToString()


# input : bytes
# output : (string, dict)
def parse_msg_data(packet):
    msg = ms_proto.Message()
    msg.ParseFromString(packet)
    return msg.WhichOneof('body'), dictify_from_body(msg.__getattribute__(msg.WhichOneof('body')))

