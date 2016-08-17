from .proto import any_result_receiver_pb2 as arr_proto
from .exceptions import *

message_table = {
        'task_result_req' : {
            'this': arr_proto.TaskResultRequest,
            'result': {
                'sleep_task': arr_proto.TaskResultRequest.SleepTask,
                'tensorflow_learning_task': arr_proto.TaskResultRequest.TensorflowLearningTask,
                'tensorflow_test_task': arr_proto.TaskResultRequest.TensorflowTestTask
            }
        },
        'task_result_res' : {
            'this': arr_proto.TaskResultResponse,
        },
    }

def handle_extra(data, key, cls):
    if key == 'result':
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


def make_packet(header, body):
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
    return arr_proto.Message(**msg).SerializeToString()

def parse_packet(packet):
    msg = arr_proto.Message()
    msg.ParseFromString(packet)
    return msg.WhichOneof('body'), dictify_from_body(msg.__getattribute__(msg.WhichOneof('body')))
