from .proto import slave_worker_pb2 as sw_proto
from .exceptions import *


#class TaskRegisterCMD:
#    @staticmethod
#    def serialize(data):
        

message_table = {
        'worker_register_req' : {
            'this': sw_proto.WorkerRegisterRequest,
        },
        'worker_register_res' : {
            'this': sw_proto.WorkerRegisterResponse,
        },
        'task_cancel_req' : {
            'this': sw_proto.TaskCancelRequest,
        },
        'task_cancel_res' : {
            'this': sw_proto.TaskCancelResponse,
        },
        'task_finish_req' : {
            'this': sw_proto.TaskFinishRequest,
        },
        'task_finish_res' : {
            'this': sw_proto.TaskFinishResponse,
        },
    }


def handle_extra(data, key, cls):
    pass


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
    return sw_proto.Message(**msg).SerializeToString()


# input : bytes
# output : (string, dict)
def parse_msg_data(msg_data):
    msg = sw_proto.Message()
    msg.ParseFromString(msg_data)
    return msg.WhichOneof('body'), dictify_from_body(msg.__getattribute__(msg.WhichOneof('body')))

