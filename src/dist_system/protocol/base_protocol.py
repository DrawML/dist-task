from .exceptions import *


class BaseProtocol:
    def __init__(self, protocol, msg_table):
        self.protocol = protocol
        self.message_table = msg_table

    def handle_extra(self, data, key, klass):
        if key == 'task':
            data[data[key + '_type']] = klass[data[key + '_type']](**data[key])

            data.pop(key + '_type')
            data.pop(key)
        elif key == 'result':
            data[data['task_type']] = klass[data['task_type']](**data[key])

            data.pop('task_type')
            data.pop(key)
        else:
            raise HandleError('%s is not valid key' % key)

    def dictify_from_body(self, body):
        field_list = body.ListFields()
        field_dict = dict()

        for desc, val in field_list:
            name, value = desc.name, val

            if hasattr(val, 'ListFields'):
                value = self.dictify_from_body(val)
                if desc.containing_oneof:
                    name = desc.containing_oneof.name
                    field_dict[name + '_type'] = desc.name

            field_dict[name] = value

        return field_dict

    # input : string, dict
    # output : bytes
    def make_msg_data(self, header, body):
        import copy
        data = copy.deepcopy(body)

        msg_meta = self.message_table.get(header)

        if msg_meta is None:
            raise HeaderError('%s is not valid header' % header)

        for key, klass in msg_meta.items():
            if key == 'this':
                msg_klass = klass
            else:
                if type(klass) is dict:
                    self.handle_extra(data, key, klass)
                elif type(klass) is list:
                    element_klass = klass[0]
                    data[key] = list(map(lambda x: element_klass(**x), data[key]))
                else:
                    data[key] = klass(**data[key])

        msg = {header: msg_klass(**data)}
        return self.protocol.Message(**msg).SerializeToString()

    # input : bytes
    # output : (string, dict)
    def parse_msg_data(self, msg_data):
        msg = self.protocol.Message()
        msg.ParseFromString(msg_data)
        return msg.WhichOneof('body'), self.dictify_from_body(msg.__getattribute__(msg.WhichOneof('body')))
