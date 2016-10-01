from dist_system.protocol import HeaderError


class BaseProtocol:
    def __init__(self, protocol, msg_table):
        self.protocol = protocol
        self.message_table = msg_table

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
            elif hasattr(val, 'sort') and hasattr(val[0], 'ListFields'):    # when list
                value = list(map(lambda x: self.dictify_from_body(x), val))

            field_dict[name] = value

        return field_dict

    # input : string, dict
    # output : bytes
    def make_msg_data(self, header, body):

        except_fields = ['task_type']

        def get_adjust_key_name(key, outer_body):
            if key == 'task':
                return outer_body['task_type']
            elif key == 'result':
                return outer_body['task_type']
            else:
                return key

        def handle_exception_case(key, meta, outer_body, outer_data):
            if key == 'task' or key == 'result':
                adj_key = get_adjust_key_name(key, outer_body)
                except_fields.append(key)
                return meta[adj_key](**outer_body[key])
            else:
                raise ValueError

        def make_msg_data_from_body(meta, body):
            if type(meta) is dict:
                data = {}
                for key, inner_meta in meta.items():
                    if key == 'this':
                        continue
                    try:
                        inner_data = handle_exception_case(key, inner_meta, body, data)
                    except ValueError:
                        inner_data = make_msg_data_from_body(inner_meta, body[key])
                    adj_key = get_adjust_key_name(key, body)
                    data[adj_key] = inner_data
            elif type(meta) is list:
                data = []
                for inner_body in body:
                    data.append(make_msg_data_from_body(meta[0], inner_body))
            else:
                data = meta(**body)
            return data

        root_meta = self.message_table.get(header)
        if root_meta is None:
            raise HeaderError('%s is not valid header' % header)

        data = make_msg_data_from_body(root_meta, body)

        for key, value in body.items():
            if not key in data and not key in except_fields:
                data[key] = body[key]

        msg = {header: root_meta['this'](**data)}
        return self.protocol.Message(**msg).SerializeToString()

    # input : bytes
    # output : (string, dict)
    def parse_msg_data(self, msg_data):
        msg = self.protocol.Message()
        msg.ParseFromString(msg_data)
        return msg.WhichOneof('body'), self.dictify_from_body(msg.__getattribute__(msg.WhichOneof('body')))
