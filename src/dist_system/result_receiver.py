"""
case if client send invalid address??
issue...
"""


class ResultReceiverAddress(object):
    def __init__(self, type_: str, ip: str, port: int):
        assert type_ == 'tcp'
        self._type = type_
        self._ip = ip
        self._port = port

    def to_dict(self):
        return {
            'type': self._type,
            'ip': self._ip,
            'port': self._port
        }

    @staticmethod
    def from_dict(dict_: dict):
        return ResultReceiverAddress(dict_['type'], dict_['ip'], dict_['port'])

    def to_zeromq_addr(self):
        # need to refine.
        return "{0}://{1}:{2}".format(self._type, self._ip, self._port)


class ResultReceiverCommunicator(object):
    def __init__(self, f_connect, f_send_msg, f_recv_msg, f_close):
        self._f_connect = f_connect
        self._f_send_msg = f_send_msg
        self._f_recv_msg = f_recv_msg
        self._f_close = f_close

    def communicate(self, result_receiver_address, msg_header, msg_body):
        self._f_connect(result_receiver_address)
        self._f_send_msg(msg_header, msg_body)
        header, body = self._f_recv_msg()
        self._f_close()
        return header, body

    def notify(self, result_receiver_address, msg_header, msg_body):
        self._f_connect(result_receiver_address)
        self._f_send_msg(msg_header, msg_body)
        self._f_close()
