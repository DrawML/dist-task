"""
case if client send invalid address??
issue...
"""
from dist_system.address import Address


class ResultReceiverAddress(Address):
    pass


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
