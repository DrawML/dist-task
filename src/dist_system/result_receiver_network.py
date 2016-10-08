import asyncio

import zmq

from dist_system.protocol import any_result_receiver
from dist_system.library import SingletonMeta
from dist_system.logger import Logger


"""
case if client send invalid address??
issue...
"""


class ResultReceiverCommunicator(metaclass=SingletonMeta):
    def __init__(self, context=None):
        self._context = context or zmq.Context()
        self._sock = None

    async def communicate(self, result_receiver_address, msg_header, msg_body):
        self._connect(result_receiver_address)
        await self._send_msg(msg_header, msg_body)
        header, body = await self._recv_msg()
        self._close()
        return header, body

    async def notify(self, result_receiver_address, msg_header, msg_body):
        self._connect(result_receiver_address)
        await self._send_msg(msg_header, msg_body)
        self._close()

    def _connect(self, result_receiver_address):
        assert self._sock is None
        self._result_receiver_address = result_receiver_address
        self._sock = self._context.socket(zmq.REQ)
        self._sock.connect(result_receiver_address.to_zeromq_addr())
        Logger().log("Connect to", result_receiver_address.to_zeromq_addr())

    async def _send_msg(self, msg_header, msg_body):
        assert self._sock is not None
        data = any_result_receiver.make_msg_data(msg_header, msg_body)
        Logger().log("To result receiver, header={0}, body={1}".format(msg_header, msg_body))
        await self._sock.send(data)

    async def _recv_msg(self):
        assert self._sock is not None
        header, body = any_result_receiver.parse_msg_data(await self._sock.recv())
        Logger().log("From result receiver, header={0}, body={1}".format(header, body))
        return header, body

    def _close(self):
        assert self._sock is not None
        self._sock.close()
        self._sock = None
        Logger().log('Close a connection to result receiver')
