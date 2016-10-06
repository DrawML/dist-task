import asyncio
import traceback

import zmq

from dist_system.library import SingletonMeta
from dist_system.logger import Logger
from dist_system.protocol import master_slave, client_master


class ClientRouter(metaclass=SingletonMeta):
    def __init__(self, context, addr, msg_handler):
        self._context = context
        self._addr = addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.ROUTER)
        self._router.bind(self._addr)
        Logger().log("client router bind to {0}".format(self._addr))

        while True:
            msg = await self._router.recv_multipart()
            await self._process(msg)

    async def _process(self, msg):
        # Logger().log("client router recv a message : {0}".format(msg))
        addr, data = self._resolve_msg(msg)
        try:
            header, body = client_master.parse_msg_data(data)
        except BaseException as e:
            print(e)

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[2]

        return addr, data

    async def dispatch_msg_coro(self, client_session_identity, header, body):
        Logger().log("to client({0}), header={1}, body={2}".format(client_session_identity, header, body), level=2)
        addr = client_session_identity.addr
        try:
            data = client_master.make_msg_data(header, body)
        except Exception as e:
            print(traceback.format_exc())
            raise
        msg = [addr, b'', data]
        await self._router.send_multipart(msg)

    def dispatch_msg(self, client_session_identity, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(client_session_identity, header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)


class SlaveRouter(metaclass=SingletonMeta):
    def __init__(self, context, addr, msg_handler):
        self._context = context
        self._addr = addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.ROUTER)
        self._router.bind(self._addr)
        Logger().log("slave router bind to {0}".format(self._addr))

        while True:
            msg = await self._router.recv_multipart()
            await self._process(msg)

    async def _process(self, msg):
        # Logger().log("slave router recv a message : {0}".format(msg))
        addr, data = self._resolve_msg(msg)
        header, body = master_slave.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    async def dispatch_msg_coro(self, slave_identity, header, body):
        Logger().log("to slave({0}), header={1}, body={2}".format(slave_identity, header, body), level=2)
        addr = slave_identity.addr
        try:
            data = master_slave.make_msg_data(header, body)
        except Exception as e:
            print(traceback.format_exc())
            raise
        msg = [addr, data]
        await self._router.send_multipart(msg)

    def dispatch_msg(self, slave_identity, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(slave_identity, header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)
