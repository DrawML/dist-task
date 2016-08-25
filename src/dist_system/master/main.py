import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import (ClientMessageHandler, SlaveMessageHandler)
from .msg_dispatcher import *
from .controller import run_heartbeat
from ..protocol import master_slave, client_master
from ..library import SingletonMeta
from functools import partial
from .task import TaskManager


class ClientRouter(metaclass=SingletonMeta):

    def __init__(self, context, addr, msg_handler):
        self._context = context
        self._addr = addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.ROUTER)
        self._router.bind(self._addr)

        while True:
            msg = await self._router.recv_multipart()
            await self._process(msg)

    async def _process(self, msg):
        addr, data = self._resolve_msg(msg)
        header, body = client_master.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    async def dispatch_msg_coro(self, client_session_identity, header, body):
        addr = client_session_identity.addr
        data = client_master.make_msg_data(header, body)
        msg = [addr, data]
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

        while True:
            msg = await self._router.recv_multipart()
            await self._process(msg)

    async def _process(self, msg):
        addr, data = self._resolve_msg(msg)
        header, body = master_slave.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    async def dispatch_msg_coro(self, slave_identity, header, body):
        addr = slave_identity.addr
        data = client_master.make_msg_data(header, body)
        msg = [addr, data]
        await self._router.send_multipart(msg)

    def dispatch_msg(self, slave_identity, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(slave_identity, header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)


async def run_master(context : Context, client_router_addr, slave_router_addr):

    TaskManager()

    client_router = ClientRouter(context, client_router_addr, ClientMessageHandler())
    slave_router = SlaveRouter(context, slave_router_addr, SlaveMessageHandler())

    ClientMessageDispatcher(partial(client_router.dispatch_msg, client_router))
    SlaveMessageDispatcher(partial(slave_router.dispatch_msg, slave_router))

    await asyncio.wait([
        asyncio.ensure_future(client_router.run()),
        asyncio.ensure_future(slave_router.run()),
        asyncio.ensure_future(run_heartbeat())
    ])


def main(client_router_addr, slave_router_addr):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_master(context, client_router_addr, slave_router_addr))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)