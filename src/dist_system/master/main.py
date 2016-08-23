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

    def dispatch_msg(self, session_identity, header, body, async=True):

        def _dispatch_msg_sync(msg):
            asyncio.wait([self._router.send_multipart(msg)])

        def _dispatch_msg_async(msg):
            asyncio.ensure_future(self._router.send_multipart(msg))

        addr = session_identity.addr
        data = client_master.make_msg_data(header, body)
        msg = [addr, data]
        if async:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


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

    def dispatch_msg(self, slave_identity, header, body, async=True):

        def _dispatch_msg_sync(msg):
            asyncio.wait([self._router.send_multipart(msg)])

        def _dispatch_msg_async(msg):
            asyncio.ensure_future(self._router.send_multipart(msg))

        addr = slave_identity.addr
        data = master_slave.make_msg_data(header, body)
        msg = [addr, data]
        if async:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


async def run_master(context : Context, client_router_addr, slave_router_addr):

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