import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import ClientMessageHandler, SlaveMessageHandler
from .controller import run_heartbeat


class ClientRouter(object):

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
        # separate data into header and body using protocol.*
        addr, data = self._resolve_msg(msg)
        header, body = ("dummy", "dummy")

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    def dispatch_msg(self, addr, data, async=True):

        # need to modify. same bugs exist in several files.
        def _dispatch_msg_sync(msg):
            self._router.send_multipart(msg)

        def _dispatch_msg_async(msg):
            async def _dispatch_msg(msg):
                await self._router.send_multipart(msg)
            asyncio.ensure_future(_dispatch_msg(msg))

        msg = [addr, data]
        if async:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


class SlaveRouter(object):
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
        # separate data into header and body using protocol.*
        addr, data = self._resolve_msg(msg)
        header, body = ("dummy", "dummy")

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    def dispatch_msg(self, addr, data, async=True):

        def _dispatch_msg_sync(msg):
            self._router.send_multipart(msg)

        def _dispatch_msg_async(msg):
            async def _dispatch_msg(msg):
                await self._router.send_multipart(msg)

            asyncio.ensure_future(_dispatch_msg(msg))

        msg = [addr, data]
        if async:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


async def run_master(context : Context, client_router_addr, slave_router_addr):

    client_router = ClientRouter(context, client_router_addr, ClientMessageHandler())
    slave_router = SlaveRouter(context, slave_router_addr, SlaveMessageHandler())

    asyncio.wait([
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