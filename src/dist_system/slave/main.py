#!/usr/bin/python3
#-*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import (MasterMessageHandler, WorkerMessageHandler)
from .controller import (WorkerCreator, run_polling_workers)


class MasterConnection(object):

    def __init__(self, context, master_addr, msg_handler):
        self._context = context
        self._master_addr = master_addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.DEALER)
        self._router.connect(self._master_addr)
        self._register()

        while True:
            msg = await self._router.recv_multipart()
            self._process(msg)

    def _register(self):
        raise NotImplementedError("send slave_register_req to master using protocol")

    def _process(self, msg):
        # separate data into header and body using protocol.*
        data = self._resolve_msg(msg)
        header, body = ("dummy", "dummy")

        # handle message
        self._msg_handler.handle_msg(header, body)

    def _resolve_msg(self, msg):
        return msg[0]

    def dispatch_msg(self, data, async=True):

        def _dispatch_msg_sync(msg):
            self._router.send_multipart(msg)

        def _dispatch_msg_async(msg):
            async def _dispatch_msg(msg):
                await self._router.send_multipart(msg)

            asyncio.ensure_future(_dispatch_msg(msg))

        msg = [data]
        if async:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


class WorkerRouter(object):

    def __init__(self, context, addr, msg_handler):
        self._context = context
        self._addr = addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.ROUTER)
        self._router.bind(self._addr)

        while True:
            msg = await self._router.recv_multipart()
            self._process(msg)

    def _process(self, msg):
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


async def run_master(context : Context, master_addr, worker_router_addr, worker_file_name):

    master_conn = MasterConnection(context, master_addr, MasterMessageHandler())
    worker_router = WorkerRouter(context, worker_router_addr, WorkerMessageHandler())
    WorkerCreator(worker_file_name)

    asyncio.wait([
        asyncio.ensure_future(master_conn.run()),
        asyncio.ensure_future(worker_router.run()),
        asyncio.ensure_future(run_polling_workers())
    ])


def main(master_addr, worker_router_addr):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_master(context, master_addr, worker_router_addr))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)