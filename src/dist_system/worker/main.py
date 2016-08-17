#!/usr/bin/python3
#-*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import SlaveMessageHandler
from .controller import (TaskInformation, do_task)


class SlaveConnection(object):

    def __init__(self, context, slave_addr, msg_handler):
        self._context = context
        self._slave_addr = slave_addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.DEALER)
        self._router.connect(self._slave_addr)
        self._register()

        while True:
            msg = await self._router.recv_multipart()
            self._process(msg)

    def _register(self):
        raise NotImplementedError("send worker_register_req to slave using protocol")

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


async def run_worker(context : Context, slave_addr, serialized_data : bytes):

    slave_conn = SlaveConnection(context, slave_addr, SlaveMessageHandler())

    asyncio.wait([
        asyncio.ensure_future(slave_conn.run()),
        asyncio.ensure_future(do_task(context, TaskInformation.from_bytes(serialized_data)))
    ])


def main(slave_addr, serialized_data : bytes):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_worker(context, slave_addr, serialized_data))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)