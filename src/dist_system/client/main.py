#!/usr/bin/python3
#-*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import ResultMessageHandler
from .controller import *
from ..protocol import master_slave, slave_worker, any_result_receiver
from functools import partial
from ..library import SingletonMeta
from ..task.task import *
from ..task.functions import *
from .task import TaskManager
from .simulator import simulate_task


class MasterConnection(object):

    def __init__(self, context : zmq.asyncio.Context, master_addr):
        self._context = context
        self._master_addr = master_addr

    def connect(self):
        self._sock = self._context.socket(zmq.REQ)
        self._sock.connect(self._master_addr)

    def close(self):
        self._sock.close()

    async def recv_msg(self):
        msg = await self._sock.recv_multipart()
        data = self._resolve_msg(msg)
        header, body = master_slave.parse_msg_data(data)
        return header, body

    def _resolve_msg(self, msg):
        return msg[0]

    def dispatch_msg(self, header, body, async_=True):

        def _dispatch_msg_sync(msg):
            asyncio.wait([self._sock.send_multipart(msg)])

        def _dispatch_msg_async(msg):
            asyncio.ensure_future(self._sock.send_multipart(msg))

        data = master_slave.make_msg_data(header, body)
        msg = [data]
        if async_:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


class ResultReceiverCommunicationRouter(metaclass=SingletonMeta):

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
        addr, data = self._resolve_msg(msg)
        header, body = slave_worker.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    def dispatch_msg(self, addr, header, body, async_=True):

        def _dispatch_msg_sync(msg):
            asyncio.wait([self._router.send_multipart(msg)])

        def _dispatch_msg_async(msg):
            asyncio.ensure_future(self._router.send_multipart(msg))

        data = slave_worker.make_msg_data(header, body)
        msg = [addr, data]
        if async_:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


async def run_master(context : Context, master_addr, result_router_addr, result_receiver_address):

    result_router = ResultReceiverCommunicationRouter(context, result_router_addr, ResultMessageHandler())

    await asyncio.wait([
        asyncio.ensure_future(result_router.run()),
        asyncio.ensure_future(simulate_task(context, master_addr, result_receiver_address))
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