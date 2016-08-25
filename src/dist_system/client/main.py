#!/usr/bin/python3
#-*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import ResultMessageHandler
from .controller import *
from ..protocol import client_master, any_result_receiver
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
        header, body = client_master.parse_msg_data(data)
        return header, body

    def _resolve_msg(self, msg):
        return msg[0]

    async def dispatch_msg_coro(self, header, body):
        data = client_master.make_msg_data(header, body)
        msg = [data]
        await self._sock.send_multipart(msg)

    def dispatch_msg(self, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)


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
        header, body = any_result_receiver.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[1]

        return addr, data

    async def dispatch_msg_coro(self, addr, header, body):
        data = any_result_receiver.make_msg_data(header, body)
        msg = [addr, data]
        await self._router.send_multipart(msg)

    def dispatch_msg(self, addr, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(addr, header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)


async def run_client(context : Context, master_addr, result_router_addr, raw_result_receiver_address : str):

    result_router = ResultReceiverCommunicationRouter(context, result_router_addr, ResultMessageHandler())

    await asyncio.wait([
        asyncio.ensure_future(result_router.run()),
        asyncio.ensure_future(simulate_task(context, master_addr, raw_result_receiver_address))
    ])


def main(master_addr, result_router_addr, raw_result_receiver_address : str):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_client(context, master_addr, result_router_addr, raw_result_receiver_address))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)