#!/usr/bin/python3
#-*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import SlaveMessageHandler
from .controller import (TaskInformation, do_task)
from .msg_dispatcher import *
from ..protocol import any_result_receiver
from .result_receiver import *
from functools import partial


class SlaveConnection(object):

    def __init__(self, context, slave_addr, msg_handler):
        self._context = context
        self._slave_addr = slave_addr
        self._msg_handler = msg_handler

    async def run(self, task_information : TaskInformation):
        self._dealer = self._context.socket(zmq.DEALER)
        self._dealer.connect(self._slave_addr)
        self._register(task_information.task_token)

        while True:
            msg = await self._dealer.recv_multipart()
            self._process(msg)

    def _register(self, task_token):
        SlaveMessageDispatcher().dispatch_msg('worker_register_req', {
            'task_token' : task_token.to_bytes()
        }, async=True)

    def _process(self, msg):
        data = self._resolve_msg(msg)
        header, body = any_result_receiver.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(header, body)

    def _resolve_msg(self, msg):
        return msg[0]

    def dispatch_msg(self, data, async_=True):

        def _dispatch_msg_sync(msg):
            asyncio.wait([self._dealer.send_multipart(msg)])

        def _dispatch_msg_async(msg):
            asyncio.ensure_future(self._dealer.send_multipart(msg))

        msg = [data]
        if async_:
            _dispatch_msg_async(msg)
        else:
            _dispatch_msg_sync(msg)


class ResultReceiverCommunicationIO(metaclass=SingletonMeta):
    def __init__(self, context = None):
        self._context = context or zmq.Context()
        self._sock = None

    def connect(self, result_receiver_address):
        assert self._sock is None
        self._result_receiver_address = result_receiver_address
        self._sock = self._context.socket(zmq.REQ)
        self._sock.connect(result_receiver_address.to_zeromq_addr())

    def send_msg(self, msg_header, msg_body):
        assert self._sock is not None
        data = any_result_receiver.make_msg_data(msg_header, msg_body)
        self._sock.send(data)

    def recv_msg(self):
        assert self._sock is not None
        header, body = any_result_receiver.parse_msg_data(self._sock.recv())
        return header, body

    def close(self):
        assert self._sock is not None
        self._sock.close()
        self._sock = None


async def run_worker(context : Context, slave_addr, serialized_data : bytes):

    header, body = any_result_receiver.parse_msg_data(serialized_data)
    assert header == 'task_register'
    task_information = TaskInformation.from_dict(body)

    slave_conn = SlaveConnection(context, slave_addr, SlaveMessageHandler())
    result_receiver_communication_io = ResultReceiverCommunicationIO()

    ResultReceiverCommunicatorWithWorker(
        partial(result_receiver_communication_io.connect, result_receiver_communication_io),
        partial(result_receiver_communication_io.send_msg, result_receiver_communication_io),
        partial(result_receiver_communication_io.recv_msg, result_receiver_communication_io),
        partial(result_receiver_communication_io.close, result_receiver_communication_io)
    )

    await asyncio.wait([
        asyncio.ensure_future(slave_conn.run(task_information)),
        asyncio.ensure_future(do_task(context, task_information))
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