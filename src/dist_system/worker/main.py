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
from ..protocol import any_result_receiver, slave_worker
from .result_receiver import *
from functools import partial
from ..logger import Logger
import traceback


class SlaveConnection(object):

    def __init__(self, context, slave_addr, msg_handler):
        self._context = context
        self._slave_addr = slave_addr
        self._msg_handler = msg_handler

    async def run(self, task_information : TaskInformation):
        self._dealer = self._context.socket(zmq.DEALER)
        self._dealer.connect(self._slave_addr)
        Logger().log("slave connection to {0}".format(self._slave_addr))
        await self._register(task_information.task_token)

        while True:
            msg = await self._dealer.recv_multipart()
            self._process(msg)

    async def _register(self, task_token):
        await self.dispatch_msg_coro('worker_register_req', {
            'task_token': task_token.to_bytes()
        })

    def _process(self, msg):
        data = self._resolve_msg(msg)
        header, body = slave_worker.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(header, body)

    def _resolve_msg(self, msg):
        return msg[0]

    async def dispatch_msg_coro(self, header, body):
        Logger().log("to slave, header={0}, body={1}".format(header, body))
        try:
            data = slave_worker.make_msg_data(header, body)
        except Exception as e:
            print(traceback.format_exc())
            raise
        msg = [data]
        await self._dealer.send_multipart(msg)

    def dispatch_msg(self, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)


class ResultReceiverCommunicationIO(metaclass=SingletonMeta):
    def __init__(self, context = None):
        self._context = context or zmq.Context()
        self._sock = None

    def connect(self, result_receiver_address):
        assert self._sock is None
        self._result_receiver_address = result_receiver_address
        self._sock = self._context.socket(zmq.REQ)
        self._sock.connect(result_receiver_address.to_zeromq_addr())
        Logger().log("Connect to", result_receiver_address.to_zeromq_addr())

    def send_msg(self, msg_header, msg_body):
        assert self._sock is not None
        data = any_result_receiver.make_msg_data(msg_header, msg_body)
        Logger().log("To result receiver, header={0}, body={1}".format(msg_header, msg_body))
        self._sock.send(data)

    def recv_msg(self):
        assert self._sock is not None
        header, body = any_result_receiver.parse_msg_data(self._sock.recv())
        Logger().log("From result receiver, header={0}, body={1}".format(header, body))
        return header, body

    def close(self):
        assert self._sock is not None
        self._sock.close()
        self._sock = None
        Logger().log('Close a connection to result receiver')


async def run_worker(context : Context, slave_addr, serialized_data : bytes):

    import random
    import time
    Logger("Worker@" + str(time.time()) + "#" + str(random.randint(1, 10000000)))

    header, body = slave_worker.parse_msg_data(serialized_data)
    assert header == 'task_register_cmd'
    task_information = TaskInformation.from_dict(body)

    slave_conn = SlaveConnection(context, slave_addr, SlaveMessageHandler())
    SlaveMessageDispatcher(slave_conn.dispatch_msg)
    result_receiver_communication_io = ResultReceiverCommunicationIO()

    ResultReceiverCommunicatorWithWorker(
        result_receiver_communication_io.connect,
        result_receiver_communication_io.send_msg,
        result_receiver_communication_io.recv_msg,
        result_receiver_communication_io.close,
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