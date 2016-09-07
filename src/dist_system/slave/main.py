#!/usr/bin/python3
#-*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import (MasterMessageHandler, WorkerMessageHandler)
from .controller import *
from ..protocol import master_slave, slave_worker, any_result_receiver
from functools import partial
from .msg_dispatcher import *
from .result_receiver import *
from .monitor.monitor import monitor
from ..logger import Logger
import traceback


class MasterConnection(metaclass=SingletonMeta):

    def __init__(self, context, master_addr, msg_handler):
        self._context = context
        self._master_addr = master_addr
        self._msg_handler = msg_handler

    async def run(self):
        self._dealer = self._context.socket(zmq.DEALER)
        self._dealer.connect(self._master_addr)
        Logger().log("master connection to {0}".format(self._master_addr))
        await self._register()

        while True:
            msg = await self._dealer.recv_multipart()
            self._process(msg)

    async def _register(self):
        await self.dispatch_msg_coro('slave_register_req', {})

    def _process(self, msg):
        data = self._resolve_msg(msg)
        header, body = master_slave.parse_msg_data(data)

        # handle message
        self._msg_handler.handle_msg(header, body)

    def _resolve_msg(self, msg):
        return msg[0]

    async def dispatch_msg_coro(self, header, body):
        Logger().log("to master, header={0}, body={1}".format(header, body))
        try:
            data = master_slave.make_msg_data(header, body)
        except Exception as e:
            print(traceback.format_exc())
            raise
        msg = [data]
        await self._dealer.send_multipart(msg)

    def dispatch_msg(self, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(header, body))
        if f_callback is not None:
            future.add_done_callback(f_callback)


class WorkerRouter(metaclass=SingletonMeta):

    def __init__(self, context, addr, msg_handler):
        self._context = context
        self._addr = addr
        self._msg_handler = msg_handler

    async def run(self):
        self._router = self._context.socket(zmq.ROUTER)
        self._router.bind(self._addr)
        Logger().log("worker router bind to {0}".format(self._addr))

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

    async def dispatch_msg_coro(self, worker_identity, header, body):
        Logger().log("to worker({0}), header={1}, body={2}".format(worker_identity, header, body))
        addr = worker_identity.addr
        try:
            data = slave_worker.make_msg_data(header, body)
        except Exception as e:
            print(traceback.format_exc())
            raise
        msg = [addr, data]
        await self._router.send_multipart(msg)

    def dispatch_msg(self, worker_identity, header, body, f_callback=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(worker_identity, header, body))
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


async def run_slave(context : Context, master_addr, worker_router_addr, worker_file_name):

    Logger("Slave")
    TaskManager()
    WorkerManager()

    master_conn = MasterConnection(context, master_addr, MasterMessageHandler())
    worker_router = WorkerRouter(context, worker_router_addr, WorkerMessageHandler())
    result_receiver_communication_io = ResultReceiverCommunicationIO()

    MasterMessageDispatcher(master_conn.dispatch_msg)
    WorkerMessageDispatcher(worker_router.dispatch_msg)
    ResultReceiverCommunicatorWithSlave(
        result_receiver_communication_io.connect,
        result_receiver_communication_io.send_msg,
        result_receiver_communication_io.recv_msg,
        result_receiver_communication_io.close,
    )

    WorkerCreator(worker_file_name)

    await asyncio.wait([
        asyncio.ensure_future(worker_router.run()),  # must be first.
        asyncio.ensure_future(master_conn.run()),
        asyncio.ensure_future(run_polling_workers()),
        asyncio.ensure_future(monitor_information())
    ])


def main(master_addr, worker_router_addr, worker_file_name):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_slave(context, master_addr, worker_router_addr, worker_file_name))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)