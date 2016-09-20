#!/usr/bin/python3
# -*- coding: utf-8 -*-
# !/usr/bin/env python

import sys
import zmq
from zmq.asyncio import Context, ZMQEventLoop
import asyncio
from .msg_handler import ResultMessageHandler
from .controller import *
from ..protocol import client_master, any_result_receiver
from functools import partial
from ..library import SingletonMeta, coroutine_with_no_exception
from ..task.task import *
from ..task.functions import *
from .task import TaskManager
from .simulator import simulate_task, _coroutine_exception_callback
from ..logger import Logger
from queue import Queue

import traceback


class MasterConnection(object):
    def __init__(self, context: zmq.asyncio.Context, master_addr):
        self._context = context
        self._master_addr = master_addr

    def connect(self):
        self._sock = self._context.socket(zmq.REQ)
        self._sock.connect(self._master_addr)
        Logger().log("master connection to {0}".format(self._master_addr))

    def close(self):
        self._sock.close()

    async def recv_msg(self):
        msg = await self._sock.recv_multipart()
        Logger().log("master connection recv a message : {0}".format(msg))
        data = self._resolve_msg(msg)
        try:
            header, body = client_master.parse_msg_data(data)
        except BaseException as e:
            print(e)
            raise
        return header, body

    def _resolve_msg(self, msg):
        return msg[0]

    async def dispatch_msg_coro(self, header, body):
        Logger().log("to master, header={0}, body={1}".format(header, body))
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
        Logger().log("result router bind to {0}".format(self._addr))

        while True:
            msg = await self._router.recv_multipart()
            self._process(msg)

    def _process(self, msg):
        addr, data = self._resolve_msg(msg)
        try:
            header, body = any_result_receiver.parse_msg_data(data)
        except Exception as e:
            print(traceback.format_exc())

        # handle message
        self._msg_handler.handle_msg(addr, header, body)

    def _resolve_msg(self, msg):
        addr = msg[0]
        data = msg[2]

        return addr, data

    async def dispatch_msg_coro(self, addr, header, body):
        data = any_result_receiver.make_msg_data(header, body)
        msg = [addr, b'', data]
        await self._router.send_multipart(msg)

    def dispatch_msg(self, addr, header, body, f_callback=None, f_args=None):
        future = asyncio.ensure_future(self.dispatch_msg_coro(addr, header, body))
        if f_callback is not None:
            import functools
            future.add_done_callback(functools.partial(f_callback, **f_args))


class TaskDeliverer(metaclass=SingletonMeta):
    def __init__(self, context: Context, master_addr, result_receiver_address, msg_queue: Queue):
        self._context = context
        self._master_addr = master_addr
        self._result_receiver_address = result_receiver_address

        self._msg_queue = msg_queue

    async def run(self):
        while True:
            task_type, task_job_dict, callback = await self._msg_queue.get()
            await self._process(task_type, task_job_dict, callback)

    async def _process(self, task_type: TaskType, task_job_dict: dict, callback):
        if task_type == TaskType.TYPE_TENSORFLOW_TASK:
            await tensorflow_task(self._context, self._master_addr, self._result_receiver_address,
                                  task_job_dict, callback)
        else:
            pass


async def tensorflow_task(context: Context, master_addr, result_receiver_address, task_job_dict: dict, callback):
    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address, TaskType.TYPE_TENSORFLOW_TASK,
                                TensorflowTaskJob.from_dict_with_whose_job('master', task_job_dict), callback),
        _coroutine_exception_callback)
    )


async def run_client(context: Context, master_addr, result_router_addr, result_receiver_address, msg_queue):
    Logger('Client')
    result_router = ResultReceiverCommunicationRouter(context, result_router_addr, ResultMessageHandler())
    deliverer = TaskDeliverer(context, master_addr, result_receiver_address, msg_queue)

    _ = asyncio.ensure_future(simulate_task(context, master_addr, result_receiver_address))

    await asyncio.wait([
        asyncio.ensure_future(result_router.run()),
        asyncio.ensure_future(deliverer.run())
    ])


def main(master_addr, result_router_addr, result_receiver_address, msg_queue):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(
            run_client(context, master_addr, result_router_addr, result_receiver_address, msg_queue))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)
