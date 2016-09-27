#!/usr/bin/python3
# -*- coding: utf-8 -*-
# !/usr/bin/env python
import queue
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
        Logger().log("master inits to connect to {0}".format(self._master_addr))

    def connect(self):
        Logger().log("master try to connect to {0}".format(self._master_addr))
        self._sock = self._context.socket(zmq.REQ)
        self._sock.connect(self._master_addr)
        Logger().log("master connection to {0}".format(self._master_addr))

    def close(self):
        self._sock.close()

    async def recv_msg(self):
        Logger().log("master try to recv to {0}".format(self._master_addr))
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
            callback = functools.partial(f_callback, **f_args)
            # this is not thread-safe (internally, call <abstract_loop.call_soon()>)
            # - future.add_done_callback(callback)
            #
            # you need to make adding callback thread-safe way
            asyncio.get_event_loop().call_soon_threadsafe(callback)


class TaskSyncManager(metaclass=SingletonMeta):
    def __index__(self):
        self._pending_queue = list()
        self._cancel_queue = list()

    def pend_experiment(self, exp_id):
        self._pending_queue.append(exp_id)

    def unpend_experiment(self, exp_id):
        if exp_id in self._pending_queue:
            self._pending_queue.remove(exp_id)
        else:
            Logger().log(" * TaskSyncManager::Unpending error - There is no experiment_id")

    def reserve_cancel(self, exp_id):
        self._cancel_queue.append(exp_id)

    def remove_from_cancel_queue(self, exp_id):
        if exp_id in self._cancel_queue:
            self._cancel_queue.remove(exp_id)
        else:
            Logger().log(" * TaskSyncManager::Canceling error - There is no experiment_id")

    def check_cancel_exp(self, exp_id):
        return exp_id in self._cancel_queue

    def check_pending_exp_id(self, exp_id):
        return exp_id in self._pending_queue


class TaskDeliverer(metaclass=SingletonMeta):
    def __init__(self, context: Context, master_addr, result_receiver_address, msg_queue: Queue):
        print('[TaskDeliverer] ', 'init! ')
        self._context = context
        self._master_addr = master_addr
        self._result_receiver_address = result_receiver_address

        self._msg_queue = msg_queue

    async def run(self):

        async def get_msg(sleep_sec=1):
            while True:
                try:
                    return self._msg_queue.get_nowait()
                except queue.Empty as e:
                    await asyncio.sleep(sleep_sec)

        while True:
            print('[TaskDeliverer] ', 'run! ')
            task_type, task_job_dict, callback = await get_msg(sleep_sec=1)
            print('[TaskDeliverer] ', 'get msg! ', task_job_dict)
            await self._process(task_type, task_job_dict, callback)

    async def _process(self, task_type: TaskType, task_job_dict: dict, callback):
        print('[TaskDeliverer] ', 'start to request! in process')
        if task_type == TaskType.TYPE_TENSORFLOW_TASK:
            print('[TaskDeliverer] ', 'request! in process')
            await register_tensorflow_task(self._context, self._master_addr, self._result_receiver_address,
                                           task_job_dict, callback)
        else:
            pass

async def register_tensorflow_task(context: Context, master_addr, result_receiver_address, task_job_dict: dict, callback):
    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address, TaskType.TYPE_TENSORFLOW_TASK,
                                TensorflowTaskJob.from_dict_with_whose_job('master', task_job_dict), callback),
        _coroutine_exception_callback)
    )


async def run_client(context: Context, master_addr, result_router_addr, result_receiver_address, msg_queue):
    Logger('Client')
    print('[ClientRunClient] ', 'in run!')
    TaskSyncManager()
    result_router = ResultReceiverCommunicationRouter(context, result_router_addr, ResultMessageHandler())
    deliverer = TaskDeliverer(context, master_addr, result_receiver_address, msg_queue)

    # _ = asyncio.ensure_future(simulate_task(context, master_addr, result_receiver_address))

    await asyncio.wait([
        asyncio.ensure_future(result_router.run()),
        asyncio.ensure_future(deliverer.run())
    ])


def main(master_addr, result_router_addr, result_receiver_address, msg_queue):
    try:
        print('[ClientMain] ', 'start!')

        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(
            run_client(context, master_addr, result_router_addr, result_receiver_address, msg_queue))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)
