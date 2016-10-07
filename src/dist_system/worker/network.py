#!/usr/bin/python3
# -*- coding: utf-8 -*-
# !/usr/bin/env python

import asyncio
import traceback

import zmq

from dist_system.library import SingletonMeta
from dist_system.logger import Logger
from dist_system.protocol import any_result_receiver, slave_worker
from dist_system.worker.controller import TaskInformation


class SlaveConnection(object):
    def __init__(self, context, slave_addr, msg_handler):
        self._context = context
        self._slave_addr = slave_addr
        self._msg_handler = msg_handler

    async def run(self, task_information: TaskInformation):
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
        Logger().log("to slave, header={0}, body={1}".format(header, body), level=2)
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
