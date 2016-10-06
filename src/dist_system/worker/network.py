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


class ResultReceiverCommunicationIO(metaclass=SingletonMeta):
    def __init__(self, context=None):
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
