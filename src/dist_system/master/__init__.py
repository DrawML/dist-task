import asyncio
import sys

from zmq.asyncio import Context, ZMQEventLoop

from dist_system.logger import Logger
from dist_system.master.network import ClientRouter, SlaveRouter
from dist_system.master.client import ClientSessionManager
from dist_system.master.controller import Scheduler
from dist_system.master.controller import run_heartbeat
from dist_system.master.msg_dispatcher import SlaveMessageDispatcher, ClientMessageDispatcher
from dist_system.master.msg_handler import (ClientMessageHandler, SlaveMessageHandler)
from dist_system.master.slave import SlaveManager
from dist_system.master.task import TaskManager
from dist_system.result_receiver_network import ResultReceiverCommunicator


async def run_master(context: Context, client_router_addr, slave_router_addr):
    Logger("Master", level=3)
    TaskManager()
    SlaveManager()
    ClientSessionManager()
    Scheduler()

    client_router = ClientRouter(context, client_router_addr, ClientMessageHandler())
    slave_router = SlaveRouter(context, slave_router_addr, SlaveMessageHandler())

    ClientMessageDispatcher(client_router.dispatch_msg)
    SlaveMessageDispatcher(slave_router.dispatch_msg)

    ResultReceiverCommunicator()

    await asyncio.wait([
        asyncio.ensure_future(client_router.run()),
        asyncio.ensure_future(slave_router.run()),
        asyncio.ensure_future(run_heartbeat())
    ])


def main(client_router_addr, slave_router_addr):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_master(context, client_router_addr, slave_router_addr))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)
