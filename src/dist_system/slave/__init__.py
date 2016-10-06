import os
import sys
import asyncio

from zmq.asyncio import Context, ZMQEventLoop

from dist_system.slave.network import MasterConnection, WorkerRouter, ResultReceiverCommunicationIO
from dist_system.library import coroutine_with_no_exception, default_coroutine_exception_callback
from dist_system.logger import Logger
from dist_system.slave.controller import monitor_information, run_polling_workers, WorkerCreator
from dist_system.slave.file import FileManager
from dist_system.slave.msg_dispatcher import MasterMessageDispatcher, WorkerMessageDispatcher
from dist_system.slave.msg_handler import MasterMessageHandler, WorkerMessageHandler
from dist_system.slave.result_receiver import ResultReceiverCommunicatorWithSlave
from dist_system.slave.task import TaskManager
from dist_system.slave.worker import WorkerManager
from dist_system.cloud_dfs import CloudDFSConnector


async def run_slave(context: Context, master_addr, worker_router_addr, slave_address, worker_file_name, cloud_dfs_addr):
    Logger("Slave", level=3)
    TaskManager()
    WorkerManager()
    FileManager(os.path.dirname(os.sys.modules[__name__].__file__) + '/files')
    CloudDFSConnector(cloud_dfs_addr.ip, cloud_dfs_addr.port)

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

    WorkerCreator(worker_file_name, slave_address, cloud_dfs_addr)

    await asyncio.wait([
        asyncio.ensure_future(worker_router.run()),  # must be first.
        asyncio.ensure_future(master_conn.run()),
        asyncio.ensure_future(run_polling_workers()),
        asyncio.ensure_future(coroutine_with_no_exception(monitor_information(), default_coroutine_exception_callback))
    ])


def main(master_addr, worker_router_addr, slave_address, worker_file_name, cloud_dfs_addr):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_slave(context, master_addr, worker_router_addr, slave_address,
                                          worker_file_name, cloud_dfs_addr))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)
