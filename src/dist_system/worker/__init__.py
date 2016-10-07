import asyncio
import sys

from zmq.asyncio import Context, ZMQEventLoop

from dist_system.logger import Logger
from dist_system.worker.network import SlaveConnection
from dist_system.protocol import slave_worker
from dist_system.worker.controller import TaskInformation, do_task
from dist_system.worker.msg_dispatcher import SlaveMessageDispatcher
from dist_system.worker.msg_handler import SlaveMessageHandler
from dist_system.result_receiver_network import ResultReceiverCommunicator
from dist_system.cloud_dfs import CloudDFSConnector


async def run_worker(context: Context, serialized_data: bytes):
    import random
    import time
    Logger("Worker@" + str(time.time()) + "#" + str(random.randint(1, 10000000)), level=2)

    Logger().log("Hello World!")

    header, body = slave_worker.parse_msg_data(serialized_data)
    assert header == 'task_register_cmd'
    task_information = TaskInformation.from_dict(body)

    slave_conn = SlaveConnection(context, task_information.slave_address.to_zeromq_addr(), SlaveMessageHandler())
    SlaveMessageDispatcher(slave_conn.dispatch_msg)
    CloudDFSConnector(task_information.cloud_dfs_address.ip, task_information.cloud_dfs_address.port)

    ResultReceiverCommunicator()

    await asyncio.wait([
        asyncio.ensure_future(slave_conn.run(task_information)),
        asyncio.ensure_future(do_task(context, task_information))
    ])


def main(serialized_data: bytes):
    try:
        loop = ZMQEventLoop()
        asyncio.set_event_loop(loop)

        context = Context()

        loop.run_until_complete(run_worker(context, serialized_data))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)
