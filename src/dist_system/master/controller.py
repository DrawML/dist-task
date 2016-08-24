import asyncio
from .slave import *
from .task import *
from ..library import SingletonMeta
from ..task.sleep_task import *
from .msg_dispatcher import *


async def run_heartbeat():
    # send "Heart Beat Req" using protocol.

    while True:
        for slave in SlaveManager().slaves:
            SlaveMessageDispatcher().dispatch_msg(slave, 'heart_beat_req', {})

        await asyncio.sleep(SlaveManager.HEARTBEAT_INTERVAL)

        expired_slaves, leak_tasks = SlaveManager().purge()
        TaskManager().redo_leak_task(leak_tasks)


class Scheduler(metaclass=SingletonMeta):

    def invoke(self):
        # 현재 waiting하고 있는 task가 있는 지 보고 available한 slave 있는지 판단하여 task를 slave에 배치한다.
        self._assign_waiting_task_to_slave()

    def _assign_waiting_task_to_slave(self):
        slave_manager = SlaveManager()
        task_manager = TaskManager()

        for task in task_manager.waiting_tasks:
            client = task.client

            try:
                slave = slave_manager.get_proper_slave(task)
            except NotAvailableSlaveError:
                continue

            task_manager.change_task_status(task, TaskStatus.STATUS_PROCESSING)
            slave.assign_task(task)

            SlaveMessageDispatcher().dispatch_msg(slave, 'task_register_req', {
                'result_receiver_address' : task.result_receiver_address.to_dict(),
                'task_token' : task.task_token.to_bytes(),
                'task' : task.job.to_dict()
            })