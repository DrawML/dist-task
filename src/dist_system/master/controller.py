import asyncio
from .slave import SlaveManager
from .task import *
from ..library import SingletonMeta


async def run_heartbeat():
    # send "Heart Beat Req" using protocol.

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
            except Exception as err:
                print("[!]", err)
                continue

            slave.assign_task(task)
            task_manager.change_task_status(task, TaskStatus.STATUS_PROCESSING)
