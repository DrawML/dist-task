import asyncio
from .slave import *
from .task import *
from ..library import SingletonMeta
from ..task.functions import get_task_type_of_task
from .msg_dispatcher import *
from typing import Iterable
import random


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
                slave = self._schedule(slave_manager.slaves, task)
            except NotAvailableSlaveError:
                continue

            task_manager.change_task_status(task, TaskStatus.STATUS_PROCESSING)
            slave.assign_task(task)

            SlaveMessageDispatcher().dispatch_msg(slave, 'task_register_req', {
                'result_receiver_address' : task.result_receiver_address.to_dict(),
                'task_token' : task.task_token.to_bytes(),
                'task' : task.job.to_dict()
            })

    def _schedule(self, slaves : Iterable, task) -> Slave:
        __schedule_dict = {
            TaskType.TYPE_SLEEP_TASK : self._schedule_sleep_task,
            TaskType.TYPE_DATA_PROCESSING_TASK : self._schedule_data_processing_task,
            TaskType.TYPE_TENSORFLOW_TASK : self._schedule_tensorflow_task
        }
        return __schedule_dict[get_task_type_of_task(task)](slaves, task)

    def _schedule_sleep_task(self, slaves : Iterable, task) -> Slave:
        return random.choice(slaves)

    def _schedule_data_processing_task(self, slaves : Iterable, task) -> Slave:
        best_slave = None
        best_cpu_rest = None
        for slave in slaves:
            info = slave.slave_information
            if info is None:
                continue
            cpu_rest = 0
            for cpu_percent in info.cpu_info.cpu_percents:
                cpu_rest += 100 - cpu_percent
            if best_slave is None or best_cpu_rest < cpu_rest:
                best_slave = slave
                best_cpu_rest = cpu_rest

        if best_slave is None:
            raise NotAvailableSlaveError
        return best_slave

    def _schedule_tensorflow_task(self, slaves : Iterable, task) -> Slave:
        # temporary... must be modified.
        return random.choice(slaves)