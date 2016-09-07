from .controller import *
import asyncio
from ..task.sleep_task import *
from .task import TaskManager
from ..library import coroutine_with_no_exception


def _coroutine_exception_callback(_, e):
        print('[!] exception occurs in coroutine :', e)


async def simulate_task(context, master_addr, result_receiver_address):
    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(7)), _coroutine_exception_callback))
    await asyncio.sleep(1)

    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(18)), _coroutine_exception_callback))
    await asyncio.sleep(1)

    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(4)), _coroutine_exception_callback))
    await asyncio.sleep(1)

    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(12)), _coroutine_exception_callback))
    await asyncio.sleep(1)

    all_tasks = TaskManager().all_tasks
    if len(all_tasks) > 0:
        task = all_tasks[0]
        asyncio.ensure_future(coroutine_with_no_exception(
            cancel_task_to_master(context, master_addr, task), _coroutine_exception_callback))