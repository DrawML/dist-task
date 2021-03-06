import asyncio

from dist_system.client.controller import register_task_to_master, cancel_task_to_master
from dist_system.client.task import TaskManager
from dist_system.library import coroutine_with_no_exception, default_coroutine_exception_callback
from dist_system.task import TaskType
from dist_system.task.sleep_task import SleepTaskJob


async def simulate_task(context, master_addr, result_receiver_address):
    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(7)), default_coroutine_exception_callback))
    await asyncio.sleep(1)

    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(18)), default_coroutine_exception_callback))
    await asyncio.sleep(1)

    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(4)), default_coroutine_exception_callback))
    await asyncio.sleep(1)

    asyncio.ensure_future(coroutine_with_no_exception(
        register_task_to_master(context, master_addr, result_receiver_address,
                                TaskType.TYPE_SLEEP_TASK, SleepTaskJob(12)), default_coroutine_exception_callback))
    await asyncio.sleep(1)

    all_tasks = TaskManager().all_tasks
    if len(all_tasks) > 0:
        task = all_tasks[0]
        asyncio.ensure_future(coroutine_with_no_exception(
            cancel_task_to_master(context, master_addr, task), default_coroutine_exception_callback))
