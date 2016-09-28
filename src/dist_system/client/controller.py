from dist_system.client import main
from dist_system.task.functions import make_task_with_task_type
from dist_system.task.task import *
from dist_system.client.task import TaskManager
from dist_system.logger import Logger


async def register_task_to_master(context, master_addr, result_receiver_address, task_type, job, callback):
    Logger().log("* register_task_to_master")
    conn = main.MasterConnection(context, master_addr)

    conn.connect()
    await conn.dispatch_msg_coro('task_register_req', {
        'result_receiver_address': result_receiver_address.to_dict(),
        'task_type': task_type.to_str(),
        'task': job.to_dict()
    })

    header, body = await conn.recv_msg()
    if header != 'task_register_res':
        raise Exception('Task Register Fail')
    status = body['status']
    task_token = TaskToken.from_bytes(body['task_token'])

    if status == 'success':
        await conn.dispatch_msg_coro('task_register_ack', {})
        task = make_task_with_task_type(task_type, job.to_dict(), 'master', task_token, result_receiver_address)
        task.callback = callback
        TaskManager().add_task(task)
    elif status == 'fail':
        error_code = body['error_code']
        raise Exception('Task Register Fail')
    else:
        # invalid message
        raise Exception('Task Register Fail')

    conn.close()


async def cancel_task_to_master(context, master_addr, task):
    conn = main.MasterConnection(context, master_addr)
    conn.connect()
    await conn.dispatch_msg_coro('task_cancel_req', {
        'task_token': task.task_token.to_bytes()
    })
    TaskManager().del_task(task)
    header, body = await conn.recv_msg()
    # nothing to do using response message.
    conn.close()
