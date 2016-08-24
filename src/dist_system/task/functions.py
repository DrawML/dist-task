from .task import *
from .sleep_task import *


def make_task_with_task_type(task_type, *args, **kwargs):
    if task_type == TaskType.TYPE_SLEEP_TASK:
        return SleepTask(*args, **kwargs)
    else:
        raise TaskTypeValueError("Invalid Task Type.")


def set_result_dict_to_task(task, result_dict):
    if isinstance(task, SleepTask):
        result = SleepTaskResult.from_dict(result_dict)
    else:
        raise TaskValueError("It is not task.")

    task.result = result