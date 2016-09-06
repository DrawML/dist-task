from .task import *
from .sleep_task import *
from .data_processing_task import *
from .tensorflow_task import *


def make_task_with_task_type(task_type: TaskType, job_dict: dict, whose_job, *args, **kwargs):
    if task_type == TaskType.TYPE_SLEEP_TASK:
        return SleepTask(*args, **kwargs, job=SleepTaskJob.from_dict(job_dict))
    elif task_type == TaskType.TYPE_DATA_PROCESSING_TASK:
        return DataProcessingTask(*args, **kwargs, job=DataProcessingTaskJob.from_dict_with_whose_job(whose_job, job_dict))
    elif task_type == TaskType.TYPE_TENSORFLOW_TASK:
        return TensorflowTask(*args, **kwargs, job=TensorflowTaskJob.from_dict_with_whose_job(whose_job, job_dict))
    else:
        raise TaskTypeValueError("Invalid Task Type.")


def set_result_dict_to_task(task: Task, result_dict: dict):
    if isinstance(task, SleepTask):
        result = SleepTaskResult.from_dict(result_dict)
    elif isinstance(task, DataProcessingTask):
        result = DataProcessingTaskResult.from_dict(result_dict)
    elif isinstance(task, TensorflowTask):
        result = TensorflowTaskResult.from_dict(result_dict)
    else:
        raise TaskValueError("It is not task.")

    task.result = result


def get_task_type_of_task(task: Task) -> TaskType:
    if isinstance(task, SleepTask):
        return TaskType.TYPE_SLEEP_TASK
    elif isinstance(task, DataProcessingTask):
        return TaskType.TYPE_DATA_PROCESSING_TASK
    elif isinstance(task, TensorflowTask):
        return TaskType.TYPE_TENSORFLOW_TASK
    else:
        raise TaskValueError("Invalid Task.")
