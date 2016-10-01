from dist_system.library import (AutoIncrementEnum, SingletonMeta)
from dist_system.task import CommonTaskManager

class TaskStatus(AutoIncrementEnum):
    STATUS_REGISTERED = ()
    STATUS_PREPROCESSING = ()
    STATUS_PROCESSING = ()

class TaskManager(CommonTaskManager, metaclass=SingletonMeta):
    def __init__(self):
        self._registered_tasks = []
        self._preprocessing_tasks = []
        self._processing_tasks = []
        self._dic_status_queue = {
            TaskStatus.STATUS_REGISTERED: self._registered_tasks,
            TaskStatus.STATUS_PREPROCESSING: self._preprocessing_tasks,
            TaskStatus.STATUS_PROCESSING: self._processing_tasks,
        }
        super().__init__(self._dic_status_queue, TaskStatus.STATUS_PREPROCESSING)
