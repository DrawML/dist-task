from ..library import (AutoIncrementEnum, SingletonMeta)
from ..task.task import *

""" Not used now..
class TaskStatus(AutoIncrementEnum):
    STATUS_REGISTERED = ()
    STATUS_PROCESSING = ()
    STATUS_COMPLETE = ()
"""

class TaskManager(CommonTaskManager, metaclass=SingletonMeta):

    def __init__(self):
        super().__init__()
