from dist_system.library import (AutoIncrementEnum, SingletonMeta)
from dist_system.task.task import *


class TaskManager(CommonTaskManager, metaclass=SingletonMeta):

    def __init__(self):
        super().__init__()
