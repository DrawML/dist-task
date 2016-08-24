from ..library import (AutoIncrementEnum, SingletonMeta)
from ..task.task import *


class TaskManager(CommonTaskManager, metaclass=SingletonMeta):

    def __init__(self):
        super().__init__()
