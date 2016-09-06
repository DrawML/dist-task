from .task import *


class SleepTaskJob(TaskJob):
    def __init__(self, seconds : int):
        super().__init__()
        self._seconds = seconds

    def _to_dict(self) -> dict:
        return {'seconds' : self._seconds }

    @staticmethod
    def _from_dict(dict_ : dict) -> 'SleepTaskJob':
        return SleepTaskJob(dict_['seconds'])

    @property
    def seconds(self):
        return self._seconds


class SleepTaskResult(TaskResult):
    def __init__(self, comment : str):
        super().__init__()
        self._comment = comment

    def _to_dict(self) -> dict:
        return {'comment' : self._comment }

    @staticmethod
    def _from_dict(dict_ : dict) -> 'SleepTaskResult':
        return SleepTaskResult(dict_['comment'])

    @property
    def comment(self):
        return self._comment

    def __str__(self):
        return "Sleep Task Comment = {0}".format(self._comment)


class SleepTask(Task):
    def __init__(self,  task_token : TaskToken, result_receiver_address : ResultReceiverAddress,
                 job : SleepTaskJob):
        assert isinstance(job, SleepTaskJob)
        super().__init__(task_token, result_receiver_address, job)

    @Task.result.setter
    def result(self, result: SleepTaskResult):
        assert isinstance(result, SleepTaskResult)
        self._result = result
