from .task import *


class DataProcessingTaskJob(TaskJob):
    pass


class DataProcessingTaskMasterJob(TaskJob):
    def __init__(self, data_file_token : str, object_code : str):
        super().__init__()
        self._data_file_token = data_file_token
        self._object_code = object_code

    def _to_dict(self) -> dict:
        return {'data_file_token': self._data_file_token, 'object_code': self._object_code }

    @staticmethod
    def _from_dict(dict_ : dict) -> 'DataProcessingTaskMasterJob':
        return DataProcessingTaskMasterJob(dict_['data_file_token'], dict_['object_code'])

    @property
    def data_file_token(self):
        return self._data_file_token

    @property
    def object_code(self):
        return self._object_code


class DataProcessingTaskSlaveJob(DataProcessingTaskJob):
    def __init__(self, data_file_token : str, executable_code : str):
        super().__init__()
        self._data_file_token = data_file_token
        self._executable_code = executable_code

    def _to_dict(self) -> dict:
        return {'data_file_token': self._data_file_token, 'executable_code': self._executable_code }

    @staticmethod
    def _from_dict(dict_ : dict) -> 'DataProcessingTaskSlaveJob':
        return DataProcessingTaskSlaveJob(dict_['data_file_token'], dict_['executable_code'])

    @property
    def data_file_token(self):
        return self._data_file_token

    @property
    def executable_code(self):
        return self._executable_code


class DataProcessingTaskWorkerJob(DataProcessingTaskJob):
    def __init__(self, data_filename : str, executable_code_filename : str):
        super().__init__()
        self._data_filename = data_filename
        self._executable_code_filename = executable_code_filename

    def _to_dict(self) -> dict:
        return {'data_filename': self._data_filename,
                'executable_code_filename': self._executable_code_filename }

    @staticmethod
    def _from_dict(dict_ : dict) -> 'DataProcessingTaskWorkerJob':
        return DataProcessingTaskWorkerJob(dict_['data_filename'], dict_['executable_code_filename'])

    @property
    def data_filename(self):
        return self._data_filename

    @property
    def executable_code_filename(self):
        return self._executable_code_filename


class DataProcessingTaskResult(TaskResult):
    def __init__(self, stdout : str, stderr : str, result_file_token : str):
        super().__init__()
        self._stdout = stdout
        self._stderr = stderr
        self._result_file_token = result_file_token

    def _to_dict(self) -> dict:
        return {'stdout' : self._stdout, 'stderr' : self._stderr,
                'result_file_token' : self._result_file_token }

    @staticmethod
    def _from_dict(dict_ : dict) -> 'DataProcessingTaskResult':
        return DataProcessingTaskResult(dict_['stdout'], dict_['stderr'], dict_['result_file_token'])

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    @property
    def result_file_token(self):
        return self._result_file_token


class DataProcessingTask(Task):
    def __init__(self,  task_token : TaskToken, result_receiver_address : ResultReceiverAddress,
                 job : DataProcessingTaskJob):
        assert isinstance(job, DataProcessingTaskJob)
        super().__init__(task_token, result_receiver_address, job)

    @Task.result.setter
    def result(self, result: DataProcessingTaskResult):
        assert isinstance(result, DataProcessingTaskResult)
        self._result = result
