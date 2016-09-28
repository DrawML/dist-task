from dist_system.task.task import *


class TensorflowTaskJob(TaskJob):

    @staticmethod
    def from_dict_with_whose_job(whose_job, dict_ : dict):
        if whose_job == 'master':
            return TensorflowTaskMasterJob.from_dict(dict_)
        elif whose_job == 'slave':
            return TensorflowTaskSlaveJob.from_dict(dict_)
        elif whose_job == 'worker':
            return TensorflowTaskWorkerJob.from_dict(dict_)
        else:
            raise TaskValueError('Invalid whose_job.')


class TensorflowTaskMasterJob(TensorflowTaskJob):
    def __init__(self, data_file_token : str, object_code : str):
        super().__init__()
        self._data_file_token = data_file_token
        self._object_code = object_code

    def _to_dict(self) -> dict:
        return {'data_file_token': self._data_file_token, 'object_code': self._object_code }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'TensorflowTaskMasterJob':
        return TensorflowTaskMasterJob(dict_['data_file_token'], dict_['object_code'])

    @property
    def data_file_token(self):
        return self._data_file_token

    @property
    def object_code(self):
        return self._object_code


class TensorflowTaskSlaveJob(TensorflowTaskJob):
    def __init__(self, data_file_token : str, executable_code : str):
        super().__init__()
        self._data_file_token = data_file_token
        self._executable_code = executable_code

    def _to_dict(self) -> dict:
        return {'data_file_token': self._data_file_token, 'executable_code': self._executable_code }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'TensorflowTaskSlaveJob':
        return TensorflowTaskSlaveJob(dict_['data_file_token'], dict_['executable_code'])

    @property
    def data_file_token(self):
        return self._data_file_token

    @property
    def executable_code(self):
        return self._executable_code


class TensorflowTaskWorkerJob(TensorflowTaskJob):
    def __init__(self, data_filename : str, executable_code_filename : str):
        super().__init__()
        self._data_filename = data_filename
        self._executable_code_filename = executable_code_filename

    def _to_dict(self) -> dict:
        return {'data_filename': self._data_filename,
                'executable_code_filename': self._executable_code_filename }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'TensorflowTaskWorkerJob':
        return TensorflowTaskWorkerJob(dict_['data_filename'], dict_['executable_code_filename'])

    @property
    def data_filename(self):
        return self._data_filename

    @property
    def executable_code_filename(self):
        return self._executable_code_filename


class TensorflowTaskResult(TaskResult):
    def __init__(self, stdout : str, stderr : str, result_file_token : str = ''):
        super().__init__()
        self._stdout = stdout
        self._stderr = stderr
        self._result_file_token = result_file_token

    def _to_dict(self) -> dict:
        return {'stdout' : self._stdout, 'stderr' : self._stderr,
                'result_file_token' : self._result_file_token }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'TensorflowTaskResult':
        return TensorflowTaskResult(dict_.get('stdout', ''), dict_.get('stderr', ''), dict_.get('result_file_token', ''))

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    @property
    def result_file_token(self):
        return self._result_file_token


class TensorflowTask(Task):
    def __init__(self,  task_token : TaskToken, result_receiver_address : ResultReceiverAddress,
                 job : TensorflowTaskJob):
        assert isinstance(job, TensorflowTaskJob)
        super().__init__(task_token, result_receiver_address, job)

    @Task.result.setter
    def result(self, result: TensorflowTaskResult):
        assert isinstance(result, TensorflowTaskResult)
        self._result = result
