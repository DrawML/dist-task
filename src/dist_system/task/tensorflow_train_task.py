from dist_system.result_receiver import ResultReceiverAddress
from dist_system.task import TaskJob, TaskResult, TaskValueError, Task, TaskToken


class TensorflowTrainTaskJob(TaskJob):
    @staticmethod
    def from_dict_with_whose_job(whose_job, dict_: dict):
        if whose_job == 'master':
            return TensorflowTrainTaskMasterJob.from_dict(dict_)
        elif whose_job == 'slave':
            return TensorflowTrainTaskSlaveJob.from_dict(dict_)
        elif whose_job == 'worker':
            return TensorflowTrainTaskWorkerJob.from_dict(dict_)
        else:
            raise TaskValueError('Invalid whose_job.')


class TensorflowTrainTaskMasterJob(TensorflowTrainTaskJob):
    def __init__(self, data_file_token: str, object_code: str):
        super().__init__()
        self._data_file_token = data_file_token
        self._object_code = object_code

    def _to_dict(self) -> dict:
        return {'data_file_token': self._data_file_token, 'object_code': self._object_code}

    @classmethod
    def _from_dict(cls, dict_: dict) -> 'TensorflowTrainTaskMasterJob':
        return TensorflowTrainTaskMasterJob(dict_['data_file_token'], dict_['object_code'])

    @property
    def data_file_token(self):
        return self._data_file_token

    @property
    def object_code(self):
        return self._object_code


class TensorflowTrainTaskSlaveJob(TensorflowTrainTaskJob):
    def __init__(self, data_file_token: str, executable_code: str):
        super().__init__()
        self._data_file_token = data_file_token
        self._executable_code = executable_code

    def _to_dict(self) -> dict:
        return {'data_file_token': self._data_file_token, 'executable_code': self._executable_code}

    @classmethod
    def _from_dict(cls, dict_: dict) -> 'TensorflowTrainTaskSlaveJob':
        return TensorflowTrainTaskSlaveJob(dict_['data_file_token'], dict_['executable_code'])

    @property
    def data_file_token(self):
        return self._data_file_token

    @property
    def executable_code(self):
        return self._executable_code


class TensorflowTrainTaskWorkerJob(TensorflowTrainTaskJob):
    def __init__(self, data_filename: str, executable_code_filename: str, session_filename: str, result_filename: str):
        super().__init__()
        self._data_filename = data_filename
        self._executable_code_filename = executable_code_filename
        self._session_filename = session_filename
        self._result_filename = result_filename

    def _to_dict(self) -> dict:
        return {'data_filename': self._data_filename,
                'executable_code_filename': self._executable_code_filename,
                'session_filename': self._session_filename,
                'result_filename': self._result_filename}

    @classmethod
    def _from_dict(cls, dict_: dict) -> 'TensorflowTrainTaskWorkerJob':
        return TensorflowTrainTaskWorkerJob(dict_['data_filename'], dict_['executable_code_filename'],
                                            dict_['session_filename'], dict_['result_filename'])

    @property
    def data_filename(self):
        return self._data_filename

    @property
    def executable_code_filename(self):
        return self._executable_code_filename

    @property
    def session_filename(self):
        return self._session_filename

    @property
    def result_filename(self):
        return self._result_filename


class TensorflowTrainTaskResult(TaskResult):
    def __init__(self, stdout: str, stderr: str, session_file_token: str, result_file_token: str = ''):
        super().__init__()
        self._stdout = stdout
        self._stderr = stderr
        self._session_file_token = session_file_token
        self._result_file_token = result_file_token

    def _to_dict(self) -> dict:
        return {'stdout': self._stdout, 'stderr': self._stderr,
                'session_file_token': self._session_file_token,
                'result_file_token': self._result_file_token}

    @classmethod
    def _from_dict(cls, dict_: dict) -> 'TensorflowTrainTaskResult':
        return TensorflowTrainTaskResult(dict_.get('stdout', ''), dict_.get('stderr', ''),
                                         dict_['session_file_token'],
                                    dict_.get('result_file_token', ''))

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    @property
    def session_file_token(self):
        return self._session_file_token

    @property
    def result_file_token(self):
        return self._result_file_token


class TensorflowTrainTask(Task):
    def __init__(self, task_token: TaskToken, result_receiver_address: ResultReceiverAddress,
                 job: TensorflowTrainTaskJob):
        assert isinstance(job, TensorflowTrainTaskJob)
        super().__init__(task_token, result_receiver_address, job)

    @Task.result.setter
    def result(self, result: TensorflowTrainTaskResult):
        assert isinstance(result, TensorflowTrainTaskResult)
        self._result = result
