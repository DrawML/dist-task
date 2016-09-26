from .task import *


class DataProcessingTaskJob(TaskJob):

    @staticmethod
    def from_dict_with_whose_job(whose_job, dict_ : dict):
        if whose_job == 'master':
            return DataProcessingTaskMasterJob.from_dict(dict_)
        elif whose_job == 'slave':
            return DataProcessingTaskSlaveJob.from_dict(dict_)
        elif whose_job == 'worker':
            return DataProcessingTaskWorkerJob.from_dict(dict_)
        else:
            raise TaskValueError('Invalid whose_job.')


class DataProcessingTaskMasterJob(TaskJob):
    def __init__(self, data_file_num : int, data_file_token_list : list, object_code : str):
        super().__init__()
        self._data_file_num = data_file_num
        self._data_file_token_list = data_file_token_list
        self._object_code = object_code

    def _to_dict(self) -> dict:
        return {'data_file_num': self._data_file_num,
                'data_file_token_list': self._data_file_token_list,
                'object_code': self._object_code }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'DataProcessingTaskMasterJob':
        return DataProcessingTaskMasterJob(dict_['data_file_num'],
                                           dict_['data_file_token_list'],
                                           dict_['object_code'])

    @property
    def data_file_num(self):
        return self._data_file_num

    @property
    def data_file_tokens(self):
        return tuple(self._data_file_token_list)

    @property
    def object_code(self):
        return self._object_code


class DataProcessingTaskSlaveJob(DataProcessingTaskJob):
    def __init__(self, data_file_num : int, data_file_token_list : list, executable_code : str):
        super().__init__()
        self._data_file_num = data_file_num
        self._data_file_token_list = data_file_token_list
        self._executable_code = executable_code

    def _to_dict(self) -> dict:
        return {'data_file_num': self._data_file_num,
                'data_file_token_list': self._data_file_token_list,
                'executable_code': self._executable_code }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'DataProcessingTaskSlaveJob':
        return DataProcessingTaskSlaveJob(dict_['data_file_num'],
                                          dict_['data_file_token_list'],
                                          dict_['executable_code'])

    @classmethod
    def from_master_job(cls, job : DataProcessingTaskMasterJob, executable_code):
        return DataProcessingTaskSlaveJob(job.data_file_num, list(job.data_file_tokens), executable_code)

    @property
    def data_file_num(self):
        return self._data_file_num

    @property
    def data_file_tokens(self):
        return tuple(self._data_file_token_list)

    @property
    def executable_code(self):
        return self._executable_code


class DataProcessingTaskWorkerJob(DataProcessingTaskJob):
    def __init__(self, data_file_num : int, data_filename_list : list, executable_code_filename : str):
        super().__init__()
        self._data_file_num = data_file_num
        self._data_filename_list = data_filename_list
        self._executable_code_filename = executable_code_filename

    def _to_dict(self) -> dict:
        return {'data_file_num': self._data_file_num,
                'data_filename_list': self._data_filename_list,
                'executable_code_filename': self._executable_code_filename }

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'DataProcessingTaskWorkerJob':
        return DataProcessingTaskWorkerJob(dict_['data_file_num'],
                                           dict_['data_filename_list'],
                                           dict_['executable_code_filename'])

    @property
    def data_file_num(self):
        return self._data_file_num

    @property
    def data_filenames(self):
        return tuple(self._data_filename_list)

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

    @classmethod
    def _from_dict(cls, dict_ : dict) -> 'DataProcessingTaskResult':
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
