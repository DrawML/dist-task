class RequestMessage():
    def __init__(self, experiment_id, task_type, task_job_dict, callback):
        self._experiment_id = experiment_id
        self._task_type = task_type
        self._task_job_dict = task_job_dict
        self._callback = callback

    @property
    def experiment_id(self):
        return self._experiment_id

    @property
    def task_type(self):
        return self._task_type

    @property
    def task_job_dict(self):
        return self._task_job_dict

    @property
    def callback(self):
        return self._callback


class CancelMessage():
    def __init__(self, experiment_id):
        self._experiment_id = experiment_id

    @property
    def experiment_id(self):
        return self._experiment_id


