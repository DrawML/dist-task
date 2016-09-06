"""
    [config - default value]
    device_count=1
    log_placement=False
    inter_threads_count=0
    intra_threads_count=0
    allow_soft_placement=False
    gpu_memory_fraction=1
"""

""" Run Config Argument.
tf_device='/cpu:0' ('/gpu:0', ... can be placed in it)
cpu_count=0 (means use of all cpu cores) (valid if tf_device='/cpu:0')
gpu_memory_fraction = 1.0 (valid if tf_device='/gpu:*')
and more if you think..(for logging..)
"""

class RunConfig:
    def __init__(self,
                 device_count=1,
                 log_placement=False,
                 inter_threads_count=0,
                 intra_threads_count=0,
                 allow_soft_placement=False,
                 gpu_memory_fraction=1):
        self._device_count = device_count
        self._log_placement = log_placement
        self._inter_threads_count = inter_threads_count
        self._intra_threads_count = intra_threads_count
        self._allow_soft_placement = allow_soft_placement
        self._gpu_memory_fraction = gpu_memory_fraction

    def __iter__(self):
        for_iter = self.__dict__.items()
        for key, value in for_iter:
            yield (key, value)

    @property
    def device_count(self):
        return self.device_count

    @device_count.setter
    def device_count(self, val):
        self.device_count = val

    @property
    def log_placement(self):
        return self.log_placement

    @log_placement.setter
    def log_placement(self, val):
        self._log_placement = val

    @property
    def inter_threads_count(self):
        return self._inter_threads_count

    @inter_threads_count.setter
    def inter_threads_count(self, val):
        self._inter_threads_count = val

    @property
    def intra_threads_count(self):
        return self._intra_threads_count

    @intra_threads_count.setter
    def intra_threads_count(self, val):
        self._intra_threads_count = val

    @property
    def allow_soft_placement(self):
        return self._allow_soft_placement

    @allow_soft_placement.setter
    def allow_soft_placement(self, val):
        self._allow_soft_placement = val

    @property
    def gpu_memory_fraction(self):
        return self._gpu_memory_fraction

    @gpu_memory_fraction.setter
    def gpu_memory_fraction(self, val):
        self._gpu_memory_fraction = val
