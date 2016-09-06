"""
    [config - default value]
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
                 tf_device="/cpu:0",
                 cpu_count=0,
                 gpu_memory_fraction=1.0,
                 log_placement=False):
        self._tf_device = tf_device
        self._cpu_count = cpu_count
        self._gpu_memory_fraction = gpu_memory_fraction

        self._log_placement = log_placement
        self._allow_soft_placement = False

    def __iter__(self):
        for_iter = self.dictify()
        for key, value in for_iter.items():
            yield (key, value)

    def dictify(self):
        config = dict()
        config['device'] = self._tf_device
        config['log_placement'] = self._log_placement

        if "gpu" in self._tf_device:
            config['allow_soft_placement'] = self._allow_soft_placement
            config['gpu_memory_fraction'] = self._gpu_memory_fraction
        else:
            config['inter_threads_count'] = self._cpu_count
            config['intra_threads_count'] = self._cpu_count

        return config

    @property
    def tf_device(self):
        return self._tf_device

    @tf_device.setter
    def tf_device(self, val):
        self._tf_device = val
        if "gpu" in val:
            self._allow_soft_placement = True
        else:
            self._allow_soft_placement = False

    @property
    def cpu_count(self):
        return self._cpu_count

    @cpu_count.setter
    def cpu_count(self, val):
        self._cpu_count = val

    @property
    def gpu_memory_fraction(self):
        return self._gpu_memory_fraction

    @gpu_memory_fraction.setter
    def gpu_memory_fraction(self, val):
        self._gpu_memory_fraction = val

    @property
    def log_placement(self):
        return self._log_placement

    @log_placement.setter
    def log_placement(self, val):
        self._log_placement = val

