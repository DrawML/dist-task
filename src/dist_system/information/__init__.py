class TensorflowGpuInformation(object):
    def __init__(self, pci_bus_id, name, tf_device, compute_capability_major, compute_capability_minor, mem_total,
                 mem_free):
        self.pci_bus_id = pci_bus_id
        self.name = name
        self.tf_device = tf_device
        self.compute_capability_major = compute_capability_major
        self.compute_capability_minor = compute_capability_minor
        self.mem_total = mem_total
        self.mem_free = mem_free

    def __repr__(self):
        return "TensorflowGpuInformation(pci_bus_id={0}, name={1}, tf_device={2}, " \
               "compute_capability={3}, mem_total={4}, mem_free={5})".format(
            self.pci_bus_id, self.name, self.tf_device,
            (self.compute_capability_major, self.compute_capability_minor), self.mem_total, self.mem_free
        )

    def to_dict(self):
        return {
            'pci_bus_id': self.pci_bus_id,
            'name': self.name,
            'tf_device': self.tf_device,
            'compute_capability_major': self.compute_capability_major,
            'compute_capability_minor': self.compute_capability_minor,
            'mem_total': self.mem_total,
            'mem_free': self.mem_free
        }

    @staticmethod
    def from_dict(dict_: dict):
        return TensorflowGpuInformation(dict_['pci_bus_id'], dict_['name'], dict_['tf_device'],
                                        dict_['compute_capability_major'], dict_['compute_capability_minor'],
                                        dict_['mem_total'], dict_['mem_free'])


class CpuInformation(object):
    def __init__(self, cpu_count: int, cpu_percents: list):
        self.cpu_count = cpu_count
        self.cpu_percents = cpu_percents

    def __repr__(self):
        return "CpuInformation(cpu_count={0}, cpu_percents={1})".format(
            self.cpu_count, self.cpu_percents
        )

    def to_dict(self):
        return {
            'cpu_count': self.cpu_count,
            'cpu_percents': self.cpu_percents,
        }

    @staticmethod
    def from_dict(dict_: dict):
        return CpuInformation(dict_['cpu_count'], dict_['cpu_percents'])


class MemoryInformation(object):
    def __init__(self, mem_total, mem_free):
        self.mem_total = mem_total
        self.mem_free = mem_free

    def __repr__(self):
        return "MemoryInformation(mem_total={0}, mem_free={1})".format(
            self.mem_total, self.mem_free
        )

    def to_dict(self):
        return {
            'mem_total': self.mem_total,
            'mem_free': self.mem_free,
        }

    @staticmethod
    def from_dict(dict_: dict):
        return MemoryInformation(dict_['mem_total'], dict_['mem_free'])


class SlaveInformation(object):
    def __init__(self, cpu_info, mem_info, tf_gpu_info_list):
        self.cpu_info = cpu_info
        self.mem_info = mem_info
        self.tf_gpu_info_list = tf_gpu_info_list

    def __repr__(self):
        return "SlaveInformation(cpu_info=<{0}>, mem_info=<{1}>, tf_gpu_info_list=<{2}>)".format(
            self.cpu_info, self.mem_info, self.tf_gpu_info_list
        )

    def to_dict(self):
        d_tf_gpu_info_list = [x.to_dict() for x in self.tf_gpu_info_list]
        return {
            'cpu_info': self.cpu_info.to_dict(),
            'mem_info': self.mem_info.to_dict(),
            'tf_gpu_info_list': d_tf_gpu_info_list
        }

    @staticmethod
    def from_dict(dict_: dict):
        tf_gpu_info_list = [TensorflowGpuInformation.from_dict(x) for x in dict_.get('tf_gpu_info_list', [])]
        return SlaveInformation(CpuInformation.from_dict(dict_['cpu_info']),
                                MemoryInformation.from_dict(dict_['mem_info']),
                                tf_gpu_info_list)


class AllocationTensorflowGpuInformation(object):
    def __init__(self, available, tf_gpu_info):
        super(AllocationTensorflowGpuInformation, self).__setattr__('available', available)
        super(AllocationTensorflowGpuInformation, self).__setattr__('tf_gpu_info', tf_gpu_info)

    def __getattr__(self, item):
        return self.tf_gpu_info.__getattribute__(item)

    def __setattr__(self, key, value):
        if hasattr(self, key):
            super(AllocationTensorflowGpuInformation, self).__setattr__(key, value)
        else:
            self.tf_gpu_info.__setattr__(key, value)


class AllocationInformation(object):
    def __init__(self, alloc_cpu_count, all_cpu_count, alloc_tf_gpu_info_list):
        self.alloc_cpu_count = alloc_cpu_count
        self.all_cpu_count = all_cpu_count
        self._alloc_tf_gpu_info_list = alloc_tf_gpu_info_list

    @property
    def alloc_tf_gpu_info_list(self):
        return tuple(self._alloc_tf_gpu_info_list)

    @property
    def avail_cpu_count(self):
        return self.all_cpu_count - self.alloc_cpu_count

    @property
    def cpu_available(self):
        return self.avail_cpu_count > 0


class AllocatedResource(object):
    def __init__(self, alloc_cpu_count=0, alloc_tf_gpu_info=None):
        self.alloc_cpu_count = alloc_cpu_count
        self.alloc_tf_gpu_info = alloc_tf_gpu_info

    def __repr__(self):
        return "AllocatedResource(alloc_cpu_count = {0}, alloc_tf_gpu_info = {1})".format(
            self.alloc_cpu_count, self.alloc_tf_gpu_info
        )
