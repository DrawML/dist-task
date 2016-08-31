class TensorflowGpuInformation(object):

    def __init__(self, pci_bus_id, name, tf_device, mem_total, mem_free):
        self.pci_bus_id = pci_bus_id
        self.name = name
        self.tf_device = tf_device
        self.mem_total = mem_total
        self.mem_free = mem_free

    def __repr__(self):
        return "TensorflowGpuInformation(pci_bus_id={0}, name={1}, tf_device={2}, mem_total={3}, mem_free={4})".format(
            self.pci_bus_id, self.name, self.tf_device, self.mem_total, self.mem_free
        )


class CpuInformation(object):

    def __init__(self, cpu_count : int, cpu_percents : list):
        self.cpu_count = cpu_count
        self.cpu_percents = cpu_percents

    def __repr__(self):
        return "CpuInformation(cpu_count={0}, cpu_percents={1})".format(
            self.cpu_count, self.cpu_percents
        )


class MemoryInformation(object):

    def __init__(self, mem_total, mem_free):
        self.mem_total = mem_total
        self.mem_free = mem_free

    def __repr__(self):
        return "MemoryInformation(mem_total={0}, mem_free={1})".format(
            self.mem_total, self.mem_free
        )


class SlaveInformation(object):

    def __init__(self, cpu_info, mem_info, tf_gpu_info_list):
        self.cpu_info = cpu_info
        self.mem_info = mem_info
        self.tf_gpu_info_list = tf_gpu_info_list

    def __repr__(self):
        return "SlaveInformation(cpu_info=<{0}>, mem_info=<{1}>, tf_gpu_info_list=<{2}>)".format(
            self.cpu_info, self.mem_info, self.tf_gpu_info_list
        )