#!/usr/bin/python3
import subprocess
import os
import sys
import xml.etree.ElementTree as ET
import copy
from dist_system.information import TensorflowGpuInformation


SRC_DIR = os.path.dirname(os.sys.modules[__name__].__file__)


def _get_tf_gpu_list():
    proc = subprocess.Popen(['bash', SRC_DIR + '/get_tensorflow_gpus.sh'],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    raw_info = out.decode()
    raw_info = raw_info.strip().split('\n')
    info_list = [(x[1:len(x) - 1], y[1:len(y) - 1]) for x, y in zip(raw_info[::2], raw_info[1::2])]

    tf_gpu_list = []
    for info in info_list:
        tf_device, gpu_info = info
        a = gpu_info.split(',')
        b = [l.split(': ') for l in a]
        tf_gpu = {
            key.strip() : value.strip()
            for key, value in b
        }
        tf_gpu['tf_device'] = tf_device
        tf_gpu_list.append(tf_gpu)

    return tf_gpu_list


def _get_cuda_device_info_list():
    import pycuda.autoinit
    import pycuda.driver as cuda

    device_info_list = []

    for device_num in range(cuda.Device.count()):
        device = cuda.Device(device_num)
        device.make_context()

        device_info = {}
        device_info['name'] = device.name()
        device_info['pci_bus_id'] = device.pci_bus_id()
        device_info['compute_capability_major'] = device.compute_capability()[0]
        device_info['compute_capability_minor'] = device.compute_capability()[1]
        device_info['memory_total'] = cuda.mem_get_info()[1]
        device_info['memory_free'] = cuda.mem_get_info()[0]

        device_info_list.append(device_info)
        cuda.Context.pop()

    return device_info_list


def _get_tf_gpu_info_list(cuda_device_info_list, tf_gpu_list):
    tf_gpu_info_list = []
    for cuda_device_info in cuda_device_info_list:
        try:
            pci_bus_id = cuda_device_info['pci_bus_id']
            tf_device = None

            for tf_gpu in tf_gpu_list:
                if pci_bus_id == tf_gpu['pci bus id']:
                    tf_device = tf_gpu['tf_device']
                    break

            if tf_device is not None:
                tf_gpu_info_list.append(
                    TensorflowGpuInformation(pci_bus_id, cuda_device_info['name'], tf_device,
                                             int(cuda_device_info['compute_capability_major']),
                                             int(cuda_device_info['compute_capability_minor']),
                                             int(cuda_device_info['memory_total']),
                                             int(cuda_device_info['memory_free']))
                )
        except:
            pass

    return tf_gpu_info_list


def monitor_tf_gpu():
    tf_gpu_list = _get_tf_gpu_list()

    cuda_device_info_list = _get_cuda_device_info_list()
    tf_gpu_info_list = _get_tf_gpu_info_list(cuda_device_info_list, tf_gpu_list)

    return tf_gpu_info_list
