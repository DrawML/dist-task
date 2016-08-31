#!/usr/bin/python3
import subprocess
import os
import sys
import xml.etree.ElementTree as ET
import copy
from ...information.information import *

# its win32, maybe there is win64 too?
is_windows = sys.platform.startswith('win')

if is_windows:
    PYTHON2 = 'C:/Python27/python.exe'
else:
    PYTHON2 = 'python2'

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


def _exec_nvidia_smi():
    proc = subprocess.Popen([PYTHON2, SRC_DIR + '/nvidia_smi.py'],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    return out.decode(), err.decode()


def _parse_nvidia_smi_result(result : str):
    try:
        root = ET.fromstring(result)
        gpu_tags = root.findall('gpu')
    except:
        return []

    gpu_info_list = []
    for gpu_tag in gpu_tags:
        try:
            r = {}
            r['name'] = gpu_tag.find('product_name').text
            r['pci_bus_id'] = gpu_tag.find('pci').find('pci_bus_id').text
            r['memory_total'] = gpu_tag.find('memory_usage').find('total').text[:-2]
            r['memory_free'] = gpu_tag.find('memory_usage').find('free').text[:-2]
            gpu_info_list.append(r)
        except:
            pass

    return gpu_info_list


def _get_tf_gpu_info_list(gpu_info_list, tf_gpu_list):
    tf_gpu_info_list = []
    for gpu_info in gpu_info_list:
        try:
            pci_bus_id = gpu_info['pci_bus_id']
            tf_device = None

            for tf_gpu in tf_gpu_list:
                if pci_bus_id == tf_gpu['pci bus id']:
                    tf_device = tf_gpu['tf_device']
                    break

            if tf_device is not None:
                tf_gpu_info_list.append(
                    TensorflowGpuInformation(pci_bus_id, gpu_info['name'], tf_device,
                                             int(gpu_info['memory_total']),
                                             int(gpu_info['memory_free']))
                )
        except:
            pass

    return tf_gpu_info_list


def monitor_tf_gpu():
    tf_gpu_list = _get_tf_gpu_list()

    out, err = _exec_nvidia_smi()
    if err:
        return []

    gpu_info_list = _parse_nvidia_smi_result(out)
    tf_gpu_info_list = _get_tf_gpu_info_list(gpu_info_list, tf_gpu_list)

    return tf_gpu_info_list


