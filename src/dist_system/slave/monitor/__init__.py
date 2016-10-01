import asyncio

import psutil

from dist_system.information import SlaveInformation, CpuInformation, MemoryInformation
from dist_system.slave.monitor.gpu_monitor import monitor_tf_gpu


async def cpu_percent_async(interval=None, *args, **kwargs):
    if interval is not None and interval > 0.0:
        psutil.cpu_percent(*args, **kwargs)
        await asyncio.sleep(interval)
    return psutil.cpu_percent(*args, **kwargs)


async def monitor():
    svmem = psutil.virtual_memory()
    return SlaveInformation(
        CpuInformation(
            psutil.cpu_count(),
            await cpu_percent_async(percpu=True)
        ),
        MemoryInformation(
            svmem.total,
            svmem.available
        ),
        monitor_tf_gpu()
    )
