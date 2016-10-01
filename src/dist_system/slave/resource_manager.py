import psutil


class ResourceManager:
    def __init__(self):
        self._cpu_count = psutil.cpu_count()
        self._cpu_available = self._cpu_count
        self._std_usage = 50
        self._processes = []

    def track(self):
        poping_candidate = []

        for idx, proc in enumerate(self._processes):
            status = proc.status()

            if status == "running":
                continue
            elif status == "terminated":
                poping_candidate.append(idx)
            else:
                raise NotImplementedError

        self._purge(poping_candidate)

    def _purge(self, candidates=[]):
        for idx in candidates:
            self._processes.pop(idx)

    async def monitor(self):
        await self._cpu_usage = psutil.cpu_percent(2, percpu=True)

    def get_cpu(self, count=1):
        if self._cpu_available < count:
            # make exception
            raise ResourceWarning

        self._cpu_available -= count

        return count

    def release_cpu(self, count):
        self._cpu_available += count

    def track_process(self, pid):
        self._processes.append(psutil.Process(pid))

    def _optimize(self):
        for proc in self._processes:
            usage = proc.cpu_percent()

            if usage > self._std_usage:
                self._rebalance_cpu(proc)

    def _rebalance_cpu(self, process):
        if self._cpu_available > 0:
            process.cpu_affinity()  # need to inspect
