from dist_system.library import SingletonMeta


class MasterMessageDispatcher(metaclass=SingletonMeta):
    def __init__(self, f_dispatch_msg):
        self._f_dispatch_msg = f_dispatch_msg

    def dispatch_msg(self, *args, **kwargs):
        self._f_dispatch_msg(*args, **kwargs)


class WorkerMessageDispatcher(metaclass=SingletonMeta):
    def __init__(self, f_dispatch_msg):
        self._f_dispatch_msg = f_dispatch_msg

    def dispatch_msg(self, *args, **kwargs):
        self._f_dispatch_msg(*args, **kwargs)
