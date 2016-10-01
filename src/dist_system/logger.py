from dist_system.library import SingletonMeta


class Logger(metaclass=SingletonMeta):
    def __init__(self, name, level=3):
        self.name = name
        self.level = level

    def log(self, msg, *args, level=3):
        if level >= self.level:
            print("[{0}] {1}".format(self.name, msg), *args)
