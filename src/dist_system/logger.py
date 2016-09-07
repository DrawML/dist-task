from .library import SingletonMeta

class Logger(metaclass=SingletonMeta):

    def __init__(self, name):
        self.name = name

    def log(self, msg):
        print("[{0}] {1}".format(self.name, msg))