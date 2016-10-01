from dist_system.library import SingletonMeta
from dist_system.result_receiver import ResultReceiverCommunicator


class ResultReceiverCommunicatorWithWorker(ResultReceiverCommunicator, metaclass=SingletonMeta):
    pass
