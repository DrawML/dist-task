from dist_system.result_receiver import ResultReceiverCommunicator
from dist_system.library import SingletonMeta


class ResultReceiverCommunicatorWithWorker(ResultReceiverCommunicator, metaclass=SingletonMeta):
    pass
