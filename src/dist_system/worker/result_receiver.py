from ..result_receiver import ResultReceiverCommunicator
from ..library import SingletonMeta


class ResultReceiverCommunicatorWithWorker(ResultReceiverCommunicator, metaclass=SingletonMeta):
    pass
