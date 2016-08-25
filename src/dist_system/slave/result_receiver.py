from ..result_receiver import ResultReceiverCommunicator
from ..library import SingletonMeta


class ResultReceiverCommunicatorWithSlave(ResultReceiverCommunicator, metaclass=SingletonMeta):
    pass