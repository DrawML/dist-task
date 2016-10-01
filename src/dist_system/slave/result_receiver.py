from dist_system.library import SingletonMeta
from dist_system.result_receiver import ResultReceiverCommunicator


class ResultReceiverCommunicatorWithSlave(ResultReceiverCommunicator, metaclass=SingletonMeta):
    pass
