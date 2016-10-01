from queue import Queue

from dist_system.client.main import main
from dist_system.result_receiver import ResultReceiverAddress

if __name__ == '__main__':
    msg_queue = Queue()
    main('tcp://127.0.0.1:16000', 'tcp://*:25000', ResultReceiverAddress('tcp', '127.0.0.1', 25000), msg_queue)
