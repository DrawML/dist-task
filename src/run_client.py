from queue import Queue

from dist_system.client import RequestMessage, Client
from dist_system.client.main import main
from dist_system.result_receiver import ResultReceiverAddress

if __name__ == '__main__':
    msg_queue = Queue()
    msg = RequestMessage('test', Client.TaskType.TYPE_SLEEP_TASK, {"seconds": 10}, None)
    msg_queue.put(msg)
    main(master_addr='tcp://127.0.0.1:16000',
         result_router_addr='tcp://*:25000',
         result_receiver_address=ResultReceiverAddress('tcp', '127.0.0.1', 25000),
         msg_queue=msg_queue)
