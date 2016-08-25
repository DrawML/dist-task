import os
from dist_system.slave.main import main

SRC_DIR = os.path.dirname(os.sys.modules[__name__].__file__)

if __name__ == '__main__':
    print(os.pardir())
    main('tcp://127.0.0.1:7000', 'tcp://*:8000', SRC_DIR + '/run_worker.py')