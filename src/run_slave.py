import os
from dist_system.slave.main import main

SRC_DIR = os.path.dirname(os.path.realpath(__file__))

if __name__ == '__main__':
    main('tcp://210.118.74.56:17000', 'tcp://*:18000', SRC_DIR + '/run_worker.py')
