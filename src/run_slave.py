import os

from dist_system.slave.main import main
from dist_system.address import SlaveAddress
from dist_system.cloud_dfs import CloudDFSAddress

SRC_DIR = os.path.dirname(os.path.realpath(__file__))

if __name__ == '__main__':
    main(master_addr='tcp://210.118.74.56:17000',
         worker_router_addr='tcp://*:18000',
         slave_address=SlaveAddress('tcp', '127.0.0.1', 18000),
         worker_file_name=SRC_DIR + '/run_worker.py',
         cloud_dfs_addr=CloudDFSAddress('127.0.0.1', 9602))
