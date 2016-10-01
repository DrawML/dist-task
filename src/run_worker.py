#!/usr/bin/python3
import sys

from dist_system.worker.main import main

if __name__ == '__main__':
    serialized_data = bytes.fromhex(sys.argv[1])
    main('tcp://127.0.0.1:18000', serialized_data)
