#!/usr/bin/python3
import sys

from dist_system.worker import main

if __name__ == '__main__':
    serialized_data = bytes.fromhex(sys.argv[1])
    try:
        main(serialized_data=serialized_data)
    finally:
        print("@@@@@@@@@@@ A Worker is terminated!!!")
