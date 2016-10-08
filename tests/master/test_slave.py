import unittest


class MyTestCase(unittest.TestCase):
    def test_print_slave(self):
        from dist_system.master.slave import Slave, SlaveIdentity
        from dist_system.logger import Logger
        Logger("Test")
        slaves = []
        for no in range(10):
            slaves.append(Slave(b"aa"))
        Logger().log("Message :", slaves)


if __name__ == '__main__':
    unittest.main()
