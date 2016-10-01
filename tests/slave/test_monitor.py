import unittest
import dist_system.slave.monitor.monitor as monitor
import asyncio


class MyTestCase(unittest.TestCase):
    def test_monitor(self):
        slave_info = asyncio.get_event_loop().run_until_complete(monitor.monitor())
        print(slave_info)


if __name__ == '__main__':
    unittest.main()
