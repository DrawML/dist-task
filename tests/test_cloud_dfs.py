import unittest


class MyTestCase(unittest.TestCase):
    def test_import(self):
        from cloud_dfs.connector import CloudDFSConnector
        CloudDFSConnector('127.0.0.1', 5000)


if __name__ == '__main__':
    unittest.main()
