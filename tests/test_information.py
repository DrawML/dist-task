import unittest
from src.dist_system.information.information import *


class MyTestCase(unittest.TestCase):
    def test_AllocationTensorflowGpuInformation(self):
        tf_gpu_info = TensorflowGpuInformation('0000:01:00.0', 'Test Gpu 1', '/gpu:0', 5, 3, 10000000000, 7000000000)
        alloc_tf_gpu_info = AllocationTensorflowGpuInformation(True, tf_gpu_info)

        self.assertEqual(alloc_tf_gpu_info.available, True)
        self.assertEqual(alloc_tf_gpu_info.name, 'Test Gpu 1')
        self.assertEqual(alloc_tf_gpu_info.compute_capability_minor, 3)
        try:
            tf_gpu_info.available
        except Exception:
            pass
        else:
            self.assertEqual(True, False)

        alloc_tf_gpu_info.available = False
        alloc_tf_gpu_info.mem_free = 123456

        self.assertEqual(alloc_tf_gpu_info.available, False)
        self.assertEqual(alloc_tf_gpu_info.mem_free, 123456)
        self.assertEqual(alloc_tf_gpu_info.mem_total, 10000000000)


if __name__ == '__main__':
    unittest.main()
