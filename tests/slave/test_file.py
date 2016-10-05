import os
import unittest

from dist_system.slave.file import FileManager, FileType, FileValueError

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
print("SRC_DIR:", SRC_DIR)


class MyTestCase(unittest.TestCase):
    def test_file_manager(self):
        try:
            file_manager = FileManager('not_absolute_dir')
        except FileValueError:
            pass
        else:
            self.assertEqual(True, False)

        TEST_FILES_DIR = SRC_DIR + '/test_files'

        file_manager = FileManager(TEST_FILES_DIR)
        self.assertTrue(os.path.exists(TEST_FILES_DIR))

        keys = [
            'key0', 'key0', 'key1', 'key0', 'key2', 'key2'
        ]
        types = [
            FileType.TYPE_DATA_FILE, FileType.TYPE_EXECUTABLE_CODE_FILE,
            FileType.TYPE_EXECUTABLE_CODE_FILE, FileType.TYPE_DATA_FILE,
            FileType.TYPE_RESULT_FILE, FileType.TYPE_SESSION_FILE
        ]
        data_list = [
            b'data0',
            'data1\nseveral lines\nhahaha',
            b'data2\nseveral lines, too\nha\tha\tha\n',
            None,
            'data4',
            b'data5'
        ]
        file_paths = []

        def check_file_data(file_path: str, org_data):
            if isinstance(org_data, str):
                mode = 'rt'
            elif isinstance(org_data, bytes):
                mode = 'rb'
            else:
                raise ValueError('invalid org_data.')

            self.assertTrue(os.path.isfile(file_path))
            with open(file_path, mode) as f:
                file_data = f.read()
            self.assertEqual(file_data, org_data)

        def print_log_after_remove():
            print("After Remove, file no pool(DATA) :", file_manager._file_no_pool[FileType.TYPE_DATA_FILE])
            print("After Remove, file no pool(EXE) :", file_manager._file_no_pool[FileType.TYPE_EXECUTABLE_CODE_FILE])
            print("After Remove, file no pool(RESULT) :", file_manager._file_no_pool[FileType.TYPE_RESULT_FILE])
            print("After Remove, file no pool(SESSION) :", file_manager._file_no_pool[FileType.TYPE_SESSION_FILE])

        file_path = file_manager.store(keys[0], types[0], data_list[0])
        print("Created File Path :", file_path)
        file_paths.append(file_path)
        check_file_data(file_path, data_list[0])

        file_path = file_manager.store(keys[1], types[1], data_list[1])
        print("Created File Path :", file_path)
        file_paths.append(file_path)
        check_file_data(file_path, data_list[1])

        file_path = file_manager.store(keys[2], types[2], data_list[2])
        print("Created File Path :", file_path)
        file_paths.append(file_path)
        check_file_data(file_path, data_list[2])

        file_manager.remove_files_using_key(keys[0])
        self.assertFalse(os.path.isfile(file_paths[0]))
        self.assertFalse(os.path.isfile(file_paths[1]))
        print_log_after_remove()

        try:
            file_manager._dic_key_files[keys[0]]
        except KeyError:
            pass
        else:
            self.assertEqual(True, False)

        file_path = file_manager.reserve(keys[3], types[3])
        print("Reserved File Path :", file_path)
        file_paths.append(file_path)
        self.assertFalse(os.path.isfile(file_paths[3]))

        file_manager.remove_files_using_key(keys[3])
        file_manager.remove_files_using_key(keys[2])
        self.assertFalse(os.path.isfile(file_paths[2]))
        self.assertFalse(os.path.isfile(file_paths[3]))
        print_log_after_remove()

        file_path = file_manager.store(keys[4], types[4], data_list[4])
        print("Created File Path :", file_path)
        file_paths.append(file_path)
        check_file_data(file_path, data_list[4])

        file_path = file_manager.store(keys[5], types[5], data_list[5])
        print("Created File Path :", file_path)
        file_paths.append(file_path)
        check_file_data(file_path, data_list[5])

        file_manager.remove_files_using_key(keys[4])
        self.assertFalse(os.path.isfile(file_paths[4]))
        self.assertFalse(os.path.isfile(file_paths[5]))
        print_log_after_remove()


if __name__ == '__main__':
    unittest.main()
