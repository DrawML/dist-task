from ..library import (AutoIncrementEnum, SingletonMeta)
import os


class FileType(AutoIncrementEnum):
    TYPE_DATA_FILE = ()
    TYPE_EXECUTABLE_CODE_FILE = ()


class FileValueError(ValueError):
    def __init__(self, msg=''):
        self._msg = msg

    def __str__(self):
        return "TaskValueError : %s" % self._msg


class FileManager(metaclass=SingletonMeta):

    __FILE_NO_CREATION_NUMBER = 10
    __FILENAME_PREFIX = {
        FileType.TYPE_DATA_FILE: "data_",
        FileType.TYPE_EXECUTABLE_CODE_FILE: "executable_code_"
    }
    __FILENAME_POSTFIX = {
        FileType.TYPE_DATA_FILE: ".txt",
        FileType.TYPE_EXECUTABLE_CODE_FILE: ".py"
    }

    def __init__(self, root_dir : str):
        if not os.path.isabs(root_dir):
            raise FileValueError('root_dir must be given by absolute directory.')
        root_dir = os.path.dirname(root_dir)
        self._ensure_dir(root_dir)
        self._root_dir = root_dir
        self._file_no_pool = {
            FileType.TYPE_DATA_FILE : [],
            FileType.TYPE_EXECUTABLE_CODE_FILE : []
        }
        self._file_no_end = {
            FileType.TYPE_DATA_FILE : 0,
            FileType.TYPE_EXECUTABLE_CODE_FILE : 0
        }
        self._dic_key_files = {
            # [(file_type, file_path)]
        }

    def _ensure_dir(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir)

    def _get_avail_file_no(self, file_type : FileType):
        pool = self._file_no_pool[file_type]
        if len(pool) < FileManager.__FILE_NO_CREATION_NUMBER:
            file_no_start = self._file_no_end[file_type]
            file_no_end = file_no_start + FileManager.__FILE_NO_CREATION_NUMBER
            for no in range(file_no_start, file_no_end):
                pool.append(no)
            self._file_no_end[file_type] = file_no_end

        return pool.pop()

    def _get_avail_filename(self, file_type : FileType):
        return FileManager.__FILENAME_PREFIX[file_type] + \
            self._get_avail_file_no(file_type) + \
            FileManager.__FILENAME_POSTFIX[file_type]

    # exception handling will be added.
    def store(self, key, file_type : FileType, file_data : str):
        file_path = self._root_dir + '/' + self._get_avail_filename(file_type)
        with open(file_path, 'w') as f:
            f.write(file_data)
        if not key in self._dic_key_files:
            self._dic_key_files[key] = []
        self._dic_key_files[key].append(file_type, file_path)
        return file_path

    def remove_files_using_key(self, key):
        try:
            files = self._dic_key_files[key]
        except KeyError:
            pass
        else:
            del self._dic_key_files[key]
            for file_type, file_path in files:
                self.remove(file_type, file_path)

    def remove(self, file_type : FileType, file_path : str):
        try:
            filename = os.path.basename(file_path)
            file_no = int(
                filename[len(FileManager.__FILENAME_PREFIX[file_type]) : \
                -len(FileManager.__FILENAME_POSTFIX[file_type])]
            )
        except Exception as e:
            raise FileValueError('invalid file_path. ' + str(e))