import os

from dist_system.library import AutoIncrementEnum, SingletonMeta


class FileType(AutoIncrementEnum):
    TYPE_DATA_FILE = ()
    TYPE_EXECUTABLE_CODE_FILE = ()
    TYPE_RESULT_FILE = ()
    TYPE_SESSION_FILE = ()


class FileDataType(AutoIncrementEnum):
    TYPE_TEXT = ()
    TYPE_BINARY = ()


class FileValueError(ValueError):
    def __init__(self, msg=''):
        self._msg = msg

    def __str__(self):
        return "FileValueError : %s" % self._msg


class FileManager(metaclass=SingletonMeta):
    __FILE_NO_CREATION_NUMBER = 10
    __FILENAME_PREFIX = {
        FileType.TYPE_DATA_FILE: "data_",
        FileType.TYPE_EXECUTABLE_CODE_FILE: "executable_code_",
        FileType.TYPE_RESULT_FILE: "result_file_",
        FileType.TYPE_SESSION_FILE: "session_file_"
    }
    __FILENAME_POSTFIX = {
        FileType.TYPE_DATA_FILE: ".txt",
        FileType.TYPE_EXECUTABLE_CODE_FILE: ".py",
        FileType.TYPE_RESULT_FILE: ".txt",
        FileType.TYPE_SESSION_FILE: ".ckpt"
    }

    def __init__(self, root_dir: str):
        if not os.path.isabs(root_dir):
            raise FileValueError('root_dir must be given by absolute directory.')
        self._ensure_dir(root_dir)
        self._root_dir = root_dir
        self._file_no_pool = {
            FileType.TYPE_DATA_FILE: [],
            FileType.TYPE_EXECUTABLE_CODE_FILE: [],
            FileType.TYPE_RESULT_FILE: [],
            FileType.TYPE_SESSION_FILE: []
        }
        self._file_no_end = {
            FileType.TYPE_DATA_FILE: 0,
            FileType.TYPE_EXECUTABLE_CODE_FILE: 0,
            FileType.TYPE_RESULT_FILE: 0,
            FileType.TYPE_SESSION_FILE: 0
        }
        self._dic_key_files = {
            # key: (file_type, file_no, file_path)
        }

    def _ensure_dir(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir)

    def _get_avail_file_no(self, file_type: FileType):
        pool = self._file_no_pool[file_type]
        if len(pool) < FileManager.__FILE_NO_CREATION_NUMBER:
            file_no_start = self._file_no_end[file_type]
            file_no_end = file_no_start + FileManager.__FILE_NO_CREATION_NUMBER
            for no in range(file_no_start, file_no_end):
                pool.append(no)
            self._file_no_end[file_type] = file_no_end

        return pool.pop()

    def _salt_to_filename(self, file_type: FileType, identity : str):
        return FileManager.__FILENAME_PREFIX[file_type] + \
               identity + \
               FileManager.__FILENAME_POSTFIX[file_type]

    def _reserve(self, key, file_type: FileType):
        file_no = self._get_avail_file_no(file_type)
        file_path = os.path.join(self._root_dir, self._salt_to_filename(file_type, str(file_no)))
        if not key in self._dic_key_files:
            self._dic_key_files[key] = []
        self._dic_key_files[key].append((file_type, file_no, file_path))
        return file_path, file_no

    def store(self, key, file_type: FileType, file_data, data_type: FileDataType = None):
        """Store data file to local file system.

        :param key: associated key to file :hashable object
        :param file_type: file type
        :param file_data: file data
        :param data_type: data type
        :return: a file path which a file is stored at
        """
        if data_type is None:
            if isinstance(file_data, str):
                data_type = FileDataType.TYPE_TEXT
            elif isinstance(file_data, bytes):
                data_type = FileDataType.TYPE_BINARY
            else:
                raise FileValueError('invalid file data.')

        if data_type == FileDataType.TYPE_TEXT:
            mode = 'wt'
        elif data_type == FileDataType.TYPE_BINARY:
            mode = 'wb'
        else:
            raise FileValueError('invalid data type.')

        file_path, file_no = self._reserve(key, file_type)
        try:
            with open(file_path, mode) as f:
                f.write(file_data)
        except OSError:
            self._remove(file_type, file_no, file_path)
            raise
        return file_path

    def reserve(self, key, file_type: FileType):
        file_path, _ = self._reserve(key, file_type)
        return file_path

    def remove_files_using_key(self, key):
        try:
            files = self._dic_key_files[key]
        except KeyError:
            pass
        else:
            del self._dic_key_files[key]
            for file_type, file_no, file_path in files:
                self._remove(file_type, file_no, file_path)

    def _remove(self, file_type: FileType, file_no: int, file_path: str):
        try:
            self._file_no_pool[file_type].append(file_no)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                # for reserve files.
                pass
        except Exception as e:
            raise FileValueError('invalid file_path. ' + str(e))
