from cloud_dfs.connector import CloudDFSConnector as BaseCloudDFSConnector
from cloud_dfs.connector import Error, NotFoundError, ParamError, UnknownError, UnprocessableError
from dist_system.library import SingletonMeta


class CloudDFSAddress(object):
    def __init__(self, ip: str, port: int):
        self._ip = ip
        self._port = port

    @property
    def ip(self):
        return self._ip

    @property
    def port(self):
        return self._port


class CloudDFSConnector(BaseCloudDFSConnector, metaclass=SingletonMeta):
    pass
