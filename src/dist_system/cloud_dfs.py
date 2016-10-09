import asyncio

from cloud_dfs.connector import CloudDFSConnector as BaseCloudDFSConnector
from cloud_dfs.connector import Error, NotFoundError, ParamError, UnknownError, UnprocessableError
from dist_system.library import SingletonMeta


class CloudDFSAddress(object):
    def __init__(self, ip: str, port: int):
        self._ip = ip
        self._port = port

    def to_dict(self):
        return {
            'ip': self._ip,
            'port': self._port
        }

    @classmethod
    def from_dict(cls, dict_: dict):
        return cls(dict_['ip'], dict_['port'])

    @property
    def ip(self):
        return self._ip

    @property
    def port(self):
        return self._port


class CloudDFSConnector(BaseCloudDFSConnector, metaclass=SingletonMeta):

    async def put_data_file_async(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        f = loop.run_in_executor(None, lambda: self.put_data_file(*args, **kwargs))
        return await f

    async def get_data_file_async(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        f = loop.run_in_executor(None, lambda: self.get_data_file(*args, **kwargs))
        return await f
