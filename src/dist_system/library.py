import traceback
from enum import Enum


class AutoIncrementEnum(Enum):
    def __new__(cls):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj


class SingletonMeta(type):
    __instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instances:
            cls.__instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls.__instances[cls]


async def coroutine_with_no_exception(coro, f_callback=None, *args, **kwargs):
    try:
        await coro
    except BaseException as e:
        if f_callback is not None:
            f_callback(coro, e, *args, **kwargs)


def default_coroutine_exception_callback(_, e):
    from dist_system.logger import Logger
    Logger().log('[!] exception occurs in coroutine :', e)
    Logger().log(traceback.format_exc())
