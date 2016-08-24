from enum import Enum
import asyncio


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


def apply_metaclass_from_class(class_, metaclass_):
    return metaclass_(class_.__name__, class_.__bases__, class_.__dict__)


def make_singleton_class_from(class_):
    return apply_metaclass_from_class(class_, SingletonMeta)


async def coroutine_with_no_exception(coro, f_callback = None, *args, **kwargs):
    try:
        await coro
    except BaseException as e:
        if f_callback is not None :
            f_callback(coro, e, *args, **kwargs)