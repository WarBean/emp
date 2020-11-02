from .task import *
from .pymp_mapper import *
from .pymp_iterator import *
from .ray_backend import *
from .ray_mapper import *
from .ray_iterator import *
# from .ray_dataloader import *

from ._version import get_versions
__version__ = get_versions()['version']
version = __version__
del get_versions


def init_backend(backend, *args, **kwargs):
    if backend == 'pymp':
        pass
    elif backend == 'ray':
        init_ray_backend(*args, **kwargs)
    else:
        raise ValueError('[backend] should be pymp or ray')


def shutdown_backend(backend, *args, **kwargs):
    if backend == 'pymp':
        pass
    elif backend == 'ray':
        shutdown_ray_backend(*args, **kwargs)
    else:
        raise ValueError('[backend] should be pymp or ray')


def mapper(backend, num_proc=None, ordered=True, chunk_size=1, report_interval=0, report_newline=False, report_name=None):
    if backend == 'pymp':
        return pymp_mapper(num_proc, ordered, chunk_size, report_interval, report_newline, report_name)
    if backend == 'ray':
        return ray_mapper(num_proc, ordered, chunk_size, report_interval, report_newline, report_name)
    raise ValueError('unknown backend [{backend}], only support [pymp] or [ray]')


def iterator(backend, prefetch_size=1, chunk_size=1):
    if prefetch_size < 1:
        raise ValueError(f'requires prefetch_size >= 1, but got {prefetch_size}')
    if chunk_size < 1:
        raise ValueError(f'requires chunk_size >= 1, but got {chunk_size}')
    if backend == 'pymp':
        return pymp_iterator(prefetch_size, chunk_size)
    if backend == 'ray':
        return ray_iterator(prefetch_size, chunk_size)
    raise ValueError('unknown backend [{backend}], only support [pymp] or [ray]')
