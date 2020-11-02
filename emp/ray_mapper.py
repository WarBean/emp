__all__ = ['ray_mapper']


import ray
import inspect
import collections
import multiprocessing as mp
from .task import map_on_tasks
from .ray_backend import init_ray_backend
from .utils import split_chunks, iterate_with_report


def pop_future_result(future_list, ordered=False, total_timeout=0.1):
    if len(future_list) == 0:
        raise RuntimeError('cannot pop from empty future_list')

    if ordered:
        value = ray.get(future_list[0])
        del future_list[0]
        return 0, value
    else:
        index = 0
        timeout = total_timeout / len(future_list)
        while True:
            try:
                value = ray.get(future_list[index], timeout=timeout)
                del future_list[index]
                break
            except ray.exceptions.GetTimeoutError:
                index = (index + 1) % len(future_list)
        return index, value


def build_mapper_function(function, num_proc, ordered, chunk_size,
                          report_interval, report_newline, report_name):
    @ray.remote
    def chunk_function(chunk_tasks):
        chunk_results = map_on_tasks(function, chunk_tasks)
        return chunk_results

    @iterate_with_report(report_interval, report_newline, report_name)
    def mapper_function(tasks):
        future_list = []

        for chunk_tasks in split_chunks(tasks, chunk_size):
            if len(future_list) == num_proc:
                _, chunk_results = pop_future_result(future_list, ordered)
                for result in chunk_results:
                    yield result
            future_list.append(chunk_function.remote(chunk_tasks))

        while len(future_list) > 0:
            _, chunk_results = pop_future_result(future_list, ordered)
            for result in chunk_results:
                yield result

    return mapper_function


def build_mapper_class(Class, num_proc, ordered, chunk_size,
                       report_interval, report_newline, report_name):
    @ray.remote
    class ChunkClass:

        def __init__(self, Class, *args, **kwargs):
            self.instance = Class(*args, **kwargs)

        def __call__(self, chunk_tasks):
            chunk_results = map_on_tasks(self.instance, chunk_tasks)
            return chunk_results

    class MapperClass:

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        @iterate_with_report(report_interval, report_newline, report_name)
        def __call__(self, tasks):
            busy_actor_list = []
            idle_actor_queue = collections.deque()
            future_list = []

            for _ in range(num_proc):
                actor = ChunkClass.remote(Class, *self.args, **self.kwargs)
                idle_actor_queue.append(actor)

            for chunk_tasks in split_chunks(tasks, chunk_size):
                if len(future_list) == num_proc:
                    index, chunk_results = pop_future_result(future_list, ordered)
                    for result in chunk_results:
                        yield result
                    idle_actor_queue.append(busy_actor_list.pop(index))
                actor = idle_actor_queue.popleft()
                busy_actor_list.append(actor)
                future_list.append(actor.__call__.remote(chunk_tasks))

            while len(future_list) > 0:
                index, chunk_results = pop_future_result(future_list, ordered)
                for result in chunk_results:
                    yield result

    return MapperClass


def ray_mapper(num_proc=None, ordered=True, chunk_size=1,
               report_interval=0, report_newline=False, report_name=None):
    init_ray_backend()
    num_proc = num_proc or mp.cpu_count()

    def decorator(api):
        if inspect.isfunction(api):
            build_mapper = build_mapper_function
        elif inspect.isclass(api):
            build_mapper = build_mapper_class
        else:
            raise TypeError("The @emp.ray_mapper(...) decorator must be applied to either a function or to a class.")
        return build_mapper(
            api, num_proc, ordered, chunk_size,
            report_interval, report_newline,
            report_name=report_name or api.__name__,
        )

    return decorator
