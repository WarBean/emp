__all__ = ['pymp_mapper']


import inspect
import multiprocessing as mp
from .task import map_on_tasks
from .utils import split_chunks, iterate_with_report


class Empty:

    pass


def chunk_worker(function, class_info, input_queue, output_queue):
    if function is None:
        Class, args, kwargs = class_info
        function = Class(*args, **kwargs)
    while True:
        obj = input_queue.get()
        if isinstance(obj, Empty):
            break
        index, chunk_tasks = obj
        chunk_results = map_on_tasks(function, chunk_tasks)
        output_queue.put((index, chunk_results))


def map_function_or_class(function, class_info, tasks, num_proc=None, ordered=True, chunk_size=1):
    if (function is None) == (class_info) is None:
        raise ValueError('one and only one of [function] and [class_info] should be provided')
    num_proc = num_proc or mp.cpu_count()

    proc_list = []
    input_queue = mp.Queue()
    output_queue = mp.Queue()
    for _ in range(num_proc):
        proc = mp.Process(target=chunk_worker, args=(function, class_info, input_queue, output_queue))
        proc.daemon = True
        proc.start()
        proc_list.append(proc)

    send_index = 0
    stop_iteration = False
    chunk_generator = enumerate(split_chunks(tasks, chunk_size))
    for _ in range(num_proc):
        indexed_chunk = next(chunk_generator, None)
        if indexed_chunk is None:
            stop_iteration = True
            break
        else:
            input_queue.put(indexed_chunk)
            send_index += 1

    if ordered:
        recv_dict = dict()
        recv_index = 0
        while recv_index < send_index:
            if recv_index not in recv_dict:
                index, chunk_results = output_queue.get()
                recv_dict[index] = chunk_results
                if not stop_iteration:
                    indexed_chunk = next(chunk_generator, None)
                    if indexed_chunk is None:
                        stop_iteration = True
                    else:
                        input_queue.put(indexed_chunk)
                        send_index += 1
            else:
                for result in recv_dict.pop(recv_index):
                    yield result
                recv_index += 1
    else:
        recv_num = 0
        while recv_num < send_index:
            index, chunk_results = output_queue.get()
            if not stop_iteration:
                indexed_chunk = next(chunk_generator, None)
                if indexed_chunk is None:
                    stop_iteration = True
                else:
                    input_queue.put(indexed_chunk)
                    send_index += 1
            for result in chunk_results:
                yield result
            recv_num += 1

    for _ in range(num_proc):
        input_queue.put(Empty())

    for proc in proc_list:
        proc.join()


def build_mapper_function(function, num_proc, ordered, chunk_size,
                          report_interval, report_newline, report_name):
    @iterate_with_report(report_interval, report_newline, report_name)
    def mapper_function(tasks):
        return map_function_or_class(function, None, tasks, num_proc, ordered, chunk_size)

    return mapper_function


def build_mapper_class(Class, num_proc, ordered, chunk_size,
                       report_interval, report_newline, report_name):
    class MapperClass:

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        @iterate_with_report(report_interval, report_newline, report_name)
        def __call__(self, tasks):
            class_info = (Class, self.args, self.kwargs)
            return map_function_or_class(None, class_info, tasks, num_proc, ordered, chunk_size)

    return MapperClass


def pymp_mapper(num_proc=None, ordered=True, chunk_size=1,
                report_interval=0, report_newline=False, report_name=None):
    num_proc = num_proc or mp.cpu_count()

    def decorator(function_or_class):
        if inspect.isfunction(function_or_class):
            build_mapper = build_mapper_function
        elif inspect.isclass(function_or_class):
            build_mapper = build_mapper_class
        else:
            raise TypeError("The @emp.pymp_mapper(...) decorator must be applied to either a function or to a class.")
        return build_mapper(
            function_or_class, num_proc, ordered, chunk_size,
            report_interval, report_newline,
            report_name=report_name or function_or_class.__name__,
        )

    return decorator
