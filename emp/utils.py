__all__ = ['split_chunks', 'iterate_with_report']


import sys
import time
import inspect


def split_chunks(items, chunk_size):
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


def _iterate_with_report(generator, total_length, report_interval, report_newline, report_name):
    for progress, result in enumerate(generator):
        progress += 1
        if progress == 1 or progress % report_interval == 0:
            date = time.strftime('%Y-%m-%d %H:%M:%S')
            msg = f'[{date}] [{report_name}] progress [{progress} / {total_length}]'
            if report_newline:
                sys.stdout.write(msg + '\n')
            else:
                sys.stdout.write('\r' + msg)
        yield result
    if progress != 1 and progress % report_interval != 0:
        date = time.strftime('%Y-%m-%d %H:%M:%S')
        msg = f'[{date}] [{report_name}] progress [{progress} / {total_length}]'
        if report_newline:
            sys.stdout.write(msg + '\n')
        else:
            sys.stdout.write('\r' + msg)
    if not report_newline:
        sys.stdout.write('\n')


def iterate_with_report(report_interval, report_newline, report_name):
    def decorator(old_method):
        if report_interval == 0:
            return old_method

        if 'self' in inspect.getfullargspec(old_method).args:
            def new_method(self, tasks, total_length=None):
                if total_length is None:
                    try:
                        total_length = len(tasks)
                    except:
                        total_length = 'unknown'
                generator = old_method(self, tasks)
                return _iterate_with_report(generator, total_length, report_interval, report_newline, report_name)
        else:
            def new_method(tasks, total_length=None):
                if total_length is None:
                    try:
                        total_length = len(tasks)
                    except:
                        total_length = 'unknown'
                generator = old_method(tasks)
                return _iterate_with_report(generator, total_length, report_interval, report_newline, report_name)

        return new_method

    return decorator
