__all__ = ['pymp_iterator']


import time
import multiprocessing as mp


def chunk_worker(api, args, kwargs, queue, chunk_size):
    chunk = []
    for item in api(*args, **kwargs):
        chunk.append(item)
        if len(chunk) == chunk_size:
            queue.put(chunk)
            chunk = []
    if len(chunk) > 0:
        queue.put(chunk)
    queue.put([])
    time.sleep(0.1)  # otherwise will raise: Connection reset by peer


def pymp_iterator(prefetch_size=1, chunk_size=1):
    def decorator(api):
        def iterator_api(*args, **kwargs):
            queue = mp.Queue(maxsize=prefetch_size)
            process = mp.Process(target=chunk_worker, args=(api, args, kwargs, queue, chunk_size))
            process.daemon = True
            process.start()
            while True:
                chunk = queue.get()
                if len(chunk) == 0:
                    break
                for item in chunk:
                    yield item
            process.join()
        return iterator_api
    return decorator
