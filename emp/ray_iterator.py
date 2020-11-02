__all__ = ['ray_iterator']


import ray
from collections import deque
from .ray_backend import init_ray_backend


@ray.remote
class ChunkIterator:

    def __init__(self, api, args, kwargs, chunk_size):
        self.iterator = api(*args, **kwargs)
        self.chunk_size = chunk_size
        self.stop_iteration = False

    def next_chunk(self):
        if self.stop_iteration:
            return []
        chunk = []
        for _ in range(self.chunk_size):
            try:
                item = next(self.iterator)
                chunk.append(item)
            except StopIteration:
                self.stop_iteration = True
        return chunk


def ray_iterator(prefetch_size=1, chunk_size=1):
    init_ray_backend()

    def decorator(api):

        def iterator_api(*args, **kwargs):
            chunk_iterator = ChunkIterator.remote(api, args, kwargs, chunk_size)
            future_queue = deque()
            for i in range(prefetch_size):
                future_queue.append(chunk_iterator.next_chunk.remote())
            while True:
                chunk = ray.get(future_queue.popleft())
                if len(chunk) == 0:
                    break
                else:
                    future_queue.append(chunk_iterator.next_chunk.remote())
                    for item in chunk:
                        yield item
            while len(future_queue) > 0:
                ray.get(future_queue.popleft())

        return iterator_api

    return decorator
