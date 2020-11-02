__all__ = ['RayDataLoader']


import os
import ray
import sys
import torch
import random
import traceback
import numpy as np
from collections import deque
from .ray_backend import init_ray_backend
from torch.utils.data.dataloader import default_collate
from torch.utils.data import SequentialSampler, RandomSampler, BatchSampler


class ExceptionWrapper(object):

    def __init__(self, exc_info):
        self.exc_type = exc_info[0]
        self.exc_msg = ''.join(traceback.format_exception(*exc_info))


@ray.remote
class BatchActor:

    def __init__(self, dataset, collate_fn, seed, cuda_visible_devices, device_id):
        os.environ['CUDA_VISIBLE_DEVICES'] = cuda_visible_devices
        torch.cuda.set_device(device_id)
        self.dataset = dataset
        self.collate_fn = collate_fn

        random.seed(seed)
        np.random.seed(seed)
        torch.manual_seed(seed)
        torch.cuda.manual_seed(seed)

    def get_batch(self, batch_indices):
        try:
            samples = [self.dataset[i] for i in batch_indices]
            batch = self.collate_fn(samples)
        except:
            return ExceptionWrapper(sys.exc_info())
        return batch


class RayDataLoaderIter(object):

    def __init__(self, loader):
        self.dataset = loader.dataset
        self.collate_fn = loader.collate_fn
        self.batch_sampler = loader.batch_sampler
        self.num_workers = loader.num_workers
        self.timeout = loader.timeout
        self.prefetch_num = loader.prefetch_num
        assert loader.prefetch_num > 1, 'need prefetch at least one iter data'
        self.sample_iter = iter(self.batch_sampler)
        self.base_seed = random.randint(0, 1000000)

        if self.num_workers > 0:
            cuda_visible_devices = ','.join(str(i) for i in range(torch.cuda.device_count()))
            cuda_visible_devices = os.environ.get('CUDA_VISIBLE_DEVICES', cuda_visible_devices)
            device_id = torch.cuda.current_device()
            self.actor_list = []
            self.future_queue_list = []
            for i in range(self.num_workers):
                actor = BatchActor.remote(
                    self.dataset,
                    self.collate_fn,
                    self.base_seed + i,
                    cuda_visible_devices,
                    device_id,
                )
                self.actor_list.append(actor)
                self.future_queue_list.append(deque())
            self.fetch_index = 0
            self.prefetch_index = 0
            for _ in range(self.prefetch_num * self.num_workers):
                self.prefetch()

    def __len__(self):
        return len(self.batch_sampler)

    def prefetch(self):
        batch_indices = next(self.sample_iter, None)
        if batch_indices is None:
            return
        actor = self.actor_list[self.prefetch_index]
        future_queue = self.future_queue_list[self.prefetch_index]
        future = actor.get_batch.remote(batch_indices)
        future_queue.append(future)
        self.prefetch_index = (self.prefetch_index + 1) % self.num_workers

    def __next__(self):
        if self.num_workers == 0:  # same-process loading
            batch_indices = next(self.sample_iter)  # may raise StopIteration
            samples = [self.dataset[i] for i in batch_indices]
            batch = self.collate_fn(samples)
        elif self.fetch_index >= len(self):
            raise StopIteration
        else:
            self.prefetch()
            future_queue = self.future_queue_list[self.fetch_index % self.num_workers]
            future = future_queue.popleft()
            if self.timeout == 0:
                batch = ray.get(future)
            else:
                try:
                    batch = ray.get(future, timeout=self.timeout)
                except ray.exceptions.GetTimeoutError:
                    raise RuntimeError(f'DataLoader timed out after {self.timeout} seconds')
            if isinstance(batch, ExceptionWrapper):
                raise batch.exc_type(batch.exc_msg)
            self.fetch_index += 1
        return batch

    def __iter__(self):
        return self

    def __getstate__(self):
        raise NotImplementedError("RayDataLoaderIter cannot be pickled")


class RayDataLoader(object):

    __initialized = False

    def __init__(self,
                 dataset,
                 batch_size=1,
                 shuffle=False,
                 sampler=None,
                 batch_sampler=None,
                 num_workers=0,
                 collate_fn=default_collate,
                 drop_last=False,
                 timeout=0,
                 prefetch_num=2):
        self.dataset = dataset
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.collate_fn = collate_fn
        self.drop_last = drop_last
        self.timeout = timeout
        self.prefetch_num = prefetch_num

        if timeout < 0:
            raise ValueError('timeout option should be non-negative')

        if batch_sampler is not None:
            if batch_size > 1 or shuffle or sampler is not None or drop_last:
                raise ValueError('batch_sampler option is mutually exclusive '
                                 'with batch_size, shuffle, sampler, and '
                                 'drop_last')
            self.batch_size = None
            self.drop_last = None

        if sampler is not None and shuffle:
            raise ValueError('sampler option is mutually exclusive with '
                             'shuffle')

        if self.num_workers < 0:
            raise ValueError('num_workers option cannot be negative; '
                             'use num_workers=0 to disable multiprocessing.')

        if batch_sampler is None:
            if sampler is None:
                if shuffle:
                    sampler = RandomSampler(dataset)
                else:
                    sampler = SequentialSampler(dataset)
            batch_sampler = BatchSampler(sampler, batch_size, drop_last)

        self.sampler = sampler
        self.batch_sampler = batch_sampler
        self.__initialized = True

    def __setattr__(self, attr, val):
        if self.__initialized and attr in ('batch_size', 'sampler', 'drop_last'):
            raise ValueError('{} attribute should not be set after {} is ' 'initialized'.format(
                attr, self.__class__.__name__,
            ))
        super(RayDataLoader, self).__setattr__(attr, val)

    def __iter__(self):
        init_ray_backend()
        return RayDataLoaderIter(self)

    def __len__(self):
        return len(self.batch_sampler)
