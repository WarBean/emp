# EMP: **E**asy **M**ultiprocessing for **P**ython

[中文文档](README_zh.md)

Table of Contents
=================

   * [EMP: <strong>E</strong>asy <strong>M</strong>ultiprocessing for <strong>P</strong>ython](#emp-easy-multiprocessing-for-python)
      * [Introduction](#introduction)
      * [Installation](#installation)
      * [Quick Start](#quick-start)
         * [emp.mapper decorator](#empmapper-decorator)
         * [emp.iterator decorator](#empiterator-decorator)

## Introduction

EMP provides a simple and effective way to accelerate your Python code. Under the hook, EMP use Python 's native `multiprocessing` package and [`Ray`](https://github.com/ray-project/ray) as backends, which are named `pymp` and `ray` respectively in EMP 's API.

Generally, `pymp` is more stable but can be slower when the input / output objects of the process is large, `ray` is faster for handling large object but can be unstable. Users can choose suitable backends in different cases.

## Installation

```
pip install PyEMP
```

## Quick Start

### `emp.mapper` decorator

With `emp.mapper`, users can easily convert a function or a class with `__call__` method into parallelized version.

##### 1.Execute a function in parallel with Python's native `multiprocessing` package as backend.

```python
import emp

@emp.mapper(backend='pymp')
def multiply(x):
    return x * 2

results = list(multiply(range(100)))
assert results == [x * 3 for x in range(100]
```

##### 2.Execute a callable object in parallel with Python 's native multiprocessing package as backend.

```python
import emp

@emp.mapper(backend='pymp')
class Multiply:
    def __init__(self, mul):
        self.mul = mul
    def __call__(self, x):
        return x * self.mul

multiply = Multiply(2)
results = list(f(range(100)))
assert results == [x * 2 for x in range(100]
```

##### 3.Execute with distributed package [`Ray`](https://github.com/ray-project/ray) as backend.

```python
# ...
@emp.mapper(backend='ray')
# ...
```

##### 4.Use different number of processes (default is the result of `multiprocessing.cpu_count()`).

```python
# ...
@emp.mapper(backend='pymp', num_proc=64)
# ...
```

##### 5.Process in chunks to reduce overhead and accelerate.

```python
# ...
@emp.mapper(backend='pymp', chunk_size=100)
# ...
```

##### 6.Automatically eeport the progress with specified interval.

```python
import emp

@emp.mapper(backend='pymp', report_interval=10, report_newline=True)
def multiply(x):
    return x * 2

results = list(multiply(range(100)))
```

Outputs:

```
[2020-11-02 22:15:38] [multiply] progress [1 / 100]
[2020-11-02 22:15:38] [multiply] progress [10 / 100]
[2020-11-02 22:15:38] [multiply] progress [20 / 100]
[2020-11-02 22:15:38] [multiply] progress [30 / 100]
[2020-11-02 22:15:38] [multiply] progress [40 / 100]
[2020-11-02 22:15:38] [multiply] progress [50 / 100]
[2020-11-02 22:15:38] [multiply] progress [60 / 100]
[2020-11-02 22:15:38] [multiply] progress [70 / 100]
[2020-11-02 22:15:38] [multiply] progress [80 / 100]
[2020-11-02 22:15:38] [multiply] progress [90 / 100]
[2020-11-02 22:15:38] [multiply] progress [100 / 100]
```

If `report_newline=False` (which is the default setting), all the report message will be printed in a single line.

##### 7.With `ordered=False`, the outputs unordered and the execution may be accelerated.

```python
import emp
import time
import random

@emp.mapper(backend='pymp', ordered=False)
def multiply(x):
    time.sleep(random.random())
    return x * 2

results = list(multiply(range(10)))
print(results)  # probably unordered
```

##### 8.Users can also fetch returned values one by one.

```python
import emp

@emp.mapper(backend='pymp')
def multiply(x):
    return x * 2

results = []
for result in multiply(range(10)):
    results.append(result)
```

### `emp.iterator` decorator

With `emp.iterator`, users can easily convert an iterator into parallelized version. When the new iterator is invoked, it will run in another process and iteratively yield results one by one to the current process.

##### 1.Use `pymp` as backend

```python
import emp

@emp.iterator(backend='pymp')
def read_file(filepath):
    for line in open(filepath):
        yield line

results = list(read_file('temp.txt'))
```

##### 2.Use `ray` as backend

```python
# ...
@emp.iterator(backend='ray')
# ...
```

##### 4.Process in chunks to reduce overhead and accelerate.

```python
# ...
@emp.iterator(backend='pymp', chunk_size=100)
# ...
```

##### 5.prefetch more chunks (default is 1) to acclerate

```python
# ...
@emp.iterator(backend='pymp', prefetch_size=10)
# ...
```
