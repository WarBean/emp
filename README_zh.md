# EMP: **E**asy **M**ultiprocessing for **P**ython

[Engilish Document](README.md)

目录
=================

   * [EMP: <strong>E</strong>asy <strong>M</strong>ultiprocessing for <strong>P</strong>ython](#emp-easy-multiprocessing-for-python)
      * [介绍](#介绍)
      * [安装](#安装)
      * [快速上手](#快速上手)
         * [emp.mapper修饰器](#empmapper修饰器)
         * [emp.iterator修饰器](#empiterator修饰器)

## 介绍

EMP旨在通过简单好用的接口，帮助用户使用多进程机制加速自己的Python代码。EMP封装了两个多进程后端，一个是Python原生的`multiprocessing`模块另一个是分布式计算框架[`Ray`](https://github.com/ray-project/ray)，在EMP的接口中分别称为`pymp`和`ray`。

一般而言，`pymp`后端用起来比较稳定，但是当处理流程的输入输出object比较大时，速度较慢；而`ray`后端处理大object速度快，但是有时候不太稳定。用户可以根据自己的需要选择合适的后端。

## 安装

```
pip install PyEMP
```

## 快速上手

### `emp.mapper`修饰器

利用`emp.mapper`，用户可以轻松将一个函数或者一个带`__call__`方法的类转换成可并行执行的版本。

##### 1.用Python原生的`multiprocessing`模块作为后端，并行执行一个函数。

```python
import emp

@emp.mapper(backend='pymp')
def multiply(x):
    return x * 2

results = list(multiply(range(100)))
assert results == [x * 3 for x in range(100)]
```

##### 2.用Python原生的`multiprocessing`模块作为后端，并行执行一个callable object。

```python
import emp

@emp.mapper(backend='pymp')
class Multiply:
    def __init__(self, mul):
        self.mul = mul
    def __call__(self, x):
        return x * self.mul

multiply = Multiply(2)
results = list(multiply(range(100)))
assert results == [x * 2 for x in range(100)]
```

##### 3.换成用分布式计算框架[`Ray`](https://github.com/ray-project/ray)作为后端。

```python
# ...
@emp.mapper(backend='ray')
# ...
```

##### 4.设置其他并行进程数 (默认值是`multiprocessing.cpu_count()`的输出).

```python
# ...
@emp.mapper(backend='pymp', num_proc=64)
# ...
```


##### 5.分块处理，减少额外消耗，从而加速。

```python
# ...
@emp.mapper(backend='pymp', chunk_size=100)
# ...
```

##### 6.按一定间隔自动打印处理进度。

```python
import emp

@emp.mapper(backend='pymp', report_interval=10, report_newline=True)
def multiply(x):
    return x * 2

results = list(multiply(range(100)))
```

输出:

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

如果设置 `report_newline=False` (默认设置), 则所有信息会在一行里面输出。

##### 7.设置`ordered=False`时，输出可能是乱序的，而执行速度可能会加快。

```python
import emp
import time
import random

@emp.mapper(backend='pymp', ordered=False)
def multiply(x):
    time.sleep(random.random())
    return x * 2

results = list(multiply(range(10)))
print(results)  # 很可能是乱序的
```

##### 8.用户也可以逐个取回结果。

```python
import emp

@emp.mapper(backend='pymp')
def multiply(x):
    return x * 2

results = []
for result in multiply(range(10)):
    results.append(result)
```

### `emp.iterator`修饰器

利用`emp.iterator`，用户可以轻松将一个迭代器转换成并行版本。调用这个新的迭代器时，它会在另一个进程里面执行，并将结果逐个返回到当前进程。

##### 1.使用`pymp`作为后端

```python
import emp

@emp.iterator(backend='pymp')
def read_file(filepath):
    for line in open(filepath):
        yield line

results = list(read_file('temp.txt'))
```

##### 2.使用`ray`作为后端

```python
# ...
@emp.iterator(backend='ray')
# ...
```

##### 4.分块处理，减少额外消耗，从而加速。

```python
# ...
@emp.iterator(backend='pymp', chunk_size=100)
# ...
```

##### 5.预取更多数据块（默认是1）从而加速

```python
# ...
@emp.iterator(backend='pymp', prefetch_size=10)
# ...
```
