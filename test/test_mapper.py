import re
import emp
import time
import pytest
import random


@pytest.mark.parametrize('backend', ['pymp', 'ray'])
@pytest.mark.parametrize('num_proc', [None, 1, 2])
@pytest.mark.parametrize('ordered', [True, False])
@pytest.mark.parametrize('chunk_size', [1, 3])
@pytest.mark.parametrize('num_tasks', [10])
@pytest.mark.parametrize('api', ['func', 'class'])
@pytest.mark.parametrize('use_task', [False, True])
@pytest.mark.parametrize('add', [0, 1])
@pytest.mark.parametrize('mul', [2])
def test_mapper_general(backend, num_proc, ordered, chunk_size, num_tasks, api, use_task, add, mul):
    sleep_seconds = 0.5
    if api == 'func':

        if not use_task:

            @emp.mapper(backend, num_proc, ordered, chunk_size)
            def compute(x):
                time.sleep(sleep_seconds * random.random())
                return (x + add) * mul

            results = list(compute(range(num_tasks)))

        else:

            @emp.mapper(backend, num_proc, ordered, chunk_size)
            def compute(x, add, mul):
                time.sleep(sleep_seconds * random.random())
                return (x + add) * mul
            results = list(compute((emp.Task(x, add, mul=mul) for x in range(num_tasks))))

    elif api == 'class':

        if not use_task:

            @emp.mapper(backend, num_proc, ordered, chunk_size)
            class Compute:

                def __init__(self, add, mul):
                    self.add = add
                    self.mul = mul

                def __call__(self, x):
                    time.sleep(sleep_seconds * random.random())
                    return (x + self.add) * self.mul

            compute = Compute(add, mul)
            results = list(compute(range(num_tasks)))

        else:

            @emp.mapper(backend, num_proc, ordered, chunk_size)
            class Compute:

                def __init__(self, add):
                    self.add = add

                def __call__(self, x, mul):
                    time.sleep(sleep_seconds * random.random())
                    return (x + self.add) * mul

            compute = Compute(add)
            results = list(compute((emp.Task(x, mul) for x in range(num_tasks))))

    else:
        raise ValueError(f'unknown [api]: {api}')

    if ordered:
        assert results == [(x + add) * mul for x in range(num_tasks)]
    else:
        assert sorted(results) == [(x + add) * mul for x in range(num_tasks)]


@pytest.mark.parametrize('backend', ['pymp', 'ray'])
@pytest.mark.parametrize('chunk_size', [1, 3])
@pytest.mark.parametrize('api', ['func', 'class'])
def test_mapper_unordered(backend, chunk_size, api):
    num_proc = None
    ordered = False
    sleep_seconds = 0.01
    num_tasks = 1000
    add = 1
    mul = 2

    if api == 'func':

        @emp.mapper(backend, num_proc, ordered, chunk_size)
        def compute(x):
            time.sleep(sleep_seconds * random.random())
            return (x + add) * mul

        results = list(compute(range(num_tasks)))

    elif api == 'class':

        @emp.mapper(backend, num_proc, ordered, chunk_size)
        class Compute:

            def __init__(self, add, mul):
                self.add = add
                self.mul = mul

            def __call__(self, x):
                time.sleep(sleep_seconds * random.random())
                return (x + self.add) * self.mul

        compute = Compute(add, mul)
        results = list(compute(range(num_tasks)))

    else:
        raise ValueError(f'unknown [api]: {api}')

    assert results != [(x + add) * mul for x in range(num_tasks)]
    assert sorted(results) == [(x + add) * mul for x in range(num_tasks)]


@pytest.mark.parametrize('backend', ['pymp', 'ray'])
@pytest.mark.parametrize('report_interval', [0, 1, 3])
@pytest.mark.parametrize('report_newline', [False, True])
@pytest.mark.parametrize('report_name', [None, 'hello'])
@pytest.mark.parametrize('num_tasks', [1, 2, 10, 15, 17])
def test_mapper_report(capsys, backend, report_interval, report_newline, report_name, num_tasks):
    @emp.mapper(
        backend,
        report_interval=report_interval,
        report_newline=report_newline,
        report_name=report_name,
    )
    def compute(x):
        time.sleep(0.01 * random.random())
        return (x + 1) * 2

    num_tasks = 10
    list(compute(range(num_tasks)))
    output = capsys.readouterr().out
    if report_interval == 0:
        assert output == ''
    else:
        if report_newline:
            num_lines = output.count('\n')
            output_lines = output.strip().split('\n')
        else:
            num_lines = output.count('\r')
            output_lines = output.strip().split('\r')
        assert num_lines == len(output_lines)
        report_name = report_name or 'compute'
        for progress in range(num_tasks):
            progress += 1
            if progress == 1 or progress % report_interval == 0 or progress == num_tasks:
                line = output_lines.pop(0)
                assert re.fullmatch(f'\[.*\] \[{report_name}\] progress \[{progress} / {num_tasks}\]', line)
