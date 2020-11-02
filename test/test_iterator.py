import emp
import pytest


@pytest.mark.parametrize('backend', ['pymp', 'ray'])
@pytest.mark.parametrize('prefetch_size', [1, 2, 10])
@pytest.mark.parametrize('chunk_size', [1, 3])
@pytest.mark.parametrize('api', ['func', 'class'])
@pytest.mark.parametrize('length', [20, 47])
def test_iterator(backend, prefetch_size, chunk_size, api, length):
    if api == 'func':
        @emp.iterator(backend, prefetch_size, chunk_size)
        def iter_api(length):
            for i in range(length):
                yield i

    elif api == 'class':
        @emp.iterator(backend, prefetch_size, chunk_size)
        class iter_api:

            def __init__(self, length):
                self.length = length
                self.i = -1

            def __iter__(self):
                return self

            def __next__(self):
                self.i += 1
                if self.i < self.length:
                    return self.i
                raise StopIteration

    result = list(iter_api(length))
    assert result == list(range(length))
