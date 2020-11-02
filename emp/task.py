__all__ = ['Task', 'zip_tasks', 'map_on_tasks']


class Task:

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        s = []
        for arg in self.args:
            s.append(f'{arg}')
        for kw, arg in self.kwargs.items():
            s.append(f'{kw}={arg}')
        s = ', '.join(s)
        return s


def zip_tasks(*seq_args, **seq_kwargs):
    tasks = []
    N = len(seq_args)
    seq_arguments = seq_args + list(seq_kwargs.values())
    for arguments in zip(*seq_arguments):
        args = arguments[:N]
        kwargs = {key: arguments[N + i] for i, key in enumerate(seq_kwargs.keys())}
        task = Task(*args, **kwargs)
        tasks.append(task)
    return tasks


def map_on_tasks(function, tasks):
    results = []
    for task in tasks:
        if isinstance(task, Task):
            result = function(*task.args, **task.kwargs)
        else:
            result = function(task)
        results.append(result)
    return results
