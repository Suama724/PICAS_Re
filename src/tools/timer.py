'''
记录，测试用时

这里contextmanager可以快速封成decorator
然后yield前后分别填入__enter__与__exit__中
这样可以用with使用而非单独一个@timer，相当于更方便些

使用：
with timer("XXX测试"):
    func(...)
'''

import time
from contextlib import contextmanager

@contextmanager
def timer(label: str):
    time_start = time.perf_counter()
    yield
    time_used = time.perf_counter() - time_start
    print(f"[Timer] {label}: {time_used:.5f}s")
