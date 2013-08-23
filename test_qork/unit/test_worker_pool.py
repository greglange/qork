from qork import worker_pool
import util


class TestCase(util.MockerTestCase):
    def test_init(self):
        pool = worker_pool.Pool(4)
        self.assertEquals(4, len(pool.workers))

    def test_free_workers(self):
        class Pool(worker_pool.Pool):
            def __init__(self, workers):
                self.workers = workers

        class Worker(object):
            def __init__(self, alive=True):
                self.alive = alive

            def is_alive(self):
                return self.alive

            def join(self):
                pass

        data = [
            [3, [None, None, None]],
            [0, [Worker(), Worker(), Worker()]],
            [1, [Worker(), Worker(), Worker(False)]],
            [2, [Worker(), Worker(False), Worker(False)]],
            [3, [Worker(False), Worker(False), Worker(False)]],
            [3, [None, Worker(False), Worker(False)]],
        ]

        for count, workers in data:
            pool = Pool(workers)
            self.assertEquals(count, pool.free_workers())
            self.assertEquals(count, pool.workers.count(None))

    def test_join(self):
        class Pool(worker_pool.Pool):
            def __init__(self, workers):
                self.workers = workers

        class Worker(object):
            def __init__(self):
                self.join_called = 0

            def join(self):
                self.join_called += 1

        data = [
            [None, Worker(), Worker()],
            [None, None, Worker()],
            [Worker(), Worker(), Worker()],
        ]

        for w in data:
            workers = w[:]
            pool = Pool(w)
            pool.join()
            for worker in workers:
                if worker:
                    self.assertEquals(1, worker.join_called)
            self.assertEquals(len(workers), pool.workers.count(None))

    def test_start(self):
        class Worker(object):
            pass

        class Pool(worker_pool.Pool):
            def __init__(self, workers):
                self.workers = workers

        data = [
            [['target1', 'args1', 'kwargs1'], [None, None, None]],
            [['target2', 'args2', 'kwargs2'], [None, Worker(), Worker()]],
            [['target3', 'args3', 'kwargs3'], [Worker(), None, Worker()]],
            [['target4', 'args4', 'kwargs4'], [Worker(), Worker(), None]],
        ]

        class Process(object):
            def __init__(self, target=None, args=None, kwargs=None):
                self.target = target
                self.args = args
                self.kwargs = kwargs
                self.start_called = 0

            def start(self):
                self.start_called += 1

        self.mock(worker_pool, 'Process', Process)

        for p, w in data:
            pool = Pool(w)
            pool.start(*p)

            for worker in pool.workers:
                if type(worker) == Process:
                    self.assertEquals(p[0], worker.target)
                    self.assertEquals(p[1], worker.args)
                    self.assertEquals(p[2], worker.kwargs)
                    self.assertEquals(1, worker.start_called)

    def test_start_exception(self):
        class Worker(object):
            pass

        class Pool(worker_pool.Pool):
            def __init__(self):
                self.workers = [Worker(), Worker(), Worker(), Worker()]

        pool = Pool()
        self.assertRaises(RuntimeError, pool.start, 'target', 'args', 'kwargs')

    def test_wait(self):
        class Pool(worker_pool.Pool):
            def __init__(self):
                self.free_workers_count = 3
                self.free_workers_called = 0

            def free_workers(self):
                self.free_workers_called += 1
                if self.free_workers_called >= self.free_workers_count:
                    return 1
                else:
                    return 0

        class Sleep(object):
            def __init__(self):
                self.called = 0

            def count(self, _):
                self.called += 1

        sleep = Sleep()

        self.mock(worker_pool, 'Pool', Pool)
        self.mock(worker_pool.time, 'sleep', sleep.count)

        pool = Pool()
        pool.wait()

        self.assertEquals(pool.free_workers_count, pool.free_workers_called)
        self.assertEquals(pool.free_workers_count - 1, sleep.called)
