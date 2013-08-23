import time

from multiprocessing import Process


class Pool(object):
    """Manages a pool of workers"""

    def __init__(self, worker_count):
        self.workers = [None] * int(worker_count)

    def free_workers(self):
        """Return the number of free workers"""
        for i, worker in enumerate(self.workers):
            if worker and not worker.is_alive():
                self.workers[i] = None
                worker.join()
        return self.workers.count(None)

    def join(self):
        """Join all active workers"""
        for i, worker in enumerate(self.workers):
            if worker:
                self.workers[i] = None
                worker.join()

    def start(self, target, args=[], kwargs={}):
        """Spawns a new process"""
        for i, worker in enumerate(self.workers):
            if worker is None:
                self.workers[i] = Process(target=target, args=args,
                                          kwargs=kwargs)
                self.workers[i].start()
                return
        raise RuntimeError("No available workers")

    def wait(self, sleep=5):
        """Blocks until a worker is free"""
        while True:
            if self.free_workers() > 0:
                return
            time.sleep(sleep)
