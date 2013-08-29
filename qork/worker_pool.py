# Copyright (c) 2013 Greg Lange
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License

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
