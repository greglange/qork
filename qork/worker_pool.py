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
import os
import signal

from daemonx.daemon import kill_child_process


class Pool(object):
    """Manages a pool of workers"""

    def __init__(self, worker_count, worker_timeout=None):
        # (start_time, process)
        self.workers = [None] * int(worker_count)
        self.worker_timeout = worker_timeout

    def free_workers(self):
        """Return the number of free workers"""
        for i, worker in enumerate(self.workers):
            if not worker:
                continue
            if not worker[1].is_alive():
                worker[1].join()
                self.workers[i] = None
            elif self.worker_timeout and \
                    time.time() - worker[0] > self.worker_timeout:
                # TODO: is this ok?  it seems to work
                # may need to write some code to get a Process class
                # works like I want it to
                os.kill(worker[1].pid, signal.SIGKILL)
                self.workers[i] = None
        return self.workers.count(None)

    def join(self, sleep=5):
        """Join all active workers"""
        while self.free_workers() < len(self.workers):
            time.sleep(sleep)

    def start(self, target, args=[], kwargs={}):
        """Spawns a new process"""
        for i, worker in enumerate(self.workers):
            if worker is None:
                worker = (
                    time.time(),
                    Process(target=target, args=args, kwargs=kwargs)
                    )
                worker[1].daemon = True
                worker[1].start()
                self.workers[i] = worker
                return
        raise RuntimeError("No available workers")

    def wait(self, sleep=5):
        """Blocks until a worker is free"""
        while True:
            if self.free_workers() > 0:
                return
            time.sleep(sleep)
