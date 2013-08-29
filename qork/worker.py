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

from signal import alarm, SIGALRM, signal


class Worker(object):
    """Does work required by messages from work queues"""

    def __init__(self, global_conf, conf_section, message):
        self.global_conf = global_conf
        self.conf_section = conf_section
        self.message = message

        conf = global_conf[conf_section]
        self.timeout_seconds = int(conf.get('timeout_seconds', 3600))

    @classmethod
    def from_message(cls, global_conf, conf_section, message):
        """Creates worker from message"""
        raise NotImplementedError

    def handle_alarm(self, signum, frame):
        raise RuntimeError("Worker timed out")

    @classmethod
    def run_with_message(cls, logger, global_conf, conf_section, message):
        """Runs the worker, sets a timeout alarm, handles exceptions,
        deletes message on success"""

        try:
            worker = cls.from_message(global_conf, conf_section, message)
        except Exception:
            logger.exception('Getting worker from message failed')
            message.handle_exception()
            return

        try:
            signal(SIGALRM, worker.handle_alarm)
            alarm(worker.timeout_seconds)
            worker.run()
        except Exception:
            logger.exception('Running worker failed')
            message.handle_exception()
        else:
            message.delete()

    def run(self):
        """Does this worker's work"""
        raise NotImplementedError
