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

from daemonx.daemon import Daemon as Daemonx

from qork.worker_pool import Pool


class Daemon(Daemonx):
    """Reads from work queues, spawns workers to do needed work"""

    def __init__(self, *args, **kwargs):
        super(Daemon, self).__init__(*args, **kwargs)

        qd_conf = self.global_conf['qork_daemon']
        self.vtime = int(qd_conf.get('visibility_timeout', 3600))
        self.pool = Pool(int(qd_conf.get('worker_count', 4)), self.vtime)

        q_conf = self.global_conf['qork']
        # that last param is whack
        queue = __import__(q_conf['queue_module'], globals(), locals(), [''])
        self._queue_reader = queue.QueueReader(q_conf)

    def get_worker_class(self, message):
        """Returns class of worker needed to do message's work"""
        try:
            worker_type = 'worker-%s' % (message.body['worker_type'])
            if worker_type not in self.global_conf:
                raise RuntimeError("Invalid worker type '%s'" % (worker_type))
            w_conf = self.global_conf[worker_type]
            import_target, class_name = w_conf['class'].rsplit('.', 1)
            module = __import__(import_target, fromlist=[import_target])
            return getattr(module, class_name)
        except Exception:
            self.logger.exception('Get worker class failed')
            message.handle_exception()
        return None

    def run_once(self, *args, **kwargs):
        """Run the daemon one time"""
        self.logger.info('Run begin')
        message = self._queue_reader.get_message()
        while message:
            self.update_progress_marker()
            klass = self.get_worker_class(message)
            if klass:
                self.logger.info('Processing message %s with %s' % (
                    message.meta['message_id'],
                    message.body['worker_type']))
                conf_section = 'worker-%s' % (message.body['worker_type'])
                self.pool.start(
                    klass.run_with_message,
                    args=[self.logger, self.global_conf, conf_section,
                          message])
                self.pool.wait()
            message = self._queue_reader.get_message()
        self.pool.join()
        self.logger.info('Run end')
        self.update_progress_marker(True)
