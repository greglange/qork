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

import signal

from qork import worker
import util


class Worker(worker.Worker):
    def __init__(self, test, message=None, run_exception=False):
        self.test = test
        self.message = message
        self.run_exception = run_exception
        self.run_called = 0
        self.alarm_called = 0
        self.signal_called = 0
        self.timeout_seconds = 'some_seconds'
        self.test.mock(worker, 'alarm', self.alarm)
        self.test.mock(worker, 'signal', self.signal)

    def run(self):
        self.run_called += 1
        if self.run_exception:
            raise Exception

    def alarm(self, seconds):
        self.alarm_called += 1
        self.test.assertEquals(self.timeout_seconds, seconds)

    def signal(self, signalnum, handler):
        self.signal_called += 1
        self.test.assertEquals(signal.SIGALRM, signalnum)
        self.test.assertEquals(self.handle_alarm, handler)


class Message(object):
    def __init__(self):
        self.delete_called = 0
        self.handle_exception_called = 0

    def delete(self):
        self.delete_called += 1

    def handle_exception(self):
        self.handle_exception_called += 1


class StringIO:
    def __init__(self, test, content):
        self.test = test
        self.content = content

    def method(self, content):
        self.test.assertEquals(self.content, content)
        return self


class InternalProxyUpload(object):
    def __init__(self, test, source_file, account, container,
                 object_name):
        self.test = test
        self.source_file = source_file
        self.account = account
        self.container = container
        self.object_name = object_name

    def upload_file(self, source_file, account, container, object_name,
                    compress=True, content_type='application/x-gzip',
                    etag=None):
        self.test.assertEquals(self.source_file, source_file)
        self.test.assertEquals(self.account, account)
        self.test.assertEquals(self.container, container)
        self.test.assertEquals(self.object_name, object_name)
        self.test.assertEquals(True, compress)
        self.test.assertEquals(content_type, 'application/x-gzip')
        self.test.assertEquals(None, etag)
        return True


class TestWorker(util.MockerTestCase):
    def test_init(self):
        conf_section = 'worker'

        global_conf = {conf_section: {'timeout_seconds': 87, }, }
        message = 'some_message'

        w = worker.Worker(global_conf, conf_section, message)
        self.assertEquals(global_conf, w.global_conf)
        self.assertEquals(message, w.message)
        self.assertEquals(
            w.timeout_seconds, global_conf[conf_section]['timeout_seconds'])

    def test_from_message(self):
        w = Worker(self)
        self.assertRaises(
            NotImplementedError, w.from_message, 'global_conf',
            'conf_section', 'message')

    def test_handle_alarm(self):
        class Worker(worker.Worker):
            def __init__(self):
                pass

        w = Worker()
        self.assertRaises(RuntimeError, w.handle_alarm, 'signum', 'frame')

    def test_run(self):
        class Worker(worker.Worker):
            def __init__(self):
                pass

        w = Worker()
        self.assertRaises(NotImplementedError, w.run)
