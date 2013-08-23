import sys

from qork import daemon
import util


class TestDaemon(util.MockerTestCase):

    def test_init(self):
        conf = {
            'qork_daemon': {
                'worker_count': 4,
                'visibility_timeout': 10,
                'interval': 100,
            },

            'qork': {
                'sqs_access_key': 'some_access_key',
                'sqs_secret_access_key': 'some_secret_access_key',
                'global_prefix': 'some_global_prefix',
                'read_queues': 'work more_work even_more_work',
            },
        }

        class Pool(object):
            def __init__(_, worker_count):
                self.assertEquals(conf['qork_daemon']['worker_count'],
                                  worker_count)

        class QueueReader(object):
            def __init__(_, *args):
                for i, key in enumerate("sqs_access_key sqs_secret_access_key "
                                        "global_prefix".split()):
                    self.assertEquals(conf['qork'][key], args[i])
                self.assertEquals(
                    conf['qork']['read_queues'].split(), args[-2])

        self.mock(daemon, 'Pool', Pool)
        self.mock(daemon, 'QueueReader', QueueReader)

        d = daemon.Daemon(conf, 'qork_daemon')
        self.assertEquals(conf, d.global_conf)
        self.assertEquals(conf['qork_daemon'], d.conf)
        self.assertEquals(conf['qork_daemon']['visibility_timeout'], d.vtime)
        self.assertEquals(conf['qork_daemon']['interval'], d.interval)

    def test_get_worker_class(self):
        conf = {
            'worker-some_worker': {
                'class': 'some_module.some_class',
            },
        }

        class Daemon(daemon.Daemon):
            def __init__(self, conf):
                self.global_conf = conf
                self.logger = None

        class Message(object):
            def __init__(self):
                self.body = {'worker_type': 'some_worker'}

            def handle_exception(self):
                pass

        d = Daemon(conf)
        m = Message()

        class Worker(object):
            @classmethod
            def from_message(cls, conf, conf_section, logger, message):
                self.assertEquals(d.global_conf, conf)
                self.assertEquals(m, message)
                return Worker()

        def mock_import(name, fromlist=None):
            self.assertEquals(name, 'some_module')
            self.assertEquals(fromlist, ['some_module'])
            return 'a_module'

        def mock_getattr(object, name):
            self.assertEquals(object, 'a_module')
            self.assertEquals(name, 'some_class')
            return Worker

        self.mock(daemon, '__import__', mock_import)
        self.mock(daemon, 'getattr', mock_getattr)

        worker = d.get_worker_class(m)
        self.assertEquals(Worker, worker)

    def test_get_worker_class_exception(self):
        class Message(object):
            def __init__(self, test):
                self.test = test
                self.body = {'worker_type': 'some_worker'}
                self.handle_exception_called = 0

            def handle_exception(self):
                (type, value, traceback) = sys.exc_info()
                self.test.assertEquals(RuntimeError, type)
                self.handle_exception_called += 1

        class Daemon(daemon.Daemon):
            def __init__(self):
                self.global_conf = {}
                self.logger = util.DumbLogger()

        d = Daemon()
        m = Message(self)
        d.get_worker_class(m)
        self.assertEquals(1, m.handle_exception_called)

    def test_get_worker_class_handle_exception(self):
        class Daemon(daemon.Daemon):
            def __init__(self):
                self.global_conf = {}
                self.logger = util.DumbLogger()

        class Message(object):
            def __init__(self):
                self.handle_exception_called = 0
                self.body = {'worker_type': 'some_worker'}

            def handle_exception(self):
                self.handle_exception_called += 1

        d = Daemon()
        message = Message()
        worker = d.get_worker_class(message)
        self.assertEquals(worker, None)
        self.assertEquals(1, message.handle_exception_called)
