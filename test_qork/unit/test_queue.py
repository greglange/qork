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

import json
import random
import uuid

from qork import queue
import util


class SQSMessage(object):
    def __init__(self):
        self.id = uuid.uuid4()


class SQSQueue(object):
    def __init__(self, name):
        self.name = name

    def set_attribute(self, *args, **kwargs):
        pass


class SQSConnection(object):
    def __init__(self, test, access_key, secret_access_key):
        self.test = test
        self.access_key = access_key
        self.secret_access_key = secret_access_key

    def method(self, access_key, secret_access_key):
        self.test.assertEquals(self.access_key, access_key)
        self.test.assertEquals(self.secret_access_key,
                               secret_access_key)
        return self

    def create_queue(self, name):
        return SQSQueue(name)


class TestQueueReader(util.MockerTestCase):

    def test_init(self):
        access_key = 'some_key'
        secret_access_key = 'some_other_key'
        global_prefix = 'test'
        queue_prefixes = 'one two three'.split()

        sqs_connection = SQSConnection(self, access_key, secret_access_key)

        self.mock(queue, 'SQSConnection', sqs_connection.method)

        qr = queue.QueueReader(access_key, secret_access_key, global_prefix,
                               queue_prefixes)
        self.assertEquals(['%s_%s' % (global_prefix, x) for x in
                          queue_prefixes], qr._queue_prefixes)
        self.assertEquals(access_key, qr._access_key)
        self.assertEquals(secret_access_key, qr._secret_access_key)

    def test_get_message(self):
        class MessageQueue(object):
            def __init__(self, test, vtime, messages):
                self.test = test
                self.vtime = vtime
                self.messages = messages.split()

            def get_message(self, vtime):
                if self.messages:
                    return self.messages.pop(0)
                return None

        class QueueReader(queue.QueueReader):
            def __init__(self, test, vtime):
                self.queues = [
                    MessageQueue(test, vtime, '1 2 3'),
                    MessageQueue(test, vtime, ''),
                    MessageQueue(test, vtime, '4'),
                    MessageQueue(test, vtime, '5 6')
                ]

            def get_queues(self):
                return self.queues

        vtime = 'some_vtime'
        msgs = []
        qr = QueueReader(self, vtime)
        m = qr.get_message(vtime)
        while m:
            msgs.append(m)
            m = qr.get_message(vtime)

        self.assertEquals('1 2 3 4 5 6'.split(), msgs)

    def test_get_queues(self):
        class MessageQueue(object):
            def __init__(self, access_key, secret_access_key, name,
                         sqs_queue=None, max_failure_count=10):
                self.access_key = access_key
                self.secret_access_key = secret_access_key
                self.name = name
                self.sqs_queue = sqs_queue
                self._max_failure_count = max_failure_count

        class Connection(object):
            def __init__(self, queues):
                self.queues = queues

            def get_all_queues(self, prefix):
                if prefix in self.queues:
                    qs = [SQSQueue('%s_%s' % (prefix, x)) for x in
                          self.queues[prefix]]
                    random.shuffle(qs)
                    return qs
                else:
                    return []

        class QueueReader(queue.QueueReader):
            def __init__(self, access_key, secret_access_key, queue_prefixes,
                         queues):
                self._access_key = access_key
                self._secret_access_key = secret_access_key
                self._queue_prefixes = queue_prefixes
                self._conn = Connection(queues)
                self._max_failure_count = None

        access_key = 'some_key'
        secret_access_key = 'some_secret_key'
        queue_prefixes = 'one two three four'.split()
        queues = {
            'one': '1 2 3 4 failure'.split(),
            'two': ''.split(),
            'three': '01 02 03 04 05 06 07 08'.split(),
            'four': '20110101 20110510 failure'.split(),
        }

        self.mock(queue, 'MessageQueue', MessageQueue)

        # without failure queues
        qr = QueueReader(access_key, secret_access_key, queue_prefixes, queues)
        queue_list = []
        for p in queue_prefixes:
            for q in queues[p]:
                if q == 'failure':
                    continue
                queue_list.append('%s_%s' % (p, q))
        for q in qr.get_queues():
            expected = queue_list.pop(0)
            self.assertEquals(expected, q.name)
        self.assertEquals(0, len(queue_list))

        # with failure queues
        qr = QueueReader(access_key, secret_access_key, queue_prefixes, queues)
        queue_list = []
        for p in queue_prefixes:
            for q in queues[p]:
                queue_list.append('%s_%s' % (p, q))
        for q in qr.get_queues(True):
            expected = queue_list.pop(0)
            self.assertEquals(expected, q.name)
        self.assertEquals(0, len(queue_list))


class TestMessageQueue(util.MockerTestCase):

    def test_init(self):
        access_key = 'some_key'
        secret_access_key = 'some_secret_key'

        sqs_connection = SQSConnection(self, access_key, secret_access_key)
        self.mock(queue, 'SQSConnection', sqs_connection.method)

        data = [
            ['some_name', 'some_name_failure', None],
            ['some_name_20111008', 'some_name_failure', None],
            ['some_name_failure', None, None],
            ['some_name', 'some_name_failure', SQSQueue('some_name')],
            ['some_name_20111008', 'some_name_failure',
             SQSQueue('some_name_20111008')],
            ['some_name_failure', None, SQSQueue('some_name_failure')],
        ]

        for d in data:
            q = queue.MessageQueue(access_key, secret_access_key, d[0], d[2])
            self.assertEquals(access_key, q._access_key)
            self.assertEquals(secret_access_key, q._secret_access_key)
            self.assertEquals(SQSConnection, type(q._conn))
            self.assertEquals(SQSQueue, type(q._sqs_queue))
            self.assertEquals(d[0], q._sqs_queue.name)
            self.assertEquals(d[1], q._failure_queue_name)

        self.assertRaises(ValueError, queue.MessageQueue, access_key,
                          secret_access_key, 'some_name',
                          SQSQueue('some_other_name'))

    def test_delete(self):
        class SQSQueue(object):
            def __init__(self):
                self.delete_called = 0

            def delete(self):
                self.delete_called += 1

        class MessageQueue(queue.MessageQueue):
            def __init__(self):
                self._sqs_queue = SQSQueue()

        q = MessageQueue()
        q.delete()
        self.assertEquals(q._sqs_queue.delete_called, 1)

    def test_delete_message(self):
        class SQSQueue(object):
            def __init__(self, test, sqs_message):
                self.test = test
                self.sqs_message = sqs_message
                self.delete_message_called = 0

            def delete_message(self, sqs_message):
                self.test.assertEquals(self.sqs_message, sqs_message)
                self.delete_message_called += 1

        class MessageQueue(queue.MessageQueue):
            def __init__(self, test, sqs_message):
                self._sqs_queue = SQSQueue(test, sqs_message)

        sqs_message = SQSMessage()
        q = MessageQueue(self, sqs_message)
        q.delete_message(sqs_message)
        self.assertEquals(1, q._sqs_queue.delete_message_called)

    def test_get_message(self):
        class Message(object):
            def __init__(self, test, queue, sqs_message, max_failure_count):
                self.test = test
                self.queue = queue
                self.sqs_message = sqs_message
                self.max_failure_count = max_failure_count

            def method(self, queue, sqs_message, max_failure_count):
                self.test.assertEquals(self.queue, queue)
                self.test.assertEquals(self.sqs_message, sqs_message)
                self.test.assertEquals(self.max_failure_count,
                                       max_failure_count)
                return self

        class SQSQueue(object):
            def __init__(self, test, vtime, message):
                self.test = test
                self.vtime = vtime
                self.message = message

            def read(self, vtime):
                self.test.assertEquals(self.vtime, vtime)
                return self.message

        class MessageQueue(queue.MessageQueue):
            def __init__(self, sqs_queue, max_failure_count):
                self._sqs_queue = sqs_queue
                self._max_failure_count = max_failure_count

        vtime = 'some_vtime'

        # message returned
        sqs_message = SQSMessage()
        sqs_queue = SQSQueue(self, vtime, sqs_message)
        max_failure_count = 'some_count'
        q = MessageQueue(sqs_queue, max_failure_count)
        message = Message(self, q, sqs_message, max_failure_count)
        self.mock(queue, 'Message', message.method)
        message_returned = q.get_message(vtime)
        self.assertEquals(message, message_returned)

        # message not returned
        sqs_queue = SQSQueue(self, vtime, None)
        q = MessageQueue(sqs_queue, max_failure_count)
        message_returned = q.get_message(vtime)
        self.assertEquals(None, message_returned)

    def test_message_count(self):
        class SQSQueue(object):
            def __init__(self, message_count):
                self.message_count = message_count
                self.count_called = 0

            def count(self):
                self.count_called += 1
                return self.message_count

        class MessageQueue(queue.MessageQueue):
            def __init__(self, message_count):
                self._sqs_queue = SQSQueue(message_count)

        for c in [0, 1, 2, 4, 8, 16]:
            q = MessageQueue(c)
            self.assertEquals(c, q.message_count())
            self.assertEquals(1, q._sqs_queue.count_called)

    def test_read_message(self):
        class Message(object):
            def __init__(self, queue, sqs_message):
                self.queue = queue
                self.sqs_message = sqs_message

        class SQSQueue(object):
            def __init__(self, test, messages):
                self.test = test
                self.messages = messages
                self.vtime = 1
                self.seen = set()

            def read(self, vtime):
                if not self.messages:
                    return None
                message = self.messages.pop(0)
                self.test.assertEquals(self.vtime, vtime)
                if message in self.seen:
                    self.vtime += 1
                self.seen.add(message)
                return message

        class MessageQueue(queue.MessageQueue):
            def __init__(self, sqs_queue):
                self._sqs_queue = sqs_queue

        self.mock(queue, 'Message', Message)

        msg1 = SQSMessage()
        msg2 = SQSMessage()
        msg3 = SQSMessage()

        messages = [SQSMessage(), msg1, msg1, SQSMessage(), msg2, msg3, msg2,
                    SQSMessage(), SQSMessage(), SQSMessage(), msg3, msg3,
                    msg3, msg3, ]

        unique_messages = set(messages)

        sqs_queue = SQSQueue(self, messages)
        q = MessageQueue(sqs_queue)

        for message in q.read_messages():
            self.assertEquals(q, message.queue)
            self.assert_(message.sqs_message in unique_messages)
            unique_messages.remove(message.sqs_message)

    def test_search_message(self):
        class Message(object):
            def __init__(self, test, meta, body, match):
                self.test = test
                self.meta = meta
                self.body = body
                self.match = match

            def matches(self, meta, body):
                self.test.assertEquals(self.meta, meta)
                self.test.assertEquals(self.body, body)
                return self.match

        class MessageQueue(queue.MessageQueue):
            def __init__(self, messages):
                self.messages = messages

            def read_messages(self):
                for message in self.messages:
                    yield message

        meta = 'some_meta'
        body = 'some_body'

        messages = [
            Message(self, meta, body, True),
            Message(self, meta, body, True),
            Message(self, meta, body, False),
            Message(self, meta, body, False),
            Message(self, meta, body, True),
            Message(self, meta, body, False),
            Message(self, meta, body, True),
        ]

        q = MessageQueue(messages)

        for message in q.search_messages(meta, body):
            self.assert_(message in messages)
            self.assertEquals(True, message.match)

    def test_send_failure(self):
        class MessageQueue(queue.MessageQueue):
            def __init__(self, test, msg, access_key, secret_access_key,
                         failure_queue_name, sqs_queue=None):
                self.test = test
                self.msg = msg
                self._access_key = access_key
                self._secret_access_key = secret_access_key
                self._failure_queue_name = failure_queue_name
                self.sqs_queue = sqs_queue
                self.send_message_called = 0

            def method(self, access_key, secret_access_key, name,
                       sqs_queue=None):
                self.test.assertEquals(self._access_key, access_key)
                self.test.assertEquals(self._secret_access_key,
                                       secret_access_key)
                self.test.assertEquals(self._failure_queue_name, name)
                self.test.assertEquals(self.sqs_queue, sqs_queue)
                return self

            def send_message(self, msg):
                self.test.assertEquals(self.msg, msg)
                self.send_message_called += 1

        msg = {'some_key': 'some_value'}
        q = MessageQueue(self, msg, 'some_key', 'some_secret_key',
                         'failed_messages')

        self.mock(queue, 'MessageQueue', q.method)
        q.send_failure(msg)
        self.assertEquals(1, q.send_message_called)

    def test_send_restore(self):
        class MessageQueue(queue.MessageQueue):
            def __init__(self, test, msg, access_key, secret_access_key,
                         restore_queue_name, sqs_queue=None):
                self.test = test
                self.msg = msg
                self._access_key = access_key
                self._secret_access_key = secret_access_key
                self.restore_queue_name = restore_queue_name
                self.sqs_queue = sqs_queue
                self.send_message_called = 0

            def method(self, access_key, secret_access_key, name,
                       sqs_queue=None):
                self.test.assertEquals(self._access_key, access_key)
                self.test.assertEquals(self._secret_access_key,
                                       secret_access_key)
                self.test.assertEquals(self.restore_queue_name, name)
                self.test.assertEquals(self.sqs_queue, sqs_queue)
                return self

            def send_message(self, msg):
                self.test.assertEquals(self.msg, msg)
                self.send_message_called += 1

        msg = {'meta': {'queue_name': 'some_queue'}}
        q = MessageQueue(self, msg, 'some_key', 'some_secret_key',
                         msg['meta']['queue_name'])

        self.mock(queue, 'MessageQueue', q.method)

        q.send_restore(msg)
        self.assertEquals(1, q.send_message_called)

    def test_send_message_value_error(self):
        class MessageQueue(queue.MessageQueue):
            def __init__(self):
                pass

        q = MessageQueue()
        self.assertRaises(ValueError, q.send_message, list())

    def test_send_message(self):
        def uuid4():
            return 'some_uuid4_key'

        def timestamp():
            return 'some_timestamp_value'

        class SQSMessage(object):
            def __init__(self, test, meta, body):
                self.test = test
                self.meta = meta
                self.body = body

            def method(self):
                return self

            def set_body(self, content):
                msg = json.loads(content)

                meta = dict([[str(x), str(y)] for x, y in
                            self.meta.iteritems()])
                self.test.assertEquals(self.meta, meta)

                body = dict([[str(x), str(y)] for x, y in
                            self.body.iteritems()])
                self.test.assertEquals(self.body, body)

                self.test.assertEquals(2, len(msg.keys()))

        class SQSQueue(object):
            def __init__(self, test, sqs_message):
                self.test = test
                self.sqs_message = sqs_message

            def write(self, sqs_message):
                self.test.assertEquals(self.sqs_message, sqs_message)
                return True

        class MessageQueue(queue.MessageQueue):
            def __init__(self, test, name, sqs_message):
                self.name = name
                self._sqs_queue = SQSQueue(test, sqs_message)

        queue_name = 'some_queue_name'

        # no body, meta
        msg = {'some_key': 'some_value'}
        meta = {'message_id': uuid4(), 'timestamp': timestamp(),
                'queue_name': queue_name}
        sqs_message = SQSMessage(self, meta, msg)

        self.mock(queue, 'SQSMessage', sqs_message.method)
        self.mock(queue, 'uuid4', uuid4)
        self.mock(queue, 'timestamp', timestamp)

        q = MessageQueue(self, 'some_queue', sqs_message)
        q.send_message(msg)

        self.mock.restore()

        # body and meta

        msg = {'meta': {'some_meta_key': 'some_value'},
               'body': {'some_body_key': 'some_value'}}

        sqs_message = SQSMessage(self, msg['meta'], msg['body'])

        self.mock(queue, 'SQSMessage', sqs_message.method)
        self.mock(queue, 'uuid4', uuid4)
        self.mock(queue, 'timestamp', timestamp)

        q = MessageQueue(self, 'some_queue', sqs_message)
        q.send_message(msg)

    def test_send_message_write_failure(self):
        class SQSMessage(object):
            def set_body(self, content):
                pass

        class SQSQueue(object):
            def write(self, message):
                return False

        class MessageQueue(queue.MessageQueue):
            def __init__(self):
                self.name = 'some_name'
                self._sqs_queue = SQSQueue()

        self.mock(queue, 'SQSMessage', SQSMessage)

        q = MessageQueue()
        self.assertRaises(RuntimeError, q.send_message, {})


class TestMessage(util.MockerTestCase):

    def test_init(self):
        q, sqs_message = 'queue', 'sqs_message'
        m = queue.Message(q, sqs_message)
        self.assertEquals(q, m._queue)
        self.assertEquals(sqs_message, m._sqs_message)
        self.assertEquals(None, m._msg)
        self.assertEquals(10, m._max_failure_count)

    def test_delete(self):
        class MessageQueue(object):
            def __init__(self, test, sqs_message):
                self.test = test
                self.sqs_message = sqs_message
                self.delete_called = 0

            def delete_message(self, sqs_message):
                self.delete_called += 1
                self.test.assertEquals(self.sqs_message, sqs_message)

        class Message(queue.Message):
            def __init__(self, test):
                self._sqs_message = SQSMessage()
                self._queue = MessageQueue(test, self._sqs_message)

        m = Message(self)
        m.delete()
        self.assertEquals(1, m._queue.delete_called)

    def test_body(self):
        class Message(queue.Message):
            def __init__(self, msg):
                self._msg = msg

            @property
            def msg(self):
                return self._msg

        msg = {'body': {'some_key': 'some_value'}}
        m = Message(msg)
        self.assertEquals(msg['body'], m.body)

    def test_get_exception(self):
        class Message(queue.Message):
            def __init__(self):
                pass

        m = Message()

        try:
            raise Exception()
        except Exception:
            ret = m._get_exception()

        self.assertEquals('Exception', ret[0])
        self.assertEquals((), ret[1])
        self.assertEquals(list, type(ret[2]))
        self.assert_(len(ret[2]) > 0)
        for line in ret[2]:
            self.assertEquals(str, type(line))

        e_arg = 'some text'
        try:
            raise Exception(e_arg)
        except:
            ret = m._get_exception()

        self.assertEquals('Exception', ret[0])
        self.assertEquals((e_arg,), ret[1])
        self.assertEquals(list, type(ret[2]))
        self.assert_(len(ret[2]) > 0)
        for line in ret[2]:
            self.assertEquals(str, type(line))

    def test_handle_exception(self):
        class MessageQueue(object):
            def __init__(self, test, msg):
                self.test = test
                self.msg = msg
                self.send_message_called = 0
                self.send_failure_called = 0

            def send_message(self, msg):
                self.send_message_called += 1
                self.test.assertEquals(self.msg, msg)

            def send_failure(self, msg):
                self.send_failure_called += 1
                self.test.assertEquals(self.msg, msg)

        class Message(queue.Message):
            def __init__(self, queue, msg, exc):
                self._msg = msg
                self._queue = queue
                self.exc = exc
                self._max_failure_count = 10
                self.delete_called = 0
                self.get_exception_called = 0

            def delete(self):
                self.delete_called += 1

            def _get_exception(self):
                self.get_exception_called += 1
                return self.exc

        # message retry, send_message called
        msg = {'meta': {}, 'body': {'some_key': 'some_value'}}
        q = MessageQueue(self, msg)
        exc = "some_exception"
        m = Message(q, msg, exc)
        m.handle_exception()
        self.assertEquals(1, q.send_message_called)
        self.assertEquals(0, q.send_failure_called)
        self.assertEquals(1, msg['meta']['failure_count'])
        self.assertEquals(1, len(msg['meta']['exceptions']))

        # message over max failures, send_failure called
        msg = {
            'meta': {'exceptions': ['some_exception'] * 10,
                     'failure_count': 10},
            'body': {'some_key': 'some_value'}}
        q = MessageQueue(self, msg)
        exc = "some_exception"
        m = Message(q, msg, exc)
        m.handle_exception()
        self.assertEquals(0, q.send_message_called)
        self.assertEquals(1, q.send_failure_called)
        self.assertEquals(11, msg['meta']['failure_count'])
        self.assertEquals(11, len(msg['meta']['exceptions']))

    def test_meta(self):
        class Message(queue.Message):
            def __init__(self, msg):
                self._msg = msg

            @property
            def msg(self):
                return self._msg

        msg = {'meta': {'some_key': 'some_value'}}
        m = Message(msg)
        self.assertEquals(msg['meta'], m.meta)

    def test_matches(self):
        class Message(queue.Message):
            def __init__(self, meta, body):
                self._msg = {'meta': meta, 'body': body}

        # no match keys
        m = Message({}, {})
        self.assertEquals(True, m.matches({}, {}))

        # a meta match key
        m = Message({'some_key': 'some_value'}, {})
        self.assertEquals(True, m.matches({'some_key': 'some_value'}, {}))
        self.assertEquals(False, m.matches({'some_key': 'some_other_value'},
                          {}))

        # a body match key
        m = Message({}, {'some_key': 'some_value'})
        self.assertEquals(True, m.matches({}, {'some_key': 'some_value'}))
        self.assertEquals(False, m.matches({},
                          {'some_key': 'some_other_value'}))

        # meta key that doesn't exist in message
        m = Message({}, {})
        self.assertEquals(False, m.matches({'some_key': 'some_value'}, {}))

        # body key that doesn't exist in message
        m = Message({}, {})
        self.assertEquals(False, m.matches({}, {'some_key': 'some_value'}))

        # timestamp range [ . . . .
        m = Message({'timestamp': '2010-06-12 12:25:10'}, {})
        meta = {'begin_timestamp': '2010-06-12 12:25:10'}
        self.assertEquals(True, m.matches(meta, {}))
        meta = {'begin_timestamp': '2009-06-12 12:25:10'}
        self.assertEquals(True, m.matches(meta, {}))
        meta = {'begin_timestamp': '2011-06-12 12:25:10'}
        self.assertEquals(False, m.matches(meta, {}))

        # timestamp range . . . . ]
        m = Message({'timestamp': '2010-06-12 12:25:10'}, {})
        meta = {'end_timestamp': '2010-06-12 12:25:10'}
        self.assertEquals(False, m.matches(meta, {}))
        meta = {'end_timestamp': '2009-06-12 12:25:10'}
        self.assertEquals(False, m.matches(meta, {}))
        meta = {'end_timestamp': '2011-06-12 12:25:10'}
        self.assertEquals(True, m.matches(meta, {}))

        # timestamp range [ . . . . ]
        m = Message({'timestamp': '2010-06-12 12:25:10'}, {})
        meta = {'begin_timestamp': '2010-06-12 12:25:10',
                'end_timestamp': '2011-06-12 12:25:10'}
        self.assertEquals(True, m.matches(meta, {}))
        meta = {'begin_timestamp': '2009-06-12 12:25:10',
                'end_timestamp': '2011-06-12 12:25:10'}
        self.assertEquals(True, m.matches(meta, {}))
        meta = {'begin_timestamp': '2009-06-12 12:25:10',
                'end_timestamp': '2010-06-12 12:25:10'}
        self.assertEquals(False, m.matches(meta, {}))
        meta = {'begin_timestamp': '2008-06-12 12:25:10',
                'end_timestamp': '2009-06-12 12:25:10'}
        self.assertEquals(False, m.matches(meta, {}))
        meta = {'begin_timestamp': '2011-06-12 12:25:10',
                'end_timestamp': '2012-06-12 12:25:10'}
        self.assertEquals(False, m.matches(meta, {}))

    def test_msg(self):
        class SQSMessage(object):
            def __init__(self, return_value):
                self.return_value = return_value
                self.get_body_called = 0

            def get_body(self):
                self.get_body_called += 1
                return self.return_value

        class Message(queue.Message):
            def __init__(self, msg, sqs_message):
                self._msg = msg
                self._sqs_message = sqs_message

        class LoadS(object):
            def __init__(self, test, arg, return_value):
                self.test = test
                self.arg = arg
                self.return_value = return_value
                self.called = 0

            def method(self, arg):
                self.called += 1
                self.test.assertEquals(self.arg, arg)
                return self.return_value

        arg = 'some_arg'
        return_value = 'some_return_value'
        loads = LoadS(self, arg, return_value)
        sqs_message = SQSMessage(arg)
        m = Message(None, sqs_message)

        self.mock(queue, 'loads', loads.method)

        # verify msg comes from the right place
        self.assertEquals(return_value, m.msg)
        self.assertEquals(1, loads.called)
        self.assertEquals(1, sqs_message.get_body_called)

        # verify msg is cached
        self.assertEquals(return_value, m.msg)
        self.assertEquals(1, loads.called)
        self.assertEquals(1, sqs_message.get_body_called)

    def test_restore(self):
        class MessageQueue(object):
            def __init__(self, test, msg):
                self.test = test
                self.msg = msg
                self.send_restore_called = 0

            def send_restore(self, msg):
                self.send_restore_called += 1
                self.test.assertEquals(self.msg, msg)

        class Message(queue.Message):
            def __init__(self, queue, msg):
                self._queue = queue
                self._msg = msg
                self.delete_called = 0

            def delete(self):
                self.delete_called += 1

        msg = {
            'body': {'some_key': 'some_value'},
            'meta': {
                'message_id': 'some_id',
                'queue_name': 'some_queue',
                'timestamp': 'some_timestamp',
            }
        }

        exp_msg = {'meta': {}, 'body': msg['body']}
        for key in "message_id queue_name timestamp".split():
            exp_msg['meta'][key] = msg['meta'][key]

        q = MessageQueue(self, exp_msg)
        m = Message(q, msg)

        m.restore()
        self.assertEquals(1, q.send_restore_called)
        self.assertEquals(1, m.delete_called)
