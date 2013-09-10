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

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message as SQSMessage
from datetime import datetime
from json import loads, dumps
import re
import sys
import traceback
from uuid import uuid4


FOURTEEN_DAYS = 14 * 24 * 60 * 60


def timestamp():  # pragma: no cover
    return datetime.today().strftime("%Y-%m-%d %H:%M:%S")


class QueueReader(object):
    """Reads messages from work queues, respecting priority"""

    def __init__(self, access_key, secret_access_key, global_prefix,
                 queue_prefixes, max_failure_count=10):
        self._queue_prefixes = ['%s_%s' % (global_prefix, x) for x in
                                queue_prefixes]
        self._access_key = access_key
        self._secret_access_key = secret_access_key
        self._conn = SQSConnection(access_key, secret_access_key)
        self._max_failure_count = int(max_failure_count)

    def get_message(self, vtime):
        """Returns the next message from work queues"""
        for queue in self.get_queues():
            message = queue.get_message(vtime)
            if message:
                return message
        return None

    def get_queues(self, include_failure_queues=False):
        """Returns queues in priority order"""
        for prefix in self._queue_prefixes:
            failure_queue = None
            for sqs_queue in sorted(self._conn.get_all_queues(prefix),
                                    key=lambda x: x.name):
                if sqs_queue.name.endswith('_failure'):
                    failure_queue = sqs_queue.name
                    continue
                yield MessageQueue(self._access_key, self._secret_access_key,
                                   sqs_queue.name,
                                   max_failure_count=self._max_failure_count)
            if include_failure_queues and failure_queue:
                yield MessageQueue(self._access_key, self._secret_access_key,
                                   failure_queue,
                                   max_failure_count=self._max_failure_count)


class QueueWriter(object):
    """Writes messages to multiple queues"""

    def __init__(self, access_key, secret_access_key, global_prefix, queues):
        self._access_key = access_key
        self._secret_access_key = secret_access_key
        self._global_prefix = global_prefix
        self._queues = queues

    def send_message(self, queue_name, msg):
        if not queue_name in self._queues:
            raise ValueError('Unexpected queue name')

        queue_name = '%s_%s' % (self._global_prefix, queue_name)
        queue = MessageQueue(
            self._access_key, self._secret_access_key, queue_name)
        queue.send_message(msg)


class MessageQueue(object):
    """Work queue, a SQS wrapper"""

    # name convention is [queue_name]_[timestamp] or [queue_name]

    def __init__(self, access_key, secret_access_key, name, sqs_queue=None,
                 max_failure_count=10):
        self._access_key = access_key
        self._secret_access_key = secret_access_key
        self._conn = SQSConnection(self._access_key, self._secret_access_key)
        self.name = name
        if re.search('_[0-9]+$', self.name):
            self._failure_queue_name = '%s_failure' % \
                (self.name.rsplit('_', 1)[0])
        elif self.name.endswith('_failure'):
            self._failure_queue_name = None
        else:
            self._failure_queue_name = '%s_failure' % (self.name)
        if sqs_queue:
            if self.name != sqs_queue.name:
                raise ValueError('Queue names do not match')
            self._sqs_queue = sqs_queue
        else:
            self._sqs_queue = self._conn.create_queue(self.name)
            self._sqs_queue.set_attribute(
                'MessageRetentionPeriod', FOURTEEN_DAYS)
        self._max_failure_count = int(max_failure_count)

    def delete(self):
        self._sqs_queue.delete()

    def delete_message(self, sqs_message):
        """Delete SQS message"""
        self._sqs_queue.delete_message(sqs_message)

    def get_message(self, vtime):
        """Returns a message"""
        sqs_message = self._sqs_queue.read(vtime)
        if sqs_message:
            return Message(self, sqs_message, self._max_failure_count)
        return None

    def message_count(self):
        """Returns number of messages in queue"""
        return self._sqs_queue.count()

    def read_messages(self):
        """Yields each message in queue once"""
        vtime = 1
        seen = set()
        sqs_message = self._sqs_queue.read(vtime)
        while sqs_message:
            if sqs_message.id in seen:
                vtime += 1
            else:
                seen.add(sqs_message.id)
                yield Message(self, sqs_message)
            sqs_message = self._sqs_queue.read(vtime)

    def search_messages(self, meta, body):
        """Yields messages that match search dicts"""
        for message in self.read_messages():
            if message.matches(meta, body):
                yield message

    def send_failure(self, msg):
        """Puts message in queue's failure queue"""
        queue = MessageQueue(self._access_key, self._secret_access_key,
                             self._failure_queue_name)
        queue.send_message(msg)

    def send_restore(self, msg):
        """Puts message from failure queue back in work queue"""
        queue = MessageQueue(self._access_key, self._secret_access_key,
                             msg['meta']['queue_name'])
        queue.send_message(msg)

    def send_message(self, msg):
        """Sends message to queue"""
        if type(msg) != dict:
            raise ValueError("Message must be a dict()")

        if not('body' in msg and 'meta' in msg):
            msg = {'meta': {}, 'body': msg}
            msg['meta']['message_id'] = str(uuid4())
            msg['meta']['queue_name'] = self.name
            msg['meta']['timestamp'] = timestamp()

        sqs_message = SQSMessage()
        sqs_message.set_body(dumps(msg, default=lambda x: str(x)))
        if not self._sqs_queue.write(sqs_message):
            raise RuntimeError("Writing message failed")


class Message(object):
    """Work message, an SQS message wrapper"""

    def __init__(self, queue, sqs_message, max_failure_count=10):
        self._queue = queue
        self._sqs_message = sqs_message
        self._max_failure_count = int(max_failure_count)
        self._msg = None

    def __str__(self):  # pragma: no cover
        """Returns a well formatted string version of message"""
        lines = []
        lines.append('[ Message ID: %s ]' % (self.meta['message_id']))
        lines.append('--- meta ---')
        for key, value in sorted(self.meta.iteritems()):
            if key in ['exceptions', 'message_id']:
                continue
            lines.append('%s: %s' % (key, value))
        if 'exceptions' in self.meta:
            lines.append('--- exceptions ---')
            for row in self.meta.get('exceptions', []):
                lines.append('%s(%s)' % (row[0],
                             ''.join(['%s,' % x for x in row[1]])))
                lines.extend(row[2])
        lines.append('--- body ---')
        for key, value in sorted(self.body.iteritems()):
            if key == 'meta':
                continue
            lines.append('%s: %s' % (key, value))
        return '\n'.join(lines)

    def delete(self):
        """Deletes message from queue"""
        self._queue.delete_message(self._sqs_message)

    @property
    def body(self):
        """Returns the body of the message"""
        return self.msg['body']

    def _get_exception(self):
        """Gets exception for storage/future reference"""
        cla, exc, trbk = sys.exc_info()
        excName = cla.__name__
        if exc.args:
            args = exc.args
        else:
            args = ()
        excTb = traceback.format_tb(trbk)
        return [excName, args, excTb]

    def handle_exception(self):
        """Log exception and possibly move message to failure queue"""
        self.meta['failure_count'] = self.meta.get('failure_count', 0) + 1
        if not 'exceptions' in self.meta:
            self.meta['exceptions'] = []
        self.meta['exceptions'].append(self._get_exception())
        if self.meta['failure_count'] < self._max_failure_count:
            self._queue.send_message(self.msg)
        else:
            self._queue.send_failure(self.msg)
        self.delete()

    @property
    def meta(self):
        """Returns a message's meta information"""
        return self.msg['meta']

    def matches(self, meta, body):
        """Returns True if message matches search dicts"""
        for key in meta:
            if key == 'begin_timestamp':
                if self.meta['timestamp'] < meta[key]:
                    return False
            elif key == 'end_timestamp':
                if self.meta['timestamp'] >= meta[key]:
                    return False
            elif not (key in self.meta and self.meta[key] == meta[key]):
                return False
        for key in body:
            if not (key in self.body and self.body[key] == body[key]):
                return False
        return True

    @property
    def msg(self):
        """Returns a message's msg dict"""
        if self._msg is None:
            self._msg = loads(self._sqs_message.get_body())
        return self._msg

    def restore(self):
        """Move a failed message back to work queue"""
        msg = {
            'meta': {
                'message_id': self.meta['message_id'],
                'queue_name': self.meta['queue_name'],
                'timestamp': self.meta['timestamp'],
            },
            'body': self.body,
        }
        self._queue.send_restore(msg)
        self.delete()
