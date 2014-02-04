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

import qork.queue.base as base
from qork.utils import list_from_csv


class QueueReader(base.QueueReader):
    """Reads messages from work queues, respecting priority"""

    def __init__(self, conf):
        super(QueueReader, self).__init__(conf)
        self._conn = SQSConnection(
            conf['access_key'], conf['secret_access_key'])

    def get_all_queues(self):
        """Returns all queues with global prefix"""
        for queue in sorted(self._conn.get_all_queues(
                self._global_prefix), key=lambda x: x.name):
            yield MessageQueue(self._conf, queue.name)

    def get_queues(self):
        """Returns read_queues in priority order"""
        for prefix in self._read_queues:
            for queue in sorted(
                    self._conn.get_all_queues(prefix), key=lambda x: x.name):
                if queue.name.endswith('_failure'):
                    continue
                yield MessageQueue(self._conf, queue.name)


class QueueWriter(base.QueueWriter):
    """Writes messages to multiple queues"""

    def send_message(self, queue_name, msg):
        if not queue_name in self._queues:
            raise ValueError('Unexpected queue name')

        queue_name = '%s_%s' % (self._global_prefix, queue_name)
        queue = MessageQueue(self._conf, queue_name)
        queue.send_message(msg)


class MessageQueue(base.MessageQueue):
    """Work queue, a SQS wrapper"""

    # name convention is [queue_name]_[timestamp] or [queue_name]

    def __init__(self, conf, name):
        super(MessageQueue, self).__init__(conf, name)

        conn = SQSConnection(
            conf['access_key'], conf['secret_access_key'])
        self._queue = conn.create_queue(self.name)
        self._queue.set_attribute(
            'MessageRetentionPeriod', base.FOURTEEN_DAYS)
        self._vtime = base.get_vtime(conf)

    def delete(self):
        self._queue.delete()

    def delete_message(self, message):
        """Delete SQS message"""
        self._queue.delete_message(message)

    def get_message(self):
        """Returns a message"""
        message = self._queue.read(self._vtime)
        if message:
            return Message(self._conf, self, message)
        return None

    def message_count(self):
        """Returns number of messages in queue"""
        return self._queue.count()

    def read_messages(self):
        """Yields each message in queue once"""
        vtime = 1
        seen = set()
        message = self._queue.read(vtime)
        while message:
            if message.id in seen:
                vtime += 1
            else:
                seen.add(message.id)
                yield Message(self._conf, self, message)
            message = self._queue.read(vtime)

    def send_failure(self, msg):
        """Puts message in queue's failure queue"""
        queue = MessageQueue(self._conf, self._failure_queue_name)
        queue.send_message(msg)

    def send_restore(self, msg):
        """Puts message from failure queue back in work queue"""
        queue = MessageQueue(self._conf, msg['meta']['queue_name'])
        queue.send_message(msg)

    def send_message(self, msg):
        """Sends message to queue"""
        if type(msg) != dict:
            raise ValueError("Message must be a dict()")

        if not('body' in msg and 'meta' in msg):
            msg = {'meta': {}, 'body': msg}
            msg['meta']['message_id'] = str(uuid4())
            msg['meta']['queue_name'] = self.name
            msg['meta']['timestamp'] = base.timestamp()

        message = SQSMessage()
        message.set_body(dumps(msg, default=lambda x: str(x)))
        if not self._queue.write(message):
            raise RuntimeError("Writing message failed")


class Message(base.Message):
    """Work message, an SQS message wrapper"""

    def delete(self):
        """Deletes message from queue"""
        self._queue.delete_message(self._message)

    def get_body(self):
        return self._message.get_body()
