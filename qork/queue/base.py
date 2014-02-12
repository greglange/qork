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

from datetime import datetime
from json import loads, dumps
import re
import sys
import traceback
from uuid import uuid4

from qork.utils import list_from_csv


FOURTEEN_DAYS = 14 * 24 * 60 * 60


def get_vtime(conf):
    return int(conf.get('vtime', 30*60))


def get_max_failure_count(conf):
    return int(conf.get('max_failure_count', 10))


def timestamp():  # pragma: no cover
    return datetime.today().strftime("%Y-%m-%d %H:%M:%S")


class QueueReader(object):
    """Reads messages from work queues, respecting priority"""

    def __init__(self, conf):
        self._conf = conf
        self._global_prefix = conf['global_prefix']
        self._read_queues = [
            '%s_%s' % (conf['global_prefix'], x) for x in
            list_from_csv(conf['read_queues'])]
        self._write_queues = [
            '%s_%s' % (conf['global_prefix'], x) for x in
            list_from_csv(conf['write_queues'])]

    def get_message(self):
        """Returns the next message from work queues"""
        for queue in self.get_queues():
            message = queue.get_message()
            if message:
                return message
        return None

    def get_all_queues(self):
        """Returns all queues with global prefix"""
        raise NotImplementedError()

    def get_queues(self):
        """Returns read queues in priority order"""
        raise NotImplementedError()


class QueueWriter(object):
    """Writes messages to multiple queues"""

    def __init__(self, conf):
        self._conf = conf
        self._global_prefix = conf['global_prefix']
        self._queues = list_from_csv(conf['write_queues'])

    def send_message(self, queue_name, msg):
        raise NotImplementedError()


class MessageQueue(object):
    """Work queue, a SQS wrapper"""

    # name convention is [queue_name]_[timestamp] or [queue_name]

    def __init__(self, conf, name):
        self._conf = conf
        self.name = name
        if re.search('_[0-9]+$', self.name):
            self._failure_queue_name = '%s_failure' % \
                (self.name.rsplit('_', 1)[0])
        elif self.name.endswith('_failure'):
            self._failure_queue_name = None
        else:
            self._failure_queue_name = '%s_failure' % (self.name)

    def delete(self):
        """Deletes the queue"""
        raise NotImplementedError()

    def delete_message(self, message):
        """Delete SQS message"""
        raise NotImplementedError()

    def get_message(self):
        """Returns a message"""
        raise NotImplementedError()

    def message_count(self):
        """Returns number of messages in queue"""
        raise NotImplementedError()

    def read_messages(self):
        """Yields each message in queue once"""
        raise NotImplementedError()

    def search_messages(self, meta, body):
        """Yields messages that match search dicts"""
        for message in self.read_messages():
            if message.matches(meta, body):
                yield message

    def send_failure(self, msg):
        """Puts message in queue's failure queue"""
        raise NotImplementedError()

    def send_restore(self, msg):
        """Puts message from failure queue back in work queue"""
        raise NotImplementedError()

    def send_message(self, msg):
        """Sends message to queue"""
        raise NotImplementedError()


class Message(object):
    """Work message, an SQS message wrapper"""

    def __init__(self, conf, queue, message):
        self._conf = conf
        self._queue = queue
        self._message = message
        self._max_failure_count = get_max_failure_count(conf)
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
        raise NotImplementedError()

    @property
    def body(self):
        """Returns the body of the message"""
        return self.msg['body']

    def get_body(self):
        """Deletes message from queue"""
        raise NotImplementedError()

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
            body = self.get_body()
            if isinstance(body, str):
                self._msg = loads(self.get_body())
            elif isinstance(body, dict):
                self._msg = body
            else:
                raise RuntimeError('Unexected body type')
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
