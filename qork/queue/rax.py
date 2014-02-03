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

from eventlet import Timeout
from datetime import datetime
from httplib import urlsplit, HTTPConnection, HTTPSConnection
from json import loads, dumps
import re
import sys
import traceback
from uuid import uuid4

import qork.queue.base as base
from qork.utils import list_from_csv


MAX_GRACE = 43200


def make_http_request(
        method, url, headers, body, timeout, acceptable_statuses, attempts):
    scheme, netloc, path, query, fragment = urlsplit(url)
    if scheme == 'https':
        Connection = HTTPSConnection
    else:
        Connection = HTTPConnection

    for attempt in xrange(attempts):
        try:
            with Timeout(timeout):
                conn = Connection(netloc)
                conn.request(method, '%s?%s' % (path, query), body, headers)
                resp = conn.getresponse()
                if resp.status in acceptable_statuses or \
                        resp.status // 100 in acceptable_statuses:
                    return resp
        except Exception as e:
            if attempt >= attempts - 1:
                raise e
        except Timeout:
            if attempt >= attempts - 1:
                raise RuntimeError('Request to %s timed out.' % (url))

    raise RuntimeError('Bad mojo')


class RAXConnection(object):
    def __init__(self, conf):
        self._conf = conf
        self.identity_end_point = conf['identity_end_point']
        self.username = conf['username']
        self.api_key = conf['api_key']
        self._auth_token = None
        self.end_point = conf['end_point']
        self.timeout = int(conf.get('timeout', 10))

    @property
    def auth_token(self):
        if self._auth_token == None:
            self.get_auth_token()
        return self._auth_token

    def create_queue(self, name):
        path = 'queues/%s' % (name)
        self.make_request('PUT', path, {}, '', (2,), 3)
        return RAXQueue(self, name)

    def get_all_queues(self, prefix=None):
        path = 'queues'
        marker = None
        limit = 100

        queues = set()

        while True:
            params = 'limit=%d' % (limit)
            if marker:
                params += '&marker=%s' % (marker)
            this_request = '%s?%s' % (path, params)

            resp = self.make_request(
                'GET', this_request, {}, '', (2,), 3)
            if resp.status == 204:
                break
            data = loads(resp.read())

            if prefix:
                queues.update(
                    [str(x['name']) for x in data['queues'] if
                    x['name'].startswith(prefix)])
            else:
                queues.update([str(x['name']) for x in data['queues']])

            if len(data['queues']) < limit:
                break

            marker = data['queues'][-1]['name']

        return queues

    def get_auth_token(self):
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'auth': {
                'RAX-KSKEY:apiKeyCredentials': {
                    'username': self.username,
                    'apiKey': self.api_key,
                }
            }
        }

        body = dumps(data)
        resp = make_http_request(
            'POST', self.identity_end_point, headers, body, 1, (2,), 3)

        data = loads(resp.read())
        self._auth_token = data['access']['token']['id']

    def get_queue_stats(self, name):
        path = 'queues/%s/stats' % (name)
        resp = self.make_request('GET', path, {}, '', (2,), 3)
        return loads(resp.read())

    def make_request(
            self, method, path, headers, body, acceptable_statuses,
            attempts):

        url = '%s/%s' % (self.end_point, path)
        headers = dict(headers)
        acceptable_statuses = list(acceptable_statuses) + [401,]

        for attempt in xrange(2):
            # TODO: what is a good value for client id?  maybe specify it in
            # the config?
            headers['Client-ID'] = 'e58668fc-26eb-11e3-8270-5b3128d43830'
            headers['X-Auth-Token'] = self.auth_token
            resp = make_http_request(method, url, headers, body, self.timeout,
                acceptable_statuses, attempts)

            if resp.status == 401:
                self.get_auth_token()
                continue
            return resp


class RAXMessage(object):
    def __init__(self, data):
        match = re.search('messages/(\w+)', data['href'])
        if not match:
            raise RuntimeError('url does not match expected pattern')
        self.id = match.group(1)
        match = re.search('claim_id=(\w+)', data['href'])
        if match:
            self.claim_id = match.group(1)
        else:
            self.claim_id = None
        self.body = data['body']

    def get_body(self):
        return self.body


class RAXQueue(object):
    def __init__(self, conn, name):
        self._conn = conn
        self.name = name

    # number of unclaimed messages in queue
    def count(self):
        stats = self._conn.get_queue_stats(self.name)
        return stats['messages']['free']

    def delete(self):
        path = 'queues/%s' % (self.name)
        self._conn.make_request('DELETE', path, {}, '', (2,), 3)

    def delete_message(self, message):
        path = 'queues/%s/messages/%s' % (
            self.name, message.id)
        if message.claim_id:
            path += '?claim_id=%s' % (message.claim_id)
        self._conn.make_request('DELETE', path, {}, '', (2,), 3)

    # get/claim next message
    def read(self, vtime):
        path = 'queues/%s/claims?limit=1' % (self.name)
        # TODO: is this value ok for grace?
        # 60 is the minimum value for ttl, grace
        vtime = max(vtime, 60)
        data = {
            'ttl': vtime,
            'grace': MAX_GRACE,
        }
        resp = self._conn.make_request(
            'POST', path, {}, dumps(data), (2,), 3)
        if resp.status == 204:
            return None
        messages = loads(resp.read())
        return RAXMessage(messages[0])

    # get/read all unclaimed messages in queue without claiming them
    def read_messages(self):
        path = 'queues/%s/messages' % (self.name)
        marker = None
        limit = 10

        while True:
            params = 'echo=true&limit=%d' % (limit)
            if marker:
                params += '&marker=%s' % (marker)
            this_request = '%s?%s' % (path, params)

            resp = self._conn.make_request(
                'GET', this_request, {}, '', (2,), 3)
            if resp.status == 204:
                break

            data = loads(resp.read())

            for message in data['messages']:
                yield RAXMessage(message)

            next_link = None
            for link in data['links']:
                if link['rel'] == 'next':
                    next_link = link['href']
            if not next_link:
                break

            match = re.search('marker=(\w+)', next_link)
            if not match:
                raise RuntimeError('url does not match expected pattern')
            marker = match.group(1)

    def write(self, message):
        url = 'queues/%s/messages' % (self.name)
        data = [
            {
                'ttl': base.FOURTEEN_DAYS,
                'body': message,
            }
        ]
        self._conn.make_request('POST', url, {}, dumps(data), (2,), 3)


class QueueReader(base.QueueReader):
    """Reads messages from work queues, respecting priority"""

    def __init__(self, conf):
        super(QueueReader, self).__init__(conf)
        self._conn = RAXConnection(conf)

    def get_queues(self, include_failure_queues=False):
        """Returns queues in priority order"""
        for prefix in self._queue_prefixes:
            failure_queue = None
            for queue in sorted(self._conn.get_all_queues(prefix)):
                if queue.endswith('_failure'):
                    failure_queue = queue
                    continue
                yield MessageQueue(self._conf, queue)
            if include_failure_queues and failure_queue:
                yield MessageQueue(self._conf, failure_queue)


class QueueWriter(base.QueueWriter):
    """Writes messages to multiple queues"""

    def send_message(self, queue_name, message):
        if not queue_name in self._queues:
            raise ValueError('Unexpected queue name')

        queue_name = '%s_%s' % (self._global_prefix, queue_name)
        queue = MessageQueue(self._conf, queue_name)
        queue.send_message(message)


class MessageQueue(base.MessageQueue):
    """Work queue, a RAX queue wrapper"""

    # name convention is [queue_name]_[timestamp] or [queue_name]

    def __init__(self, conf, name):
        super(MessageQueue, self).__init__(conf, name)

        conn = RAXConnection(conf)
        self._queue = conn.create_queue(self.name)
        self._vtime = base.get_vtime(conf)

    def delete(self):
        self._queue.delete()

    def delete_message(self, message):
        """Delete RAX message"""
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
        for message in self._queue.read_messages():
            yield Message(self._conf, self, message)

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

        self._queue.write(msg)


class Message(base.Message):
    """Work message, a RAX message wrapper"""

    def delete(self):
        """Deletes message from queue"""
        self._queue.delete_message(self._message)

    def get_body(self):
        return self._message.get_body()
