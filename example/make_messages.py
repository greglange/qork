#!/usr/bin/env python

from sys import argv, exit

from swift.common import utils
from qork import queue

def make_queue_name(conf, name, postfix=None):
    queue_name = '%s_%s' % (conf['global_prefix'], name)
    if postfix:
        queue_name += '_%s' % (postfix)
    return queue_name


if __name__ == '__main__':
    if len(argv) != 2:
        print '%s CONF_FILE' % (argv[0])
        exit()

    conf = utils.readconf(argv[1], 'qork')

    data = [
        ['add', 7, 12, None],
        ['add', 8, 19, None],
        ['subtract', 7, 12, None],
        ['subtract', 8, 19, None],
        ['multiply', 5, 8, '2011'],
        ['multiply', 3, 4, '2012'],
        ['multiply', 5, 6, '2013'],
        ['divide', 8, 4, None],
        ['divide', 10, 0, None],
    ]

    for i, d in enumerate(data):
        queue_name = make_queue_name(conf, d[0], d[3])
        q = queue.MessageQueue(conf['sqs_access_key'],
            conf['sqs_secret_access_key'], queue_name)

        body = {
            'worker_type': d[0],
            'result_file': '/tmp/worker%d.result' % (i),
            'x': d[1],
            'y': d[2],
        }
        q.send_message(body)
