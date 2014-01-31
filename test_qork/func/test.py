import sys

from daemonx.daemon import read_config
from qork.worker import Worker
from qork.utils import list_from_csv


class DumbLogger(object):
    def __getattr__(self, n):
        return self.foo

    def foo(self, *a, **kw):
        pass


def get_worker_class(global_conf, message):
    """Returns class of worker needed to do message's work"""
    worker_type = 'worker-%s' % (message.body['worker_type'])
    if worker_type not in global_conf:
        raise RuntimeError("Invalid worker type '%s'" % (worker_type))
    conf = global_conf[worker_type]
    import_target, class_name = conf['class'].rsplit('.', 1)
    module = __import__(import_target, fromlist=[import_target])
    return getattr(module, class_name)


# check command line
if len(sys.argv) < 2:
    sys.exit('%s test.conf queue_service command' % (sys.argv[0]))

# specify aws or rax on the command line
for queue_service in 'aws rax'.split():
    if queue_service in sys.argv:
        break
else:
    sys.exit('aws or rax are only valid queue_service values')

# load config file and get appropriate config section

global_conf = read_config(sys.argv[1])
conf = global_conf['qork-%s' % (queue_service)]

# import correct queue module

queue = __import__(conf['queue_module'], globals(), locals(), [''])

if 'delete' in sys.argv:
    # delete old queues
    qr = queue.QueueReader(conf)
    for q in qr.get_queues(True):
        q.delete()
elif 'write' in sys.argv:
    # construct queue writer and send messages
    qw = queue.QueueWriter(conf)
    for queue in list_from_csv(conf['write_queues']):
        for worker_type in 'do_nothing raise_exception'.split():
            body = {
                'worker_type': worker_type,
            }
            qw.send_message(queue, body)
elif 'print' in sys.argv:
    # read messages from queues and print them
    qr = queue.QueueReader(conf)
    for queue in qr.get_queues(True):
        print '### %s ###' % (queue.name)
        for message in queue.read_messages():
            print message
elif 'count' in sys.argv:
    # print counts of unclaimed messages in queues
    qr = queue.QueueReader(conf)
    for queue in qr.get_queues(True):
        print '%s: %d' % (queue.name, queue.message_count())
elif 'work' in sys.argv:
    # do work on messages in queues
    qr = queue.QueueReader(conf)
    while True:
        message = qr.get_message()
        if not message:
            break
        conf_section = 'worker-%s' % (message.body['worker_type'])
        klass = get_worker_class(global_conf, message)
        klass.run_with_message(
            DumbLogger(), global_conf, conf_section, message)
elif 'restore' in sys.argv:
    # restore from error queue
    qr = queue.QueueReader(conf)
    for queue in qr.get_queues(True):
        if not queue.name.endswith('_failure'):
            continue
        while True:
            message = queue.get_message()
            if not message:
                break
            message.restore()
else:
    # print usage
    print 'usage'
