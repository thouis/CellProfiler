import os
import time

import zmq
from zmq.eventloop import ioloop, zmqstream
import multiprocessing

import worker

class Manager(object):
    def __init__(self, commands_port, feedback_port):
        # set up ZMQ sockets
        context = zmq.Context()

        # Connect to GUI
        print "Manager connecting to %s and %s" % (commands_port, feedback_port)
        self.gui_commands = context.socket(zmq.PULL)
        self.gui_commands.connect("tcp://127.0.0.1:%d" % commands_port)
        self.gui_feedback = context.socket(zmq.PUSH)
        self.gui_feedback.connect("tcp://127.0.0.1:%d" % feedback_port)

        # Create worker-facing sockets
        def socket_random_port(socket_type):
            s = context.socket(socket_type)
            p = s.bind_to_random_port('tcp://127.0.0.1')
            return s, p
        self.jobs, jobs_port = socket_random_port(zmq.ROUTER)
        self.results, results_port = socket_random_port(zmq.PULL)
        self.control, control_port = socket_random_port(zmq.PUB)
        self.exceptions, exceptions_port = socket_random_port(zmq.ROUTER)

        # start workers - XXX move to separate function to handle restarting when needed
        try:
            count = multiprocessing.cpu_count()
        except NotImplementedError:
            count = 2
        ports = {'jobs' : jobs_port,
                 'results' : results_port,
                 'control' : control_port,
                 'exceptions' : exceptions_port}
        workers = [multiprocessing.Process(target=worker.worker, args=(ports,)) for n in range(count)]
        for w in workers:
            w.daemon = True  # autokill when parent exits
            w.start()

        # setup the handlers - use ZMQStreams because we want to call
        # the hander for every message.  see
        # http://lists.zeromq.org/pipermail/zeromq-dev/2011-May/011509.html
        self.loop = loop = ioloop.IOLoop()
        for socket, handler in zip([self.gui_commands, self.jobs, self.results, self.exceptions],
                                   [self.on_gui_command, self.on_job_request, self.on_results, self.on_exceptions]):
            zmqstream.ZMQStream(socket, loop).on_recv(handler)

        # Keep workers alive.  They die if starved more than a few minutes
        ioloop.PeriodicCallback(self.keep_alive, 5 * 1000, loop).start()

        # workers waiting
        self.workers_waiting = []

        # jobs waiting
        self.jobs_waiting = []

        # serve
        loop.start()

    def on_gui_command(self, msg):
        msg = msg[0]
        print "GUI COMMAND", msg
        if msg == 'quit':
            self.loop.stop()
        elif msg == 'interrupt':
            print "inter"
            self.control.send('interrupt')
            self.jobs_waiting = []
        elif msg == 'forcefail':
            self.control.send('forcefail', flags=zmq.NOBLOCK)
        else:
            # number of jobs to queue
            for i in range(int(msg)):
                self.jobs_waiting.append("%d" % (i + 1))
            self.feed_workers()

    def on_job_request(self, msg):
        self.workers_waiting.append(msg[:-1])
        self.feed_workers()

    def feed_workers(self):
        while self.workers_waiting and self.jobs_waiting:
            worker = self.workers_waiting.pop(0)
            job = self.jobs_waiting.pop(0)
            self.jobs.send_multipart(worker + [job])
            self.gui_feedback.send("sent job %s to %s" % (job, worker))

    def on_results(self, msg):
        if msg[0] != '0':
            self.gui_feedback.send("Result job %s : %s" % (msg[0], msg[1]))

    def on_exceptions(self, msg):
        worker = msg[:msg.index(b'')]
        msg = msg[msg.index(b'') + 1:]
        # XXX - For now, always kick workers into debug mode.  It's
        # not that costly, it leaves them in a reasonable state, and
        # sending the "quit" command is sufficient to start them up
        # again.
        self.exceptions.send_multipart(worker + [b'', 'debug'])
        # forward information to gui, let it deal with the worker as it will.
        self.gui_feedback.send('exception')
        self.gui_feedback.send_multipart(msg)

    def keep_alive(self):
        for worker in self.workers_waiting:
            self.jobs.send_multipart(worker + ['0'])
        self.workers_waiting = []

def manager(commands_port, feedback_port):
    m = Manager(commands_port, feedback_port)  # it just runs
