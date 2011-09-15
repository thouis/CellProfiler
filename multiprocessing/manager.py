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
        workers = [multiprocessing.Process(target=worker.worker, args=(jobs_port, results_port, control_port, exceptions_port)) for n in range(count)]
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
        print "GUI COMMAND", msg
        if msg[0] == 'quit':
            self.loop.stop()
        elif msg[0] == 'interrupt':
            self.control.send(interrupt, flags=zmq.NOBLOCK)
        else:
            # number of jobs to queue
            for i in range(int(msg[0])):
                self.jobs_waiting.append("%d" % (i + 1))
            self.feed_workers()

    def on_job_request(self, msg):
        print "JOB REQ", msg
        self.workers_waiting.append(msg[-2])
        self.feed_workers()

    def feed_workers(self):
        while self.workers_waiting and self.jobs_waiting:
            worker = self.workers_waiting.pop(0)
            job = self.jobs_waiting.pop(0)
            self.jobs.send_multipart(worker + [job])
            self.gui_feedback.send("sent job %s to %s" % (job, worker))

    def on_results(self, msg):
        print "RESULTS", msg

    def on_exceptions(self, msg):
        print "EXCEPTION", msg

    def keep_alive(self):
        print "KEEP ALIVE", leb(self.workers_waiting)
        for worker in self.workers_waiting:
            self.jobs.send_multipart(worker + ['0'])
        self.workers_waiting = []

def manager(commands_port, feedback_port):
    m = Manager(commands_port, feedback_port)  # it just runs
