import time
import sys
import os
import traceback
import pdb
import StringIO

import zmq

import util

class Worker(object):
    def __init__(self, ports):
        theworker = self
        jobs_port = ports['jobs']
        results_port = ports['results']
        control_port = ports['control']
        exceptions_port = ports['exceptions']

        # set up ZMQ sockets
        context = zmq.Context()

        def connect_port(socket_type, port):
            s = context.socket(socket_type)
            s.connect("tcp://127.0.0.1:%d" % port)
            return s

        print "Worker connecting to %s" % ((jobs_port, results_port, control_port, exceptions_port),)
        # Connect to Manager
        self.jobs = connect_port(zmq.REQ, jobs_port)
        self.results = connect_port(zmq.PUSH, results_port)
        self.control = connect_port(zmq.SUB, control_port)
        self.exceptions = connect_port(zmq.REQ, exceptions_port)

        # watch every message on control
        self.control.setsockopt(zmq.SUBSCRIBE, '')

        while True:
            # These are outside the try/except, because if something
            # goes wrong communicating with the manager, there's
            # probably not much we can do other than give up.
            self.jobs.send("work, please")
            work = self.jobs.recv()
            try:
                result = self.do_work(int(work))
            except:
                result = 'exception'  # in case of exception, return a null result.
                self.handle_exception()
            self.results.send_multipart([work, "%s" % (result,)])

    def do_work(self, amount):
        if amount == 0:
            self.check_control()
            # keepalive signal
            return 0
        for count in range(amount):
            time.sleep(1)
            kontinue = self.check_control()
            if not kontinue:
                return 'interrupted'
        return amount ** 2

    def check_control(self):
        if zmq.select([self.control], [], [], timeout=0.1)[0] == []:
            return True  # no control messages, proceed
        msg = self.control.recv()
        if msg == 'interrupt':
            return False
        raise ValueError('Worker received unknown control message: %s' % msg)

    def handle_exception(self):
        # We report the exception and traceback to the manager, and if
        # it replies with "debug", we start a pdb.post_mortem() on the
        # traceback, connected to a pair of debugging sockets.
        exc_info = self.exc_info = sys.exc_info()
        # anticipate being requested to be debugged
        pdb = ZMQPdb()
        self.exceptions.send_multipart([str(exc_info), str(pdb.port_in), str(pdb.port_out)])
        disposition = self.exceptions.recv()
        if disposition == 'debug':
            # See pdb.post_mortem()
            pdb.reset()
            pdb.interaction(None, exc_info[2])
        # see the work loop above, where we'll send a null response
        # after the exception is handled, and hopefully go back to
        # work.

class ZMQPdb(pdb.Pdb):
    def __init__(self):
        # create ports
        context = zmq.Context()
        self.sock_in = pdbsock_in = context.socket(zmq.PULL)
        self.sock_out = pdbsock_out = context.socket(zmq.PUSH)
        self.port_in = pdbsock_in.bind_to_random_port('tcp://127.0.0.1')
        self.port_out = pdbsock_out.bind_to_random_port('tcp://127.0.0.1')

        # create pipes for pdb input/output
        self.stdin_r, self.stdin_w = [os.fdopen(fd, m, 0) for fd, m in zip(os.pipe(), ['r', 'w'])]
        self.stdout_r, self.stdout_w = [os.fdopen(fd, m, 0) for fd, m in zip(os.pipe(), ['r', 'w'])]

        # init underlying pdb instance
        pdb.Pdb.__init__(self, stdin=self.stdin_r, stdout=self.stdout_w)
        self.prompt = ''

        # create async communication threads
        self.stop = False
        self.stdin_thread = util.socket_to_pipe_thread(self.sock_in, self.stdin_w, lambda: self.stop)
        self.stdout_thread = util.pipe_to_socket_thread(self.stdout_r, self.sock_out, lambda: self.stop)

        self.sock_out.setsockopt(zmq.LINGER, 0)  # allow shutdown with output waiting

    def interaction(self, *args):
        # start async communication threads
        self.stdin_thread.start()
        self.stdout_thread.start()
        pdb.Pdb.interaction(self, *args)
        self.stop = True
        self.sock_out.send('<detaching>')

def worker(ports):
    w = Worker(ports)
