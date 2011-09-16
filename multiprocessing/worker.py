import time
import sys
import traceback

import zmq

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
                result = 0  # in case of exception, return a null result.
                self.handle_exception()
            self.results.send("%s" % result)

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
        self.exceptions.send_multipart([str(exc_info)])
        disposition = self.exceptions.recv()
        if disposition == 'debug':
            # What we should do here: Create a pdb.Pdb instance and a
            # separate thread that feeds data between pipes connected
            # to the Pdb instance and a REQ/REP pair linked to the
            # Manager or GUI.  The postcmd() method of the Pdb
            # instance should signal the feeder thread to read from
            # the Pdb stdout and send the result on the socket.  See
            # the implementation of pdb.post_mortem() for how to use
            # the Pdb instance.
            pass
        # see the work loop above, where we'll send a null response
        # after the exception is handled, and hopefully go back to
        # work.

def worker(ports):
     w = Worker(ports)
