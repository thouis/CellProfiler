import time

import zmq

class Worker(object):
    def __init__(self, jobs_port, results_port, control_port, exceptions_port):
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

        while True:
            # for now, just request and reply
            self.jobs.send("work, please")
            work = self.jobs.recv()
            print "got work", work
            self.results.send("%d" % int(work) ** 2)

def worker(*args):
    w = Worker(*args)
