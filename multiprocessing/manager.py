import os
import time

import zmq

def manager(q):
    # get the GUI's address
    commands_port, feedback_port = q.get()
    print "Manager binding to %s and %s" % (commands_port, feedback_port)

    # set up ZMQ sockets
    context = zmq.Context()
    gui_commands = context.socket(zmq.PULL)
    gui_commands.connect("tcp://127.0.0.1:%d" % commands_port)
    gui_feedback = context.socket(zmq.PUSH)
    gui_feedback.connect("tcp://127.0.0.1:%d" % feedback_port)

    # for now, echo twice
    while True:
        msg = gui_commands.recv()
        gui_feedback.send(msg)
        time.sleep(1)
        gui_feedback.send(msg)
