# Test program simulating the GUI process for multiprocessing

import os
import sys
import threading
import readline
import time
import Queue

import zmq
import multiprocessing

import manager

class TestGUI(object):
    def __init__(self):
        # zmq setup
        context = zmq.Context()
        self.gui_commands = context.socket(zmq.PUSH)
        self.gui_feedback = context.socket(zmq.PULL)
        commands_port = self.gui_commands.bind_to_random_port("tcp://127.0.0.1")
        feedback_port = self.gui_feedback.bind_to_random_port("tcp://127.0.0.1")

        # start manager
        self.m = multiprocessing.Process(target=manager.manager, args=(commands_port, feedback_port))
        self.m.start()

        # queues for command input and manager output
        self.input_queue = Queue.Queue()
        self.output_queue = Queue.Queue()

    def run(self):
        input_thread = threading.Thread(target=self.read_commands)
        output_thread = threading.Thread(target=self.write_output)
        commands_thread = threading.Thread(target=self.process_commands)
        input_thread.daemon = True  # we exit when the commands thread does
        output_thread.daemon = True
        input_thread.start()
        output_thread.start()
        commands_thread.start()

        # run until commands thread ends
        commands_thread.join()
        self.m.terminate()

    def read_commands(self):
        # start the wxconsole
        import wxcon
        self.output_queue.put("Type quit to exit")
        wxcon.start_console(self.input_queue, self.output_queue)

    def write_output(self):
        while True:
            # wait for feedback, post to console
            msg = self.gui_feedback.recv()
            self.output_queue.put(msg)

    def process_commands(self):
        while True:
            command = str(self.input_queue.get())
            # commands are:
            # N - submit N jobs
            # interrupt - stop current jobs
            # quit - kill subprocesses and exit
            if command == 'quit':
                self.send_quit()
                time.sleep(1)
                return
            elif command in ['interrupt', 'forcefail']:
                self.send_interrupt(command)
            else:
                try:
                    n = int(command)
                    self.send_jobs(n)
                except:
                    self.output_queue.put(" bad command: " + command)
                    pass

    def send_quit(self):
        try:
            self.gui_commands.send("quit", flags=zmq.NOBLOCK)
        except:
            print "failed to send quit", e
            pass  # couldn't send quit, probably because the manager went away

    def send_interrupt(self, command):
        try:
            self.gui_commands.send(command, flags=zmq.NOBLOCK)
        except Exception, e:
            print "failed to send interrupt", e
            pass  # XXX - server probably died

    def send_jobs(self, n):
        try:
            self.gui_commands.send("%d" % n, flags=zmq.NOBLOCK)
        except:
            print "failed to send jobs", e
            pass  # XXX - server probably died

if __name__ == '__main__':
    TestGUI().run()
