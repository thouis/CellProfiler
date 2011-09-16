# Test program simulating the GUI process for multiprocessing

import os
import sys
import threading
import readline
import time

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

        # pipe for command input
        self.commands_pipe_r, self.commands_pipe_w = os.pipe()

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
        # we do this in a separate thread to allow asynchronous output
        try:
            while True:
                os.write(self.commands_pipe_w, raw_input("> "))
        except:
            os.write(self.commands_pipe_w, "quit")

    def write_output(self):
        while True:
            # wait for feedback
            msg = self.gui_feedback.recv()
            # overwrite current typing
            sys.stdout.write("\r  " + (' ' * len(readline.get_line_buffer())))
            # write line
            sys.stdout.write("\rMSG from Manager: >%s<\n" % msg)
            # redisplay prompt
            readline.redisplay()

    def process_commands(self):
        while True:
            command = os.read(self.commands_pipe_r, 1024)
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
                    print "bad command", command
                    pass

    def send_quit(self):
        try:
            self.gui_commands.send("quit", flags=zmq.NOBLOCK)
        except:
            pass  # couldn't send quit, probably because the manager went away

    def send_interrupt(self, command):
        try:
            self.gui_commands.send(command, flags=zmq.NOBLOCK)
        except:
            pass  # XXX - server probably died

    def send_jobs(self, n):
        try:
            self.gui_commands.send("%d" % n, flags=zmq.NOBLOCK)
        except:
            pass  # XXX - server probably died

if __name__ == '__main__':
    TestGUI().run()
