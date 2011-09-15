# Test program simulating the GUI process for multiprocessing

import os
import sys
import threading
import readline
import time

import zmq
import multiprocessing

import manager

def read_commands():
    # we do this in a separate thread to allow asynchronous output
    try:
        while True:
            os.write(commands_pipe_w, raw_input("> "))
    except:
        os.write(commands_pipe_w, "quit")

def write_output():
    while True:
        # wait for feedback
        msg = gui_feedback.recv()
        # overwrite current typing
        sys.stdout.write("\r  " + (' ' * len(readline.get_line_buffer())))
        # write line
        sys.stdout.write("\rMSG from Manager: >%s<\n" % msg)
        # redisplay prompt
        readline.redisplay()

def process_commands():
    while True:
        command = os.read(commands_pipe_r, 1024)
        # commands are:
        # N - submit N jobs
        # interrupt - stop current jobs
        # quit - kill subprocesses and exit
        if command == 'quit':
            send_quit()
            return
        elif command == 'interrupt':
            send_interrupt()
        else:
            try:
                n = int(command)
                send_jobs(n)
            except:
                print "bad command", command
                pass

def send_quit():
    gui_commands.send("quit")

def send_interrupt():
    gui_commands.send("interrupt")

def send_jobs(n):
    gui_commands.send("%d" % n)


if __name__ == '__main__':
    # Queue to communicate to manager
    q = multiprocessing.Queue()

    # start manager
    m = multiprocessing.Process(target=manager.manager, args=(q,))
    m.daemon = True  # autokill when parent exits
    m.start()

    # zmq setup
    context = zmq.Context()
    gui_commands = context.socket(zmq.PUSH)
    commands_port = gui_commands.bind_to_random_port("tcp://127.0.0.1")
    gui_feedback = context.socket(zmq.PULL)
    feedback_port = gui_feedback.bind_to_random_port("tcp://127.0.0.1")

    # tell the Manager where to bind, then hangup
    print "GUI ports:", commands_port, feedback_port
    q.put((commands_port, feedback_port))

    # pipe for command input
    commands_pipe_r, commands_pipe_w = os.pipe()

    input_thread = threading.Thread(target=read_commands)
    output_thread = threading.Thread(target=write_output)
    commands_thread = threading.Thread(target=process_commands)
    input_thread.daemon = True  # we exit when the commands thread does
    output_thread.daemon = True
    input_thread.start()
    output_thread.start()
    commands_thread.start()

    # run until commands thread ends
    commands_thread.join()
