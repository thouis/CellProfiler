import zmq
import threading
import fcntl  # I don't think this will work on Windows
import os

def socket_to_pipe(socket, pipe, stop):
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    while not stop():
        if poller.poll(100):  # ms
            msg = socket.recv()
            pipe.write(msg)
            pipe.flush()

def socket_to_pipe_thread(*args):
    return threading.Thread(target=socket_to_pipe, args=args)

def pipe_to_socket(pipe, socket, stop):
    # need to set nonblocking
    flags = fcntl.fcntl(pipe.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(pipe.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
    poller = zmq.Poller()
    poller.register(pipe.fileno(), zmq.POLLIN)
    while not stop():
        if poller.poll(100):  # ms
            msg = pipe.read()
            socket.send(msg)

def pipe_to_socket_thread(*args):
    return threading.Thread(target=pipe_to_socket, args=args)
