"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import socket
import threading
import time
import click
import mapreduce.utils
import socket

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(), 
        )

        self.workers = {} 
        self.signals = {"shutdown": False}
        self.threads = []

        TCPThread = threading.Thread(target=self.createTCPServer, args=(host, port)) 
        self.threads.append(TCPThread)
        UDPThread = threading.Thread(target=self.createUDPServer, args=(host, port)) 
        self.threads.append(UDPThread)

        LOGGER.info("Start TCP server thread")
        TCPThread.start()
        LOGGER.info("Start UDP server thread")
        UDPThread.start() 
        
        self.jobs = []
        self.job_id = 0

        TCPThread.join()
        UDPThread.join()


    def createTCPServer(self, host, port): 
        """Test TCP Socket Server."""
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be
        # closed when an exception is raised or control flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            LOGGER.debug(
                "TCP bind %s:%s", host, port
            )
            sock.bind((host, port))
            sock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)
                # Receive data, one chunk at a time.  If recv() times out before we
                # can read a chunk, then go back to the top of the loop and try
                # again.  When the client closes the connection, recv() returns
                # empty data, which breaks out of the loop.  We make a simplifying
                # assumption that the client will always cleanly close the
                # connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                    LOGGER.debug(
                        "TCP recv \n%s", json.dumps(message_dict, indent=2)
                    )

                    if message_dict['message_type'] == 'register':
                        worker_host = message_dict['worker_host']
                        worker_port = message_dict['worker_port']
                        self.workers[(worker_host, worker_port)] = 'alive'
                        message_ack = json.dumps({"message_type": "register_ack", "worker_host": worker_host, "worker_port": worker_port}, indent=2)
                        self.sendTCPMsg(worker_host, worker_port, message_ack)
                    elif message_dict['message_type'] == 'shutdown':
                        self.signals['shutdown'] = True
                        for worker_host, worker_port in self.workers.keys(): 
                            msg = json.dumps({"message_type": "shutdown"})
                            self.sendTCPMsg(worker_host, worker_port, msg)

                except json.JSONDecodeError:
                    continue


    def createUDPServer(self, host, port): 
        """Test UDP Socket Server."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            LOGGER.debug(
                "UDP bind %s:%s", host, port
            )
            sock.bind((host, port))
            sock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                print(message_dict)


    def sendTCPMsg(self, host, port, msg): 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((host, port))
            # send a message
            LOGGER.debug(
                "TCP send to %s:%s \n%s", host, port, msg
            )
            sock.sendall(msg.encode('utf-8'))


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
