"""MapReduce framework Worker node."""
import os
import logging
import json
import socket
import time
import click
import mapreduce.utils
import threading

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Worker host=%s port=%s manager_host=%s, manager_port=%s pwd=%s",
            host, port, manager_host, manager_port, os.getcwd(),
        )
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.workers = {} 
        self.signals = {"shutdown": False}
        self.threads = []

        TCPThread = threading.Thread(target=self.createTCPServer, args=(host, port))
        self.threads.append(TCPThread)

        LOGGER.info("Start TCP server thread")
        TCPThread.start()

        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((manager_host, manager_port))
            # send a message
            message = json.dumps({"message_type": "register", "worker_host": host, "worker_port": port}, indent=2)
            LOGGER.debug(
                "TCP send to %s:%s \n%s", manager_host, manager_port, message
            )
            LOGGER.info(
                "Sent connection request to Manager %s:%s", manager_host, manager_port
            )
            sock.sendall(message.encode('utf-8'))

        for thread in self.threads:
            thread.join()


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

                    if message_dict['message_type'] == 'register_ack':
                        UDPThread = threading.Thread(target=self.sendHeartbeat, args=(host, port))
                        self.threads.append(UDPThread)

                        LOGGER.info("Starting heartbeat thread")
                        UDPThread.start()
                    elif message_dict['message_type'] == 'shutdown':
                        self.signals['shutdown'] = True

                except json.JSONDecodeError:
                    continue
                
    
    def sendHeartbeat(self, host, port):
        """Test UDP Socket Client."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect(("localhost", 6000))
            # Send a message
            while not self.signals['shutdown']:
                message = json.dumps({"message_type": "heartbeat", "worker_host": host, "worker_port": port})
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)


    def startJob(self, input_dir, output_dir, map_exe, reduc_exe, num_map, num_reduc):
        """Send Job registration to manager"""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((self.manager_host, self.manager_port))
            # send a message
            message = json.dumps({
                "message_type": "new_manager_job", 
                "input_directory": input_dir, 
                "output_directory": output_dir,
                "mapper_executable": map_exe,
                "reducer_executable": reduc_exe,
                "num_mappers" : num_map,
                "num_reducers" : num_reduc
            }, indent=2)
            LOGGER.debug(
                "TCP send to %s:%s \n%s", self.manager_host, self.manager_port, message
            )
            LOGGER.info(
                "Sent connection request to Manager %s:%s", self.manager_host, self.manager_port
            )
            sock.sendall(message.encode('utf-8'))


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
