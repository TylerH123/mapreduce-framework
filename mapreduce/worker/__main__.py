"""MapReduce framework Worker node."""
import click
import hashlib
import heapq
import json
import logging
import mapreduce.utils
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import contextlib

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

        TCPThread = threading.Thread(
            target=self.createTCPServer, args=(host, port))
        self.threads.append(TCPThread)

        LOGGER.info("Start TCP server thread")
        TCPThread.start()

        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((manager_host, manager_port))
            # send a message
            message = json.dumps(
                {"message_type": "register", "worker_host": host, "worker_port": port}, indent=2)
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
                    LOGGER.info(
                        "TCP recv \n%s", json.dumps(message_dict, indent=2)
                    )

                    if message_dict['message_type'] == 'register_ack':
                        UDPThread = threading.Thread(
                            target=self.sendHeartbeat, args=(host, port))
                        self.threads.append(UDPThread)

                        LOGGER.info("Starting heartbeat thread")
                        UDPThread.start()
                        
                    elif message_dict['message_type'] == 'new_map_task':
                        map_task = threading.Thread(
                            target=self.startMapTask, args=(message_dict,))
                        self.threads.append(map_task)

                        LOGGER.info("Starting mapper thread")
                        map_task.start()
                        map_task.join()
                        
                    elif message_dict['message_type'] == 'new_reduce_task':
                        reduce_task = threading.Thread(
                            target=self.startReduceTask, args=(message_dict,))
                        self.threads.append(reduce_task)

                        LOGGER.info("Starting reduce thread")
                        reduce_task.start()
                        reduce_task.join()
                        
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
                message = json.dumps(
                    {"message_type": "heartbeat", "worker_host": host, "worker_port": port})
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)

    def startMapTask(self, message_dict):
        executable = message_dict['executable']
        input_paths = message_dict['input_paths']

        task_id = message_dict['task_id']
        prefix = f'mapreduce-local-task{task_id}'

        # Temporary directory for map output files
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            for file in input_paths:
                with open(file) as infile:
                    # Run executable on input file and pipe output to memory
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        # Organize matching keys to same files for reduce
                        for line in map_process.stdout:
                            word = line.split()[0]
                            hexdigest = hashlib.md5(
                                word.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition = keyhash % message_dict['num_partitions']

                            filename = f'maptask{task_id:05d}-part{partition:05d}'
                            filepath = os.path.join(tmpdir, filename)
                            with open(filepath, "a") as f:
                                f.write(line)
            temp_output_path = pathlib.Path(tmpdir)
            temp_output_files = list(temp_output_path.glob('*'))
            # Sort keys within individual files and copy files to output directory
            for file in temp_output_files:
                lines = []
                with open(file, "r") as f:
                    lines = f.readlines()
                    lines.sort()
                output_path = pathlib.Path(message_dict['output_directory'])
                filename = os.path.basename(file).split('/')[-1]
                output_file = os.path.join(output_path, filename)
                LOGGER.debug(output_file)
                with open(output_file, "w") as f:
                    f.writelines(lines)

            message = json.dumps({
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": message_dict['worker_host'],
                "worker_port": message_dict['worker_port']
            })
            self.sendTCPMsg(self.manager_host, self.manager_port, message)
    
    def startReduceTask(self, message_dict):
        input_paths = message_dict['input_paths']

        files = []
        with contextlib.ExitStack() as stack:
            files = [stack.enter_context(open(input)) for input in input_paths]
            # All opened files will automatically be closed at the end of
            # the with statement, even if attempts to open files later
            # in the list raise an exception

            task_id = message_dict['task_id']
            executable = message_dict['executable']
            instream = heapq.merge(*files)

            prefix = f'mapreduce-local-task{task_id}-'
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                file = f'part-{task_id:05d}'
                filepath = os.path.join(tmpdir, file)

                with open(filepath, 'w') as f:
                    with subprocess.Popen(
                        [executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=f,
                    ) as reduce_process:
                        for line in instream:
                            reduce_process.stdin.write(line)
                
                output_path = message_dict['output_directory']
                shutil.move(filepath, output_path)
                LOGGER.info(output_path)

            message = json.dumps({
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": message_dict['worker_host'],
                "worker_port": message_dict['worker_port']
            })
            self.sendTCPMsg(self.manager_host, self.manager_port, message)
    
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
