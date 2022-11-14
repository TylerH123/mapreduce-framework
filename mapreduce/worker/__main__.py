"""MapReduce framework Worker node."""
import hashlib
import heapq
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import contextlib
import click

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

        tcp_thread = threading.Thread(
            target=self.create_tcp_server, args=(host, port))
        self.threads.append(tcp_thread)

        LOGGER.info("Start TCP server thread")
        tcp_thread.start()

        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((manager_host, manager_port))
            # send a message
            msg = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port
            }
            message = json.dumps(msg, indent=2)
            LOGGER.debug(
                "TCP send to %s:%s \n%s", manager_host, manager_port, message
            )
            LOGGER.info(
                "Sent connection request to Manager %s:%s",
                manager_host, manager_port
            )
            sock.sendall(message.encode('utf-8'))

        for thread in self.threads:
            thread.join()

    def create_tcp_server(self, host, port):
        """Test TCP Socket Server."""
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be
        # closed when an exception is raised or control flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock_1:
            # Bind the socket to the server
            sock_1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            LOGGER.debug(
                "TCP bind %s:%s", host, port
            )
            sock_1.bind((host, port))
            sock_1.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock_1.settimeout(1)
            while not self.signals["shutdown"]:
                # Wait for a connection for 1s.
                # The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket = sock_1.accept()[0]
                except socket.timeout:
                    continue
                # Receive data, one chunk at a time.
                # If recv() times out before we can read a chunk,
                # then go back to the top of the loop and try
                # again.
                # When the client closes the connection,
                # recv() returns empty data,
                # which breaks out of the loop. We make a simplifying
                # assumption that the client will always cleanly close the
                # connection.
                clientsocket.settimeout(1)
                with clientsocket:
                    message_chunks_1 = []
                    while True:
                        try:
                            data_1 = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data_1:
                            break
                        message_chunks_1.append(data_1)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks_1)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                    LOGGER.info(
                        "TCP recv \n%s", json.dumps(message_dict, indent=2)
                    )

                    if message_dict['message_type'] == 'register_ack':
                        udp_thread = threading.Thread(
                            target=self.send_heartbeat, args=(host, port))
                        self.threads.append(udp_thread)

                        LOGGER.info("Starting heartbeat thread")
                        udp_thread.start()

                    elif message_dict['message_type'] == 'new_map_task':
                        LOGGER.debug("Received map job")
                        map_task = threading.Thread(
                            target=self.start_map_task, args=(message_dict,))
                        self.threads.append(map_task)

                        LOGGER.info("Starting mapper thread")
                        map_task.start()
                        map_task.join()

                    elif message_dict['message_type'] == 'new_reduce_task':
                        reduce_task = threading.Thread(
                            target=self.start_reduce_task,
                            args=(message_dict,))
                        self.threads.append(reduce_task)

                        LOGGER.info("Starting reduce thread")
                        reduce_task.start()
                        reduce_task.join()

                    elif message_dict['message_type'] == 'shutdown':
                        self.signals['shutdown'] = True

                except json.JSONDecodeError:
                    continue

    def send_heartbeat(self, host, port):
        """Test UDP Socket Client."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((self.manager_host, self.manager_port))
            # Send a message
            while not self.signals['shutdown']:
                message = json.dumps({
                    "message_type": "heartbeat",
                    "worker_host": host,
                    "worker_port": port
                })
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)

    def hash_word(self, word, mod):
        """Hash word."""
        hexdigest = hashlib.md5(
            word.encode("utf-8")).hexdigest()
        keyhash = int(hexdigest, base=16)
        return keyhash % mod

    def start_map_task(self, message_dict):
        """Start map task."""
        task_id = message_dict['task_id']

        # Temporary directory for map output files
        prefix = f'mapreduce-local-task{task_id:05d}-'
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            with contextlib.ExitStack() as stk:
                files = {}
                for file in message_dict['input_paths']:
                    with open(file, encoding='utf-8') as infile:
                        # Run executable on input and pipe output to memory
                        with subprocess.Popen(
                            [message_dict['executable']],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as map_process:
                            # Organize matching keys to same files for reduce
                            for line in map_process.stdout:
                                part = self.hash_word(
                                    line.split('\t')[0],
                                    message_dict['num_partitions'])

                                tmp_name = \
                                    f'maptask{task_id:05d}-part{part:05d}'
                                f_path = os.path.join(tmpdir, tmp_name)
                                if part not in files:
                                    files[part] = stk.enter_context(
                                        open(f_path, "a", encoding='utf-8'))
                                files[part].write(line)

            temp_output_files = list(pathlib.Path(tmpdir).glob('*'))

            self.sort_files(temp_output_files, message_dict)
            self.finish_task(task_id, message_dict)

    def start_reduce_task(self, message_dict):
        """Start reduce task."""
        input_paths = message_dict['input_paths']

        with contextlib.ExitStack() as stk:
            files = []
            for inp in input_paths:
                files.append(stk.enter_context(open(inp, encoding='utf-8')))
            # All opened files will automatically be closed at the end of
            # the with statement, even if attempts to open files later
            # in the list raise an exception

            task_id = message_dict['task_id']
            executable = message_dict['executable']
            instream = heapq.merge(*files)

            prefix = f'mapreduce-local-task{task_id:05d}-'
            LOGGER.debug(prefix)
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                filepath = os.path.join(tmpdir, f'part-{task_id:05d}')

                with open(filepath, 'w', encoding='utf-8') as writefile:
                    with subprocess.Popen(
                        [executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=writefile,
                    ) as reduce_process:
                        for line in instream:
                            reduce_process.stdin.write(line)

                shutil.move(filepath, message_dict['output_directory'])
            self.finish_task(message_dict['task_id'], message_dict)

    def send_tcp_msg(self, host, port, msg):
        """Send TCP message."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((host, port))
            # send a message
            LOGGER.debug(
                "TCP send to %s:%s \n%s", host, port, msg
            )
            sock.sendall(msg.encode('utf-8'))

    def finish_task(self, task_id, message_dict):
        """Send finish msg to manager."""
        msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": message_dict['worker_host'],
            "worker_port": message_dict['worker_port']
        }
        self.send_tcp_msg(self.manager_host, self.manager_port,
                          json.dumps(msg, indent=2))

    def sort_files(self, temp_output_files, message_dict):
        """Sort keys within individual files."""
        # copy files to output directory
        for file in temp_output_files:
            lines = []
            with open(file, 'r', encoding='utf-8') as readfile:
                lines = readfile.readlines()
                lines.sort()

            output_file = os.path.join(
                pathlib.Path(message_dict['output_directory']),
                os.path.basename(file).split('/')[-1])
            with open(output_file, 'w', encoding='utf-8') as writefile:
                writefile.writelines(lines)


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
