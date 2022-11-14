"""MapReduce framework Manager node."""
import collections
import json
import logging
import os
import pathlib
import shutil
import socket
import tempfile
import threading
import time
import click

# Configure logging
LOGGER = logging.getLogger(__name__)


class Tasks:
    """Represents a task class."""

    def __init__(self):
        """Initialize task class."""
        self.tasks = collections.defaultdict(list)
        self.tasks_queue = collections.deque()
        self.num_tasks = 0

    def get_tasks(self):
        """Get tasks."""
        return self.tasks

    def get_tasks_queue(self):
        """Get tasks queue."""
        return self.tasks_queue

    def get_num_tasks(self):
        """Get num tasks."""
        return self.num_tasks


class Workers:
    """Represents workers data structure."""

    def __init__(self):
        """Initialize worker class."""
        # [ index: (host, port, status, task:dict) ] (status: "r", "b", "d")
        self.workers = []
        # { (worker_host:str, worker_port:int) -> index:int }
        self.worker_inds = {}
        self.workers_avail = 0
        # { (worker_host:str, worker_port:int) -> time:float }
        self.heartbeat_tracker = {}

    def get_workers(self):
        """Get workers."""
        return self.workers

    def get_worker_inds(self):
        """Get worker indexes."""
        return self.worker_inds

    def get_workers_avail(self):
        """Get workers available."""
        return self.workers_avail

    def get_heartbeat_tracker(self):
        """Get heartbeat tracker."""
        return self.heartbeat_tracker


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.workers = Workers()
        # { job_id:int -> job_info:dict }
        self.jobs = {}
        self.job_queue = collections.deque()
        self.job_id = 0
        self.tasks = Tasks()
        self.signals = {"shutdown": False}
        self.threads = []

        tcp_thread = threading.Thread(target=self.create_tcp_server,
                                      args=(host, port))
        self.threads.append(tcp_thread)
        udp_thread = threading.Thread(target=self.create_udp_server,
                                      args=(host, port))
        self.threads.append(udp_thread)

        LOGGER.info("Start TCP server thread")
        tcp_thread.start()
        LOGGER.info("Start UDP server thread")
        udp_thread.start()
        job_thread = threading.Thread(target=self.assign_job)
        self.threads.append(job_thread)
        job_thread.start()
        tcp_thread.join()
        udp_thread.join()
        job_thread.join()

    def create_tcp_server(self, host, port):
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
                # Wait for a connection for 1s.
                # The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket = sock.accept()[0]
                except socket.timeout:
                    continue
                # Socket recv() will block for a maximum of 1 second.
                # If you omit this, it blocks indefinitely,
                # waiting for packets.
                clientsocket.settimeout(1)
                # Receive data, one chunk at a time.
                # If recv() times out before we can read a chunk,
                # then go back to the top of the loop and try
                # again.
                # When the client closes the connection,
                # recv() returns empty data,
                # which breaks out of the loop. We make a simplifying
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
                        self.register_worker(message_dict)
                    elif message_dict['message_type'] == 'new_manager_job':
                        self.handle_new_job(message_dict)

                    elif message_dict['message_type'] == 'finished':
                        self.handle_finished_task(message_dict)

                    elif message_dict['message_type'] == 'shutdown':
                        self.shutdown()

                except json.JSONDecodeError:
                    continue

    def create_udp_server(self, host, port):
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

                for worker in self.workers.heartbeat_tracker.items():
                    if worker[1] and \
                       time.time() - worker[1] > 10:
                        self.handle_dead_worker(worker[0])
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue

                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                if message_dict['message_type'] == 'heartbeat':
                    worker_host = message_dict['worker_host']
                    worker_port = message_dict['worker_port']
                    worker = (worker_host, worker_port)
                    self.workers.heartbeat_tracker[worker] = time.time()

    def send_tcp_msg(self, host, port, msg):
        """Send a tcp msg."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((host, port))
            # send a message
            LOGGER.debug(
                "TCP send to %s:%s \n%s", host, port, msg
            )
            try:
                sock.sendall(msg.encode('utf-8'))
                return True
            except ConnectionRefusedError:
                worker = (host, port)
                self.handle_dead_worker(worker)
                return False

    def assign_job(self):
        """Handle a new job."""
        while not self.signals['shutdown']:
            if self.job_queue:
                LOGGER.debug("Starting a job")
                current_job_id = self.job_queue.popleft()
                current_job = self.jobs[current_job_id]
                LOGGER.debug("Current job id: %s", current_job_id)
                LOGGER.debug("\n%s", json.dumps(current_job, indent=2))
                prefix = f"mapreduce-shared-job{current_job_id:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    self.start_stage(current_job, False, tmpdir)
                    self.start_stage(current_job, True, tmpdir)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            else:
                time.sleep(1)

    def assign_task_to_workers(self, task):
        """Find next ready worker and give them a task."""
        for worker in self.workers.workers:
            if worker[2] == "r":
                task['worker_host'] = worker[0]
                task['worker_port'] = worker[1]
                worker[3] = task
                worker[2] = "b"
                return self.send_tcp_msg(worker[0], worker[1],
                                         json.dumps(task, indent=2))
        return False

    def partition_map_task(self, job):
        """Partition map tasks."""
        LOGGER.debug("Partitioning map tasks")
        input_dir = job['input_directory']
        num_mappers = job['num_mappers']
        path = pathlib.Path(input_dir)
        files = list(path.glob('*'))
        files.sort()
        for i, file in enumerate(files):
            self.tasks.tasks[i % num_mappers].append(str(file))

    def partition_reduce_task(self, input_directory):
        """Partition reduce tasks."""
        LOGGER.debug("Partitioning reduce tasks")
        path = pathlib.Path(input_directory)
        files = list(path.glob('*'))
        for file in files:
            filename = str(file)
            file_num = int(filename[-5:])
            self.tasks.tasks[file_num].append(filename)

    def register_worker(self, message_dict):
        """Register worker."""
        worker_host = message_dict['worker_host']
        worker_port = message_dict['worker_port']
        LOGGER.info("Acknowledged new worker on %s:%s",
                    worker_host, worker_port)
        self.workers.workers.append([worker_host, worker_port, "r", None])
        worker_id = len(self.workers.workers) - 1
        self.workers.worker_inds[(worker_host, worker_port)] = worker_id
        self.workers.workers_avail += 1
        msg = {
            "message_type": "register_ack",
            "worker_host": worker_host,
            "worker_port": worker_port
        }
        message_ack = json.dumps(msg, indent=2)
        self.send_tcp_msg(worker_host, worker_port, message_ack)

    def handle_new_job(self, message_dict):
        """Process incoming job."""
        LOGGER.info("Received new job")
        output = message_dict['output_directory']
        output = message_dict['output_directory']
        if os.path.exists(output):
            LOGGER.debug("Deleting output directory")
            shutil.rmtree(output, ignore_errors=True)
        os.mkdir(output)
        LOGGER.debug("Created new dir: %s", output)
        self.jobs[self.job_id] = message_dict
        self.job_queue.append(self.job_id)
        self.job_id += 1

    def handle_finished_task(self, message_dict):
        """Process finished task from worker."""
        worker_host = message_dict['worker_host']
        worker_port = message_dict['worker_port']
        worker_ind = self.workers.worker_inds[(worker_host, worker_port)]
        self.workers.workers[worker_ind][2] = 'r'
        self.workers.workers[worker_ind][3] = None
        self.workers.workers_avail += 1
        self.tasks.num_tasks -= 1
        LOGGER.debug("Worker %s:%s ready to accept jobs",
                     worker_host, worker_port)

    def shutdown(self):
        """Shutdown manager and workers."""
        LOGGER.debug("Received shutdown")
        for worker in self.workers.workers:
            if worker[2] != 'd':
                msg = json.dumps({"message_type": "shutdown"})
                self.send_tcp_msg(worker[0], worker[1], msg)
        LOGGER.debug("Shutting down")
        self.signals['shutdown'] = True

    def assign_tasks(self, job, reduce, tempdir):
        """Assign tasks to workers."""
        LOGGER.debug("Assigning tasks")
        num_reducers = job['num_reducers']
        if reduce:
            executable = job['reducer_executable']
            output_dir = job['output_directory']
        else:
            executable = job['mapper_executable']
            output_dir = tempdir
        while not self.signals['shutdown'] and self.tasks.tasks_queue:
            # LOGGER.debug(self.tasks)
            if self.workers.workers_avail > 0:
                task_id = self.tasks.tasks_queue[0]
                if reduce:
                    task_message = {
                        "message_type": "new_reduce_task",
                        "task_id": task_id,
                        "executable": executable,
                        "input_paths": self.tasks.tasks[task_id],
                        "output_directory": output_dir,
                        "worker_host": None,
                        "worker_port": None
                    }
                else:
                    task_message = {
                        "message_type": "new_map_task",
                        "task_id": task_id,
                        "executable": executable,
                        "input_paths": self.tasks.tasks[task_id],
                        "output_directory": output_dir,
                        "num_partitions": num_reducers,
                        "worker_host": None,
                        "worker_port": None
                    }
                if self.assign_task_to_workers(task_message):
                    self.tasks.tasks_queue.popleft()
                    self.tasks.tasks.pop(task_id)
                    self.workers.workers_avail -= 1
            else:
                time.sleep(1)

    def start_stage(self, job, reduce, tmpdir):
        """Start map or reduce stage."""
        if reduce:
            self.partition_reduce_task(tmpdir)
            stage = "reduce"
            for i in range(job['num_reducers']):
                self.tasks.tasks_queue.append(i)
        else:
            self.partition_map_task(job)
            stage = "map"
            for i in range(job['num_mappers']):
                self.tasks.tasks_queue.append(i)
        self.tasks.num_tasks = len(self.tasks.tasks_queue)
        # LOGGER.debug(self.tasks_queue)
        LOGGER.info("Begin %s stage", stage)
        while not self.signals['shutdown'] and self.tasks.num_tasks != 0:
            self.assign_tasks(job, reduce, tmpdir)
            LOGGER.debug("Waiting on %s to finish...", stage)
            time.sleep(1)

    def handle_dead_worker(self, worker):
        """Process dead workers."""
        LOGGER.info("Lost connection to worker %s:%s", worker[0], worker[1])
        worker_ind = self.workers.worker_inds[worker]
        LOGGER.debug("Worker index: %s", worker_ind)
        if self.workers.workers[worker_ind][2] == 'r':
            self.workers.workers_avail -= 1
        elif self.workers.workers[worker_ind][2] == 'b':
            LOGGER.debug('REASSIGN DEAD WORKER TASK')
            task = self.workers.workers[worker_ind][3]
            task_id = task['task_id']
            self.tasks.tasks_queue.append(task_id)
            self.tasks.tasks[task_id] = task['input_paths']
        self.workers.workers[worker_ind][2] = 'd'
        self.workers.workers[worker_ind][3] = None
        self.workers.heartbeat_tracker[worker] = None


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
