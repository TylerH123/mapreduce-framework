import socket
import logging
import time

# Configure logging
LOGGER = logging.getLogger(__name__)

def create_tcp_server(host, port, signals, handle_message):
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
		while not signals["shutdown"]:
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
			handle_message(message_str)
                

def create_udp_server(host, port, signals, handle_message):
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
		while not signals["shutdown"]:
			try:
				message_bytes = sock.recv(4096)
			except socket.timeout:
				continue
			message_str = message_bytes.decode("utf-8")
			handle_message(message_str)


def send_tcp_msg(host, port, msg, error_callback: None):
	"""Send TCP message."""
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
			if error_callback:
				error_callback()
			return False
		

def create_tcp_socket(manager_host, manager_port, create_message):
	# create an INET, STREAMing socket, this is TCP
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
		# connect to the server
		sock.connect((manager_host, manager_port))
		message = create_message(manager_host, manager_port)
		sock.sendall(message.encode('utf-8'))


def send_heartbeat(signals, manager_host, manager_port, message):
        """Test UDP Socket Client."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((manager_host, manager_port))
            # Send a message
            while not signals['shutdown']:
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)