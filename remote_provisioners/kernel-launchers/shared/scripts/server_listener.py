import base64
import json
import logging
import os
import random
import signal
import socket
import uuid
from multiprocessing import Process
from threading import Thread
from typing import Dict, List, Optional

from Cryptodome.Cipher import AES, PKCS1_v1_5
from Cryptodome.PublicKey import RSA
from Cryptodome.Random import get_random_bytes
from Cryptodome.Util.Padding import pad
from jupyter_client.connect import write_connection_file

LAUNCHER_VERSION = 1  # Indicate to server the version of this launcher (payloads may vary)

max_port_range_retries = int(os.getenv("MAX_PORT_RANGE_RETRIES", "5"))

log_level = os.getenv("LOG_LEVEL", "10")
log_level = int(log_level) if log_level.isdigit() else log_level

logging.basicConfig(format="[%(levelname)1.1s %(asctime)s.%(msecs).03d %(name)s] %(message)s")

logger = logging.getLogger("server_listener for R launcher")
logger.setLevel(log_level)


def _encrypt(connection_info_bytes: bytes, public_key: str) -> bytes:
    """Encrypt the connection information using a generated AES key that is then encrypted using
    the public key passed from the server.  Both are then returned in an encoded JSON payload.
    """
    aes_key = get_random_bytes(16)
    cipher = AES.new(aes_key, mode=AES.MODE_ECB)

    # Encrypt the connection info using the aes_key
    encrypted_connection_info = cipher.encrypt(pad(connection_info_bytes, 16))
    b64_connection_info = base64.b64encode(encrypted_connection_info)

    # Encrypt the aes_key using the server's public key
    imported_public_key = RSA.importKey(base64.b64decode(public_key.encode()))
    cipher = PKCS1_v1_5.new(key=imported_public_key)
    encrypted_key = base64.b64encode(cipher.encrypt(aes_key))

    # Compose the payload and Base64 encode it
    payload = {
        "version": LAUNCHER_VERSION,
        "key": encrypted_key.decode(),
        "conn_info": b64_connection_info.decode(),
    }
    b64_payload = base64.b64encode(json.dumps(payload).encode(encoding="utf-8"))
    return b64_payload


def return_connection_info(
    connection_file: str, response_addr: str, lower_port: int, upper_port: int, kernel_id: str, public_key: str
) -> socket:
    """Returns the connection information corresponding to this kernel."""
    response_parts = response_addr.split(":")
    if len(response_parts) != 2:
        logger.error(
            f"Invalid format for response address '{response_addr}'. Assuming 'pull' mode..."
        )
        return

    response_ip = response_parts[0]
    try:
        response_port = int(response_parts[1])
    except ValueError:
        logger.error(
            f"Invalid port component found in response address '{response_addr}'. Assuming 'pull' mode..."
        )
        return

    with open(connection_file) as fp:
        cf_json = json.load(fp)
        fp.close()

    # add process and process group ids into connection info
    pid = os.getpid()
    cf_json["pid"] = pid
    cf_json["pgid"] = os.getpgid(pid)

    # prepare socket address for handling signals
    comm_sock = prepare_comm_socket(lower_port, upper_port)
    cf_json["comm_port"] = comm_sock.getsockname()[1]
    cf_json["kernel_id"] = kernel_id

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((response_ip, response_port))
        json_content = json.dumps(cf_json).encode(encoding="utf-8")
        logger.debug(f"JSON Payload '{json_content}")
        payload = _encrypt(json_content, public_key)
        logger.debug(f"Encrypted Payload '{payload}")
        s.send(payload)

    return comm_sock


def prepare_comm_socket(lower_port: int, upper_port: int) -> socket:
    """Prepares the socket to which the server will send signal and shutdown requests."""
    sock = _select_socket(lower_port, upper_port)
    logger.info(
        f"Signal socket bound to host: {sock.getsockname()[0]}, port: {sock.getsockname()[1]}"
    )
    sock.listen(1)
    sock.settimeout(5)
    return sock


def _select_ports(count, lower_port: int, upper_port: int) -> List:
    """Select and return n random ports that are available and adhere to the given port range, if applicable."""
    ports = []
    sockets = []
    for _ in range(count):
        sock = _select_socket(lower_port, upper_port)
        ports.append(sock.getsockname()[1])
        sockets.append(sock)
    for sock in sockets:
        sock.close()
    return ports


def _select_socket(lower_port: int, upper_port: int) -> socket:
    """Create and return a socket whose port is available and adheres to the given port range, if applicable."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    found_port = False
    retries = 0
    while not found_port:
        try:
            sock.bind(("0.0.0.0", _get_candidate_port(lower_port, upper_port)))
            found_port = True
        except Exception:
            retries = retries + 1
            if retries > max_port_range_retries:
                raise RuntimeError(
                    f"Failed to locate port within range {lower_port}..{upper_port} "
                    f"after {max_port_range_retries} retries!"
                )
    return sock


def _get_candidate_port(lower_port: int, upper_port: int) -> int:
    """Returns a port within the given range.  If the range is zero, the zero is returned."""
    range_size = upper_port - lower_port
    if range_size == 0:
        return 0
    return random.randint(lower_port, upper_port)


def get_server_request(sock: socket) -> Dict:
    """Gets a request from the server and returns the corresponding dictionary."""
    conn = None
    data = ""
    request_info = None
    try:
        logger.info("DEBUG: get_server_request: waiting for socket accept")
        conn, addr = sock.accept()
        logger.info("DEBUG: get_server_request: socket accepted")
        while True:
            buffer: bytes = conn.recv(1024)
            if buffer == b'':  # send is complete
                if len(data) > 0:
                    request_info = json.loads(data)
                else:
                    logger.info("DEBUG: get_server_request: no data received - returning None")
                break
            else:
                logger.info(f"DEBUG: get_server_request: received buffer: '{buffer}'")
            data = data + buffer.decode("utf-8")  # append what we received until we get no more...
    except Exception as ex:
        if type(ex) is not socket.timeout:
            raise ex
    finally:
        if conn:
            conn.close()

    return request_info


def server_listener(connection_file: str, response_addr: str, lower_port: int, upper_port: int,
                    kernel_id: str, public_key: str, parent_pid: int, cluster_type: Optional[str] = "none") -> None:
    """Waits for requests from the server and processes each when received.  Currently,
    these will be one of a sending a signal to the corresponding kernel process (signum) or
    stopping the listener and exiting the kernel (shutdown).
    """
    comm_socket: socket = return_connection_info(
        connection_file,
        response_addr,
        int(lower_port),
        int(upper_port),
        kernel_id,
        public_key
    )
    shutdown = False
    while not shutdown:
        request = get_server_request(comm_socket)
        if request:
            signum = -1  # prevent logging poll requests since that occurs every 3 seconds
            if request.get("signum") is not None:
                signum = int(request.get("signum"))
                os.kill(parent_pid, signum)
                if signum == 2 and cluster_type == "spark":
                    os.kill(parent_pid, signal.SIGUSR2)
            if request.get("shutdown") is not None:
                shutdown = bool(request.get("shutdown"))
            if signum != 0:
                logger.info(f"server_listener got request: {request}")


def setup_server_listener(
        conn_filename: str,
        parent_pid: int,
        lower_port: int,
        upper_port: int,
        response_addr: str,
        kernel_id: str,
        public_key: str,
        cluster_type: Optional[str] = None,
        as_thread: Optional[bool] = True
) -> None:
    """Initializes the server listener thread or process depending on the `as_thread` parameter.

    Currently, R kernels use a thread for the listener while Python kernels use a process.
    """
    key = str(uuid.uuid4()).encode()  # convert to bytes

    ports = _select_ports(5, lower_port, upper_port)

    write_connection_file(
        fname=conn_filename,
        ip="0.0.0.0",
        key=key,
        shell_port=ports[0],
        iopub_port=ports[1],
        stdin_port=ports[2],
        hb_port=ports[3],
        control_port=ports[4],
    )
    if as_thread:
        server_listener_thread = Thread(
            target=server_listener,
            args=(conn_filename,
                  response_addr,
                  int(lower_port),
                  int(upper_port),
                  kernel_id,
                  public_key,
                  int(parent_pid),
                  cluster_type,
                  ),
        )
        server_listener_thread.start()
    else:
        server_listener_process = Process(
            target=server_listener,
            args=(conn_filename,
                  response_addr,
                  int(lower_port),
                  int(upper_port),
                  kernel_id,
                  public_key,
                  int(parent_pid),
                  cluster_type,
                  ),
        )
        server_listener_process.start()

    return


__all__ = [
    "setup_server_listener",
]
