import logging
import socket
import orjson

logger = logging.getLogger(__name__)

MESSAGE_LENGTH_BYTES = 20

def send_message_to_socket(sock, msg):
    size = str(len(msg)).rjust(MESSAGE_LENGTH_BYTES).encode()
    sock.sendall(size)
    sock.sendall(msg)

def recv_message_from_socket(sock, timeout=1):
    size = int(recv_exactly_nbytes_from_socket(sock, MESSAGE_LENGTH_BYTES, timeout=timeout))
    if size is None:
        return None

    return recv_exactly_nbytes_from_socket(sock, size, timeout=timeout)

def recv_exactly_nbytes_from_socket(sock, nbytes, timeout=1):
    # TODO-timeout: implement the timeout here.
    chunks = []
    while nbytes > 0:
        chunk = sock.recv(nbytes)
        if chunk == b'':
            raise IOError("Incomplete message")
        chunks.append(chunk)
        nbytes -= len(chunk)
    return b''.join(chunks)


def encode_json_message(msg):
    return orjson.dumps(msg)

def decode_json_message(msg):
    return orjson.loads(msg.decode('utf-8'))
