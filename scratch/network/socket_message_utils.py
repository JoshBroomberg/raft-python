import orjson

MESSAGE_LENGTH_BYTES = 32

def send_message_to_socket(sock, msg):
    size = str(len(msg)).rjust(MESSAGE_LENGTH_BYTES).encode()
    sock.sendall(size)
    sock.sendall(msg)

def recv_message_from_socket(sock):
    size = int(recv_exactly_nbytes_from_socket(sock, MESSAGE_LENGTH_BYTES))
    return recv_exactly_nbytes_from_socket(sock, size)

def recv_exactly_nbytes_from_socket(sock, nbytes):
    chunks = []
    while nbytes > 0:
        chunk = sock.recv(nbytes)
        if chunk == b'':
            raise IOError("Incomplete message")
        chunks.append(chunk)
        nbytes -= len(chunk)
    return b''.join(chunks)

def decode_json_message(msg):
    return orjson.loads(msg.decode('utf-8'))


