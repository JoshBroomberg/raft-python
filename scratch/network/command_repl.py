from raft.network.socket_message_utils import send_message_to_socket, recv_message_from_socket
from socket import socket, AF_INET, SOCK_STREAM
import orjson
import sys

def run():
    port = int(sys.argv[1])
    print("connecting to port", port)

    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(('localhost', port))
    try:
        while True:
            msg = input("Say >")
            if not msg:
                break

            parts = msg.split(" ")
            cmd = parts[0]
            kwargs = {}
            if len(parts) > 1:
                for part in parts[1:]:
                    kwarg_parts = part.split("=")
                    kwargs[kwarg_parts[0]] = kwarg_parts[1]
            
            rpc_message = orjson.dumps({'cmd': cmd, 'kwargs': kwargs})
            send_message_to_socket(sock, rpc_message)       
            response = recv_message_from_socket(sock)
            print("Received >", response.decode('utf-8'))
    except Exception as e:
        print("Error", e)
    finally:
        print("Closing client connection")
        sock.close()