from collections import defaultdict
import logging

import select
from socket import socket, AF_INET, SOCK_STREAM

from typing import Optional
import threading


from raft.network.message_utils import send_message_to_socket, recv_message_from_socket, encode_json_message, decode_json_message
from raft.node.commands import COMMAND_TO_REQUEST_MODEL, COMMAND_TO_RESPONSE_MODEL
from raft.config import HOSTS

from pydantic import BaseModel

logger = logging.getLogger(__name__)

class ConnectionClient():
    def connect(self, node_id) -> Optional[socket]:
        try:
            logger.info("Connecting to node %s", node_id)
            addr = HOSTS[node_id]
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            logger.info("Connected to node %s", node_id)
            return sock
        except Exception:
            return None

class RPCCommandClient:
    def send_command(self, node_id, cmd, request_body: BaseModel, response_model=None, timeout=1):
        raise NotImplementedError()
    
class DirectRPCCommandClient:
    def __init__(self, node_to_recipient_obj):
        self.node_to_recipient_obj = node_to_recipient_obj
        self.blocked_communication = set()

    def send_command(self, node_id, cmd, request_body: BaseModel, response_model=None, timeout=1):
        if node_id in self.blocked_communication:
            return None # simulates timeout
        
        recipient = self.node_to_recipient_obj[node_id]
        return getattr(recipient, cmd)(request=request_body).get()

    def register_recipient(self, node_id, recipient):
        self.node_to_recipient_obj[node_id] = recipient
    
    def block_communication(self, node_id):
        self.blocked_communication.add(node_id)
    
    def unblock_communication(self, node_id):
        self.blocked_communication.remove(node_id)

class NetworkRPCCommandClient(RPCCommandClient):
    def __init__(self, connection_client=None):
        if connection_client is None:
            connection_client = ConnectionClient()
        
        self.connection_client = connection_client

        self.sockets = {}
        self.node_lock = defaultdict(lambda : threading.Lock())

    def send_command(self, node_id, cmd, request_body: BaseModel, timeout=1):
        
        # TODO-timeout: lock timeout?
        with self.node_lock[node_id]:
            sock = self.get_socket(node_id)

            # If we failed to connect, return None
            # TODO: this should retry at least once. Needs to distinguish
            # between a failure to get a new connection (on this attempt)
            # and a failure to send the message (on a retry)
            if sock is None:
                logger.info("Failed to get socket for node %s", node_id)
                return None
            
            body = request_body.model_dump()
            rpc_message = {'command': cmd, 'arguments': body}
            logger.debug('Sending message %s to node %s', rpc_message, node_id)

            # TODO-next: add handling here to actively detect dead connections
            try:
                logger.info("Sending cmd '%s' to node %s", cmd, node_id)
                send_message_to_socket(sock, encode_json_message(rpc_message))
                logger.info("Sent cmd '%s' to node %s", cmd, node_id)
                
                logger.info("Waiting to receive reply for '%s' from node %s", cmd, node_id)
                # TODO-timeout: real timeout here
                # This is how long we'll wait to be able to received something from the 
                # socket. This is timeout from the client's perspective.
                ready = select.select([sock], [], [], 20)
                logger.debug('Wait to receive reply from node %s elapsed', rpc_message, node_id)

                if ready[0]:
                    logger.debug('There is something to read from node %s', rpc_message, node_id)
                    response = recv_message_from_socket(sock, timeout=timeout)
                    logger.info("Read the reply for '%s' from node %s", cmd, node_id)
                else:
                    logger.debug('There is nothing to read from node %s', rpc_message, node_id)
                    return None

            except Exception:
                self.delete_socket(node_id)
                logger.info("Error sending message to node %s. Cleaning up socket.")
                return None
            
            try:
                response_model = COMMAND_TO_RESPONSE_MODEL[cmd]
                
                try:
                    dict_resp = decode_json_message(response)
                except Exception:
                    logger.exception("Error parsing response", exc_info=True)
                    return None

                if "error" in dict_resp:
                    logger.error("Error in response: %s", dict_resp["error"])
                    return None

                return response_model.model_validate_json(response)
            except Exception:
                logger.exception("Error parsing response", exc_info=True)
                return None
    
    def get_socket(self, node_id):
        if self.sockets.get(node_id, None) is None:
            sock = self.connection_client.connect(node_id)
            self.sockets[node_id] = sock
        
        return self.sockets[node_id]

    def delete_socket(self, node_id):
        sock = self.sockets.pop(node_id)
        sock.close()

    def close(self):
        for _, sock in self.sockets.items():
            sock.close()
        self.sockets = {}

def run_repl():
    client = NetworkRPCCommandClient()

    try:
        while True:
            msg = input("Say > ")
            if not msg:
                break
            
            try:
                parts = msg.split(" ")
                if parts[0] == "exit":
                    break
                
                if parts[0] == "clear":
                    client.close()
                    continue

                node_id = int(parts[0])
                cmd = parts[1]
                
                args = {}
                for part in parts[2:]:
                    key, value = part.split("=")
                    args[key] = value

                request_model = COMMAND_TO_REQUEST_MODEL[cmd]
                request_body = request_model.model_validate(args)
            except Exception as e:
                print("Error parsing input", e)
                continue

            try:
                response = client.send_command(
                    node_id=node_id,
                    cmd=cmd,
                    request_body=request_body
                )

                if response is None:
                    print(">>> Received empty response! Timeout or error")
                    continue

                print(">>> Received:", response.model_dump())

            except Exception as e:
                print("Error sending command", e)
                continue

    except Exception as e:
        logger.exception("Error in client repl", exc_info=True)
    finally:
        print("Closing client connection")
        client.close()