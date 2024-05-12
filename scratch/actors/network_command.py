import logging
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR

import pykka
from dependency_injector.wiring import inject
import orjson

from raft.network.socket_message_utils import send_message_to_socket, recv_message_from_socket, decode_json_message

class CommandConnectionListener(pykka.ThreadingActor):
    @inject
    def __init__(self, addr: tuple[str, int], command_recipient_actor_ref: pykka.ActorRef):
        super().__init__()
        self.logger = logging.getLogger(__class__.__name__)
        self.addr = addr
        self.command_recipient_actor_ref = command_recipient_actor_ref

    def on_start(self) -> None:
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.sock.bind(self.addr)
        self.sock.listen()

        self.logger.info('Listening on %s', self.addr)
        try:
            while True:
                client_socket, client_addr = self.sock.accept()
                CommandMessageProcessor.start(
                    client_socket=client_socket,
                    addr=client_addr,
                    command_recipient_actor_ref=self.command_recipient_actor_ref.proxy()
                )
        except (IOError, KeyboardInterrupt):
            self.logger.info('Closing server listener socket')
        finally:
            self.kill_socket()
    
    def on_failure(self, exception_type, exception_value, traceback) -> None:
        self.kill_socket()
    
    def on_stop(self) -> None:
        self.kill_socket()

    def kill_socket(self) -> None:
        if self.sock:
            self.sock.close()
            self.sock = None

class CommandMessageProcessor(pykka.ThreadingActor):
    @inject
    def __init__(self, client_socket: socket, addr: tuple[str, int], command_recipient_actor_ref: pykka.ActorRef):
        super().__init__()
        self.logger = logging.getLogger(__class__.__name__)
        self.client_socket = client_socket
        self.addr = addr
        self.command_recipient_actor_ref = command_recipient_actor_ref

    def on_start(self) -> None:
        try:
            self.logger.info('Starting listen to client on %s', self.addr)
            while True:
                msg = recv_message_from_socket(self.client_socket)
                self.logger.info('Received message %s', msg)

                try:
                    decoded_message = decode_json_message(msg)
                except orjson.JSONDecodeError:
                    self.logger.error('Failed to decode JSON message')
                    send_message_to_socket(self.client_socket, orjson.dumps({'error': 'Failed to decode JSON message'}))
                    continue

                cmd = decoded_message.get("cmd")
                if not cmd:
                    self.logger.error('No command in message')
                    send_message_to_socket(self.client_socket, orjson.dumps({'error': 'No command in message'}))
                    continue

                func = getattr(self.command_recipient_actor_ref, cmd)
                if not func:
                    self.logger.error('Command not found')
                    send_message_to_socket(self.client_socket, orjson.dumps({'error': f'Command {cmd} not found'}))
                    continue

                kwargs = decoded_message.get("kwargs", {})
                try:
                    res = func(**kwargs)
                    send_message_to_socket(self.client_socket, orjson.dumps(res.get()))
                except Exception as e:
                    self.logger.exception('Error processing command', exc_info=True)
                    send_message_to_socket(self.client_socket, orjson.dumps({'error': str(e)}))
        except Exception as e:
            self.logger.exception('Handling exception', exc_info=True)
        finally:
            self.logger.info('Closing connection to client')
            self.client_socket.close()