
import logging
import pykka

class CommandConnectionListener(pykka.ThreadingActor):
    def __init__(self, addr, command_recipient_actor_ref):
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
