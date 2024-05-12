from unittest.mock import MagicMock

from raft.network.inbound_rpc import handle_inbound_rpc_socket
from raft.network.message_utils import send_message_to_socket
from raft.node.commands import PingRequestBody, PingResponseBody

class MockSocket():
    def __init__(self, send_buffer, receive_buffer):
        self.send_buffer = send_buffer
        self.receive_buffer = receive_buffer

    def sendall(self, data):
        self.send_buffer.extend(data)

    def recv(self, size):
        if len(self.receive_buffer) < size:
            raise IOError("Socket closed")
        
        data = self.receive_buffer[:size]
        self.receive_buffer = self.receive_buffer[size:]
        return data

    def settimeout(self, timeout):
        pass
    
    def close(self):
        self.closed = True


def test_inbound_rpc():
    send_buffer = bytearray()
    receive_buffer = bytearray()

    outbound_socket = MockSocket(send_buffer, receive_buffer)
    inbound_socket = MockSocket(receive_buffer, send_buffer)

    send_message_to_socket(outbound_socket, b'{"command": "ping", "arguments": {"x": "y"}}')
    assert inbound_socket.receive_buffer == bytearray(b'                  44{"command": "ping", "arguments": {"x": "y"}}')
    
    command_recipient = MagicMock()
    command_result_future = MagicMock()
    command_result_future.get.return_value = PingResponseBody(x="y")
    command_recipient.ping.return_value = command_result_future

    handle_inbound_rpc_socket(command_recipient, inbound_socket)
    assert command_recipient.ping.call_count == 1
    assert command_recipient.ping.call_args.kwargs == {
        "request": PingRequestBody(x="y")
    }

    print(inbound_socket.send_buffer)
    assert inbound_socket.send_buffer == bytearray(b'                   9{"x":"y"}')