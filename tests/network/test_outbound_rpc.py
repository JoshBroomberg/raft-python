from unittest.mock import MagicMock

from pydantic import BaseModel

from raft.network.outbound_rpc import NetworkRPCCommandClient
from raft.node.commands import PingRequestBody, PingResponseBody, PING

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


def test_outbound_rpc():
    send_buffer = bytearray()
    receive_buffer = bytearray()

    outbound_socket = MockSocket(send_buffer, receive_buffer)
    # Mock the response from the RPC client
    outbound_socket.receive_buffer = bytearray(b'                   9{"x":"y"}')

    mock_connection_client = MagicMock()
    mock_connection_client.connect.return_value = outbound_socket
    
    ping_request = PingRequestBody(x="y")
    resp: PingResponseBody = NetworkRPCCommandClient(mock_connection_client).send_command(
        node_id=0,
        cmd=PING,
        request_body=ping_request,
    )
    assert mock_connection_client.connect.call_count == 1
    assert mock_connection_client.connect.call_args.args == (0,)

    assert outbound_socket.send_buffer == bytearray(b'                  40{"command":"ping","arguments":{"x":"y"}}')
    assert resp.x == ping_request.x