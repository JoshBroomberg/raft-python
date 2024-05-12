
from raft.network.outbound_rpc import NetworkRPCCommandClient
from raft.node.commands import NewCommandRequest, NewCommandResponse
import time
import threading

def run_test_client(client_id):
    client = NetworkRPCCommandClient()
    command_id = 0
    
    while command_id < 10:
        response: NewCommandResponse = client.send_command(
            node_id=0,
            cmd="new_command",
            request_body=NewCommandRequest(
                command=f"{client_id}-{command_id}",
            )
        )
        if not response.success:
            print(f"Client {client_id} failed to send command {command_id}")

        command_id += 1

# run 5 clients in threads

start = time.time()
threads = []
for i in range(10):
    t = threading.Thread(target=run_test_client, args=(i,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print(time.time()- start)
