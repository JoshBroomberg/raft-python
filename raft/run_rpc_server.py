from functools import partial
import logging
import random
import sys
import threading
import time

from raft.node.persisted_state import PersistedState
from raft.node.actor import RaftNodeActor, RaftNodeState
from raft.node.log import RaftLog
from raft.network.inbound_rpc import handle_inbound_rpc_socket, run_inbound_rpc_listener
from raft.config import HOSTS

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(threadName)s %(levelname)s - %(message)s',
)

# set log level for raft.network module to WARNING
logging.getLogger('raft.network').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

SECONDS_PER_TICK = 0.01

node_id = int(sys.argv[1])
rand_db_num = random.randint(0, 100000)

state_db_path = f'/tmp/raft_state_{node_id}_{rand_db_num}.db'
ps = PersistedState(state_db_path, current_term=0, voted_for=None)

log_db_path = f'/tmp/raft_log_{node_id}_{rand_db_num}.db'
log = RaftLog(log_db_path)

# Seed the random number generator with node id
random.seed(node_id)

node_state = RaftNodeState(
    node_id=node_id,
    leader_id=None, #NOTE: set to 0 to start cluster with a leader
    is_candidate=False,
    election_timeout=random.uniform(7, 15),
)

def run():
    actor = RaftNodeActor.start(
        node_state=node_state,
        persisted_state=ps,
        log=log,
    ).proxy()

    def tick():
        while True:
            actor.tick()
            time.sleep(SECONDS_PER_TICK)

    threading.Thread(
        target=tick,
        daemon=True
    ).start()

    run_inbound_rpc_listener(
        HOSTS[node_id],
        partial(handle_inbound_rpc_socket, actor)
    )

run()