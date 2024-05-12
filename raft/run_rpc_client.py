import logging
import sys


from raft.network.outbound_rpc import run_repl

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(threadName)s %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)

run_repl()
