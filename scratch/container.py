from dependency_injector import containers, providers
from raft.actors.keyval import KeyValue
from raft.actors.network_command import CommandConnectionListener
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(threadName)s %(levelname)s - %(message)s',
)

class Container(containers.DeclarativeContainer):
    # service = providers.Factory(Service)

    keyvalue_actor = providers.Resource(
        KeyValue.start,
        # snapshot_name='/tmp/1714938502425.pickle',
    )

    network_actor = providers.Resource(
        CommandConnectionListener.start,
        addr=('localhost', 12345),
        command_recipient_actor_ref=keyvalue_actor,
    )

