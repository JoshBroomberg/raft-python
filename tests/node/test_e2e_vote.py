
from contextlib import contextmanager
import os
import pytest

import pykka

from raft.node.persisted_state import PersistedState
from raft.node.actor import RaftNodeActor, RaftNodeState
from raft.node.commands import *
from raft.node.log import RaftLog
from raft.network.outbound_rpc import DirectRPCCommandClient
from raft.node.actor import MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT

@contextmanager
def persisted_state(db_path):
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass

    yield PersistedState(db_path, current_term=0, voted_for=None)

    os.remove(db_path)

@contextmanager
def raft_log(db_path):
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass

    yield RaftLog(db_path)

    os.remove(db_path)

@pytest.fixture
def direct_rpc_client():
    return DirectRPCCommandClient(
        node_to_recipient_obj={}
    )

@pytest.fixture
def actors(direct_rpc_client):
    with (
        persisted_state("/tmp/node-0-test-state.db") as ps_0,
        raft_log("/tmp/node-0-test-log.db") as log_0,
        persisted_state("/tmp/node-1-test-state.db") as ps_1,
        raft_log("/tmp/node-1-test-log.db") as log_1,
        persisted_state("/tmp/node-2-test-state.db") as ps_2,
        raft_log("/tmp/node-2-test-log.db") as log_2
    ):
        NODE_ID_0 = 0
        node_state_0 = RaftNodeState(
            node_id=NODE_ID_0,
            election_timeout=MIN_ELECTION_TIMEOUT,
        )
        actor_0 = RaftNodeActor.start(
            node_state=node_state_0,
            persisted_state=ps_0,
            log=log_0,
            rpc_client=direct_rpc_client,
        ).proxy()
        direct_rpc_client.register_recipient(NODE_ID_0, actor_0)

        NODE_ID_1 = 1
        node_state_1 = RaftNodeState(
            node_id=NODE_ID_1,
            election_timeout=min(MIN_ELECTION_TIMEOUT + 15, MAX_ELECTION_TIMEOUT),
        )
        actor_1 = RaftNodeActor.start(
            node_state=node_state_1,
            persisted_state=ps_1,
            log=log_1,
            rpc_client=direct_rpc_client,
        ).proxy()
        direct_rpc_client.register_recipient(NODE_ID_1, actor_1)

        NODE_ID_2 = 2
        node_state_2 = RaftNodeState(
            node_id=NODE_ID_2,
            election_timeout=min(MIN_ELECTION_TIMEOUT + 15, MAX_ELECTION_TIMEOUT),
        )
        actor_2 = RaftNodeActor.start(
            node_state=node_state_2,
            persisted_state=ps_2,
            log=log_2,
            rpc_client=direct_rpc_client,
        ).proxy()

        direct_rpc_client.register_recipient(NODE_ID_2, actor_2)

        yield [actor_0, actor_1, actor_2]

    actor_0.stop()
    actor_1.stop()
    actor_2.stop()

def tick_all(actors, n=1):
    for _ in range(n):
        tick_futures = [actor.tick() for actor in actors]
        pykka.get_all(tick_futures)

def test_basic_election(actors):
    for actor in actors:
        assert not actor.node_state.get().is_leader
        assert not actor.node_state.get().is_candidate
    
    # After one tick, no-one has hit election timeout
    tick_all(actors, 1)

    for actor in actors:
        assert not actor.node_state.get().is_leader
        assert not actor.node_state.get().is_candidate
    
    # Tick up to the minimum election timeout 
    # node 0 becomes a candidate but doesn't yet trigger election
    tick_all(actors, MIN_ELECTION_TIMEOUT - 1)
    assert actors[0].node_state.get().is_candidate
    assert not actors[0].node_state.get().is_leader
    assert actors[0].persisted_state.get().get_voted_for() is None

    for actor in actors[1:]:
        assert not actor.node_state.get().is_leader
        assert not actor.node_state.get().is_candidate
    
    # On next tick, node 0 triggers election and wins
    tick_all(actors, 1)
    assert actors[0].persisted_state.get().get_voted_for() == 0
    assert not actors[0].node_state.get().is_candidate
    assert actors[0].node_state.get().is_leader

def test_election_with_gated_follower(actors):
    # NOTE: this test makes the assumption
    # that node 0 can get through two elections
    # before nodes 1/2 trigger their first election
    # this is achieved with manual manipulation of
    # the election timeouts

    for actor in actors:
        assert not actor.node_state.get().is_leader
        assert not actor.node_state.get().is_candidate
    
    # After two ticks, no-one has hit election timeout
    tick_all(actors, 2)

    for actor in actors:
        assert not actor.node_state.get().is_leader
        assert not actor.node_state.get().is_candidate
    
    # decrease the time since last heard by 2, simulating
    # an old leader that sent a message to these two nodes
    # slightly after the new leader
    actors[1].node_state.get().ticks_since_leader_message -= 2
    actors[2].node_state.get().ticks_since_leader_message -= 2
    assert actors[1].node_state.get().ticks_since_leader_message == 0
    assert actors[2].node_state.get().ticks_since_leader_message == 0

    # Tick up to the minimum election timeout 
    # node 0 becomes a candidate but doesn't yet trigger election
    tick_all(actors, MIN_ELECTION_TIMEOUT - 2)
    assert actors[0].node_state.get().is_candidate
    assert not actors[0].node_state.get().is_leader
    assert actors[0].persisted_state.get().get_voted_for() is None

    for actor in actors[1:]:
        assert not actor.node_state.get().is_leader
        assert not actor.node_state.get().is_candidate
    
    # On next tick, node 0 triggers election and but loses
    # as nodes 1 and 2 have heard from the old leader
    tick_all(actors, 1)
    assert actors[0].persisted_state.get().get_voted_for() == 0
    assert actors[0].persisted_state.get().get_current_term() == 1
    assert actors[0].node_state.get().is_candidate
    assert not actors[0].node_state.get().is_leader

    # TODO: introspect the messages sent to confirm.

    # Tick once, node 0 is still a candidate but hasn't incremented term
    tick_all(actors, 1)
    assert actors[0].persisted_state.get().get_voted_for() == 0
    assert actors[0].persisted_state.get().get_current_term() == 1
    assert actors[0].node_state.get().is_candidate

    # Tick up through the min election timeout again
    # node 0 triggers new election, increments term and wins
    tick_all(actors, MIN_ELECTION_TIMEOUT-1)
    assert actors[0].persisted_state.get().get_voted_for() == 0
    assert actors[0].persisted_state.get().get_current_term() == 2
    assert not actors[0].node_state.get().is_candidate
    assert actors[0].node_state.get().is_leader