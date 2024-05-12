import copy
import pytest
import os
from raft.node.persisted_state import PersistedState
from raft.node.actor import RaftNodeActor, RaftNodeState
from raft.node.commands import AppendEntriesRequest, LogEntry, NewCommandRequest, NewCommandResponse
from raft.node.log import RaftLog
from raft.network.outbound_rpc import DirectRPCCommandClient
from contextlib import contextmanager
# Parameterized fixture to return a persistent state at a parameterized path

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
def leader_actor(direct_rpc_client):
    with (
        persisted_state("/tmp/leader-test-state.db") as ps,
        raft_log("/tmp/leader-test-log.db") as log
    ):
        NODE_ID = 0
        node_state = RaftNodeState(
            node_id=NODE_ID,
            election_timeout=10,
            commit_index=0,
            last_applied_index=0,
            leader_id=0,
            is_candidate=False,
            peers=[1, 2],
            peer_match_index={
                1: 0,
                2: 0
            },
            peer_next_index={
                1: 0,
                2: 0
            }
        )
        
        actor = RaftNodeActor.start(
            node_state=node_state,
            persisted_state=ps,
            log=log,
            rpc_client=direct_rpc_client,
        ).proxy()

        direct_rpc_client.register_recipient(NODE_ID, actor)

        yield actor

    actor.stop()

@pytest.fixture
def follower_actors(direct_rpc_client):
    with (
        persisted_state("/tmp/follower-1-test-state.db") as ps_1,
        raft_log("/tmp/follower-1-test-log.db") as log_1,
        persisted_state("/tmp/follower-2-test-state.db") as ps_2,
        raft_log("/tmp/follower-2-test-log.db") as log_2
    ):
        NODE_ID_1 = 1
        node_state = RaftNodeState(
            node_id=NODE_ID_1,
            election_timeout=10,
            commit_index=0,
            last_applied_index=0,
            leader_id=0,
            is_candidate=False,
            peers=[0, 2],
            peer_match_index={
                0: 0,
                2: 0
            },
            peer_next_index={
                0: 0,
                2: 0
            }
        )
        
        actor_1 = RaftNodeActor.start(
            node_state=node_state,
            persisted_state=ps_1,
            log=log_1,
            rpc_client=direct_rpc_client,
        ).proxy()

        direct_rpc_client.register_recipient(NODE_ID_1, actor_1)

        NODE_ID_2 = 2
        node_state = RaftNodeState(
            node_id=NODE_ID_2,
            election_timeout=10,
            commit_index=0,
            last_applied_index=0,
            leader_id=0,
            is_candidate=False,
            peers=[0, 1],
            peer_match_index={
                0: 0,
                1: 0

            },
            peer_next_index={
                0: 0,
                1: 0
            }
        )
        
        actor_2 = RaftNodeActor.start(
            node_state=node_state,
            persisted_state=ps_2,
            log=log_2,
            rpc_client=direct_rpc_client,
        ).proxy()

        direct_rpc_client.register_recipient(NODE_ID_2, actor_2)

        yield [actor_1, actor_2]

    actor_1.stop()
    actor_2.stop()


def test_direct_rpc_works(direct_rpc_client, follower_actors):
    log = [
        LogEntry(term=0, command="foo"),
        LogEntry(term=0, command="bar"),
    ]

    direct_rpc_client.send_command(
        node_id=1,
        cmd="append_log_entries",
        request_body=AppendEntriesRequest(
            term=0,
            leader_id=0,
            prev_log_index=-1,
            prev_log_term=0,
            entries=log,
            leader_commit=0
        )
    )

    assert follower_actors[0].log.get().get_entries() == log

def test_send_command_to_leader_succeeds_and_replicates_to_follower(direct_rpc_client, leader_actor, follower_actors):
    resp = direct_rpc_client.send_command(
        node_id=0,
        cmd="new_command",
        request_body=NewCommandRequest(
            command="foo"
        )
    )

    assert resp.success
    assert leader_actor.log.get().get_entries() == [LogEntry(term=0, command='foo')]
    for follower in follower_actors:
        assert follower.log.get().get_entries() == [LogEntry(term=0, command='foo')]
        assert follower.log.get().get_entry_at_index(0) == LogEntry(term=0, command='foo')
    
    resp = direct_rpc_client.send_command(
        node_id=0,
        cmd="new_command",
        request_body=NewCommandRequest(
            command="bar"
        )
    )
    assert resp.success
    assert leader_actor.log.get().get_entries() == [LogEntry(term=0, command='foo'), LogEntry(term=0, command='bar')]
    for follower in follower_actors:
        assert follower.log.get().get_entries() == [LogEntry(term=0, command='foo'), LogEntry(term=0, command='bar')]
        assert follower.log.get().get_entry_at_index(1) == LogEntry(term=0, command='bar')

def test_send_command_to_leader_fails_if_follower_rejects(direct_rpc_client, leader_actor, follower_actors):
    log = [
        LogEntry(term=0, command="foo"),
        LogEntry(term=0, command="bar"),
    ]

    # Put the follower into a state where it is on
    # term 1 via a message from node 2 (not visible to node 0 'leader')
    direct_rpc_client.send_command(
        node_id=1,
        cmd="append_log_entries",
        request_body=AppendEntriesRequest(
            term=1,
            leader_id=2,
            prev_log_index=-1,
            prev_log_term=0,
            entries=log,
            leader_commit=0
        )
    )
    assert follower_actors[0].log.get().get_entries() == log
    assert follower_actors[0].persisted_state.get().get_current_term() == 1

    assert leader_actor.node_state.get().is_leader
    resp = direct_rpc_client.send_command(
        node_id=0,
        cmd="new_command",
        request_body=NewCommandRequest(
            command="foo"
        )
    )
    assert not resp.success
    assert not leader_actor.node_state.get().is_leader # no longer leader

def test_send_command_to_leader_with_blocked_comms(direct_rpc_client, leader_actor, follower_actors):
    # Block comms to 2 so that 2 doesn't get the message
    direct_rpc_client.block_communication(2)

    resp = direct_rpc_client.send_command(
        node_id=0,
        cmd="new_command",
        request_body=NewCommandRequest(
            command="foo"
        )
    )

    assert resp.success
    assert leader_actor.log.get().get_entries() == [LogEntry(term=0, command='foo')]
    assert follower_actors[0].log.get().get_entries() == [LogEntry(term=0, command='foo')]
    assert follower_actors[1].log.get().get_entries() == []

    direct_rpc_client.unblock_communication(2)

    leader_actor.tick().get()
    
    assert follower_actors[0].log.get().get_entries() == [LogEntry(term=0, command='foo')]
    assert follower_actors[1].log.get().get_entries() == [LogEntry(term=0, command='foo')]

def test_tick_catches_up_missing_follower_with_gaps(direct_rpc_client, leader_actor, follower_actors):
    pass