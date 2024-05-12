import pytest
import os
from raft.node.persisted_state import PersistedState
from raft.node.actor import RaftNodeActor, RaftNodeState
from raft.node.commands import AppendEntriesRequest, LogEntry

@pytest.fixture
def persisted_state():
    db_path = "/tmp/test.db"
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass
    
    ps = PersistedState(db_path, current_term=0, voted_for=None)
    yield ps
    os.remove(db_path)

@pytest.fixture
def actor(persisted_state):
    node_state = RaftNodeState(
        node_id=0,
        leader_id=1,
        is_candidate=False,
    )
    
    actor = RaftNodeActor.start(
        node_state=node_state,
        persisted_state=persisted_state,
    ).proxy()

    yield actor

    actor.stop()

def test_log_append_at_zero(actor):
    request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=-1,
        prev_log_term=0,
        entries=[
            LogEntry(term=0, command="foo"),
            LogEntry(term=0, command="bar"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == request.entries
    
def test_log_append_valid_second_after_first(actor):
    final_log = [
        LogEntry(term=0, command="foo"),
        LogEntry(term=0, command="bar"),
        LogEntry(term=0, command="baz"),
        LogEntry(term=0, command="m4"),
    ]

    first_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=-1,
        prev_log_term=0,
        entries=final_log[:2],
        leader_commit=0
    )

    res = actor.append_log_entries(first_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == first_request.entries

    second_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=0,
        entries=final_log[2:3],
        leader_commit=0
    )

    res = actor.append_log_entries(second_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == final_log[:3]

    # the append log is idempotent with identical requests
    third_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=0,
        entries=final_log[2:3],
        leader_commit=0
    )

    res = actor.append_log_entries(third_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == final_log[:3]

    # the append log is idempotent with overlapping inputs
    third_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=0,
        entries=final_log[2:],
        leader_commit=0
    )

    res = actor.append_log_entries(third_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == final_log


def test_log_append_empty(actor):
    first_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=-1,
        prev_log_term=0,
        entries=[
            LogEntry(term=0, command="foo"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(first_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == first_request.entries

    # Valid empty request
    second_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0
    )

    res = actor.append_log_entries(second_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}

    # Invalid empty request because of term mismatch
    second_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=1,
        entries=[],
        leader_commit=0
    )

    res = actor.append_log_entries(second_request).get()
    assert res.model_dump() =={"success": False, "term": 0, "reason": "follower log diverged"}

def test_log_append_fail_if_missing_entries(actor):
    first_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=-1,
        prev_log_term=0,
        entries=[
            LogEntry(term=0, command="foo"),
            LogEntry(term=0, command="bar"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(first_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == first_request.entries

    second_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=5, # implies missing data on follower
        prev_log_term=0,
        entries=[
            LogEntry(term=0, command="baz"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(second_request).get()
    assert res.model_dump() =={"success": False, "term": 0, "reason": "follower log incomplete"}

def test_log_append_fail_if_term_mismatch(actor):
    first_request = AppendEntriesRequest(
        term=0,
        leader_id=1,
        prev_log_index=-1,
        prev_log_term=0,
        entries=[
            LogEntry(term=0, command="foo"),
            LogEntry(term=0, command="bar"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(first_request).get()
    assert res.model_dump() =={"success": True, "term": 0, "reason": ""}
    assert actor.log.get().get_entries() == first_request.entries

    # Invalid request because the term of the log entry
    # at prev_log_index doesn't match prev_log_term. 
    second_request = AppendEntriesRequest(
        term=1,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=1, # implies the log entry at 1 on the follower is wrong/diverged
        entries=[
            LogEntry(term=1, command="baz"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(second_request).get()
    assert res.model_dump() =={"success": False, "term": 0, "reason": "follower log diverged"}

    # The leader rolls back to the previous entry, which does
    # match the term, and the append succeeds.
    third_request = AppendEntriesRequest(
        term=1,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[
            LogEntry(term=1, command="baz"),
        ],
        leader_commit=0
    )

    res = actor.append_log_entries(third_request).get()
    assert res.model_dump() =={"success": True, "term": 1, "reason": ""}