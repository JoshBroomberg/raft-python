import copy
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, TimeoutError
import queue
import logging
import random
import pykka
import threading
import time
from textwrap import dedent
from typing import Dict, List, Optional

from raft.kv.actor import KeyValue
from raft.node.commands import *
from raft.node.persisted_state import PersistedState
from raft.node.log import RaftLog
from raft.network.outbound_rpc import RPCCommandClient, NetworkRPCCommandClient
from raft.config import HOSTS
from dataclasses import dataclass

logger = logging.getLogger(__name__)

TERM_MISMATCH = "term mismatch"
FOLLOWER_LOG_INCOMPLETE="follower log incomplete"
FOLLOWER_LOG_DIVERGED="follower log diverged"

NOT_LEADER="not leader"
IS_LEADER="is leader"

TOO_EARLY="too early to vote"
ALREADY_VOTED="already voted"
LOG_INCOMPLETE="log incomplete"

MIN_ELECTION_TIMEOUT = 10
MAX_ELECTION_TIMEOUT = 50

class NoLongerLeaderException(Exception):
    pass

@dataclass
class RaftNodeState():
    node_id: int
    is_candidate: bool = False
    leader_id: Optional[int] = None

    election_timeout: Optional[int] = None
    commit_index: Optional[int] = None
    last_applied_index: Optional[int] = None

    peers: Optional[List[int]] = None
    peer_next_index: Optional[Dict[int, int]] = None
    peer_match_index: Optional[Dict[int, int]] = None

    ticks_since_leader_message: int = 0

    def __post_init__(self):
        if self.election_timeout is None:
            self.election_timeout = random.randint(
                MIN_ELECTION_TIMEOUT,
                MAX_ELECTION_TIMEOUT,
            )
        
        if self.commit_index is None:
            self.commit_index = -1
        
        if self.last_applied_index is None:
            self.last_applied_index = -1

        assert not (self.is_leader and self.is_candidate)
        assert self.election_timeout > 0
        assert self.commit_index >= -1
        assert self.last_applied_index >= -1
        assert self.commit_index >= self.last_applied_index
        
        if self.peers is None:
            self.peers = [p for p in HOSTS if p != self.node_id]
        else:
            assert self.node_id not in self.peers

        if self.peer_match_index is not None:
            for peer in self.peers:
                assert peer in self.peer_match_index and self.peer_match_index[peer] <= self.commit_index
            assert len(self.peers) == len(self.peer_match_index)

        if self.peer_next_index is not None:
            for peer in self.peers:
                assert peer in self.peer_next_index and self.peer_next_index[peer] <= self.commit_index

            assert len(self.peers) == len(self.peer_next_index)

    @property
    def is_follower(self) -> bool:
        return not (self.is_candidate or self.is_leader)
    
    @property
    def is_leader(self) -> bool:
        return (self.leader_id == self.node_id)


class RaftNodeActor(pykka.ThreadingActor):
    use_daemon_thread = True

    def __init__(
            self,
            node_state: RaftNodeState,
            persisted_state: PersistedState,
            log: RaftLog = None,
            rpc_client: RPCCommandClient = None,
        ):
        super().__init__()
        self.node_state = node_state
        self.persisted_state = persisted_state
        self.log = log if log is not None else RaftLog()

        self.log = log if log is not None else RaftLog()
        self.rpc_client = rpc_client if rpc_client is not None else NetworkRPCCommandClient()

        if self.node_state.is_leader and (
            self.node_state.peer_match_index is None
            or
            self.node_state.peer_next_index is None
        ):
            self._init_peer_state()

        # TODO: integrate the KV app in an eventually consistent way
        # self.kv = KeyValueActor.start().proxy()
        
        self.entries_pending_commit = threading.Event()
        self.entries_were_committed = threading.Event()
        
        def commit_log_entries():
            while True:
                self.entries_pending_commit.wait()
                self.entries_pending_commit.clear()
                
                logger.info("Leader node %s is processing new entries for commit", self.node_state.node_id)
                
                if not self.node_state.is_leader:
                    continue

                required_acks = len(self.node_state.peers) // 2
                sorted_match_indexes = sorted(self.node_state.peer_match_index.values())[::-1]
                
                # The match index at the required acks - 1 in the sorted
                # list is the smallest match index on required acks + leader
                new_commit_index = sorted_match_indexes[required_acks-1]
                if new_commit_index > self.node_state.commit_index:
                    logger.info("Leader %s committing index %s", self.node_state.node_id, new_commit_index)
                    self._update_commit_index(new_commit_index)
                    
                    # Unlatch all client listeners
                    self.entries_were_committed.set()
                    self.entries_were_committed.clear()

        # TODO: check this thread is alive?
        threading.Thread(
            target=commit_log_entries,
            daemon=True
        ).start()

        self.commits_pending_apply = threading.Event()
        self.commits_were_applied = threading.Event()
        
        def apply_log_entries():
            while True:
                self.commits_pending_apply.wait()
                self.commits_pending_apply.clear()

                logger.info("Node %s is processing new entries for apply", self.node_state.node_id)

                last_commit = self.node_state.commit_index
                last_applied = self.node_state.last_applied_index
                n_to_apply = last_commit - last_applied

                if n_to_apply > 0:
                    # Lock out threads waiting to consume
                    self.commits_were_applied.clear()
                    for _ in range(n_to_apply):
                        self.node_state.last_applied_index += 1
                        entry = self.log.get_entry_at_index(self.node_state.last_applied_index)
                        logger.info("Node %s applied command %s", self.node_state.node_id, entry.command)
                    
                        # TODO: integrate the KV app here
                        # TODO: need a command structure that can be applied
                        # self.kv.apply(entry.command)

                    self.commits_were_applied.set()
        
        # TODO: check this thread is alive?
        threading.Thread(
            target=apply_log_entries,
            daemon=True
        ).start()

    def on_stop(self) -> None:
        self.rpc_client.close()
        return super().on_stop()

    def tick(self):
        if self.node_state.is_leader:
            current_term = self.persisted_state.get_current_term()
            self._replicate_log_to_followers_v2(current_term)

            # TODO: check if you have managed to run a successful quorum
            # in a period shorter than election timeout. If not, revert
            # to the follower state to avoid deaf leader keeping the cluster
            # in a partially broken state.
        else:
            # If not leader, increment time since hearing from leader
            self.node_state.ticks_since_leader_message += 1

            if self.node_state.is_candidate:
                # We only trigger a vote if either we haven't voted
                # or if we have voted but we have exceeded timeout
                election_timeout = self.node_state.ticks_since_leader_message >= self.node_state.election_timeout
                no_election = self.persisted_state.get_voted_for() is None
                if no_election or election_timeout:
                    self._attempt_election()


            elif self.node_state.is_follower:
                if self.node_state.ticks_since_leader_message >= self.node_state.election_timeout:
                    # If follower, and haven't heard from then become a candidate
                    # TODO: immediate election?
                    self._become_candidate()
    
    ################ UTIL ####################

    def dump_state(self, request: NOOPRequest):
        logger.info(self._state_str())
        logger.info("NODE_LOG: %s", self.log.get_entries())
        return DumpStateResponse(
            len_log=self.log.get_length(),
            commit_index=self.node_state.commit_index,
            last_applied_index=self.node_state.last_applied_index
        )
    
    def force_election(self, request: NOOPRequest):
        '''
        Simulates a network brownout where a follower becomes a candidate
        '''
        if self.node_state.is_leader:
            raise ValueError("Can't force from the leader")
        else:
            self._become_candidate()
            result = self._attempt_election()
            return ForceElectionResponse(won_election=result)
        
    ################## RPC  ####################

    def request_vote(self, request: RequestVoteRequest):
        """
        MODIFICATION FROM PAPER: reject if you're leader even if term is out of date.

        - check if not already voted or vote matches requester
          - if yes, check if requesters log is up to date (Term >= follower term, index >= index in that term)
            - if yes, record vote and reply (blocks becoming candidate), set heard from leader
            - if not, reject with current term etc
          - if not, then reject

        """

        current_term = self.persisted_state.get_current_term()

        # Reject request to vote if you are the leader
        if self.node_state.is_leader:
            return RequestVoteResponse(
                term=current_term,
                vote_granted=False,
                reason=IS_LEADER
            )
        

        # If requester has smaller term, let them know so that they revert to follower
        if request.term < current_term:
            return RequestVoteResponse(
                term=current_term,
                vote_granted=False,
                reason=TERM_MISMATCH
            )

        # Guards against a candidate that has heard from a leader voting for another leader.
        if self.node_state.leader_id is not None and self.node_state.ticks_since_leader_message < MIN_ELECTION_TIMEOUT:
            return RequestVoteResponse(
                term=current_term,
                vote_granted=False,
                reason=TOO_EARLY
            )
        
        # If term of request is higher than current term, 
        # update to that term and reset voted_for. 
        # May not end up voting for this candidate due to log completeness
        # but we are now genuinely in a new term and can vote for anyone.
        if request.term > current_term:
            current_term = request.term
            voted_for = None
            self._update_term(new_term=current_term, voted_for=voted_for)
        
        # This is a vote request from the current term (perhaps another candidate)
        else:
            voted_for = self.persisted_state.get_voted_for()

        if voted_for is None or voted_for == request.candidate_id:
            local_last_index = self.log.get_length() - 1
            local_last_term = self.log.get_entry_at_index(local_last_index).term if local_last_index >= 0 else 0

            more_recent_term = request.last_log_term > local_last_term
            more_recent_index = request.last_log_index >= local_last_index

            if more_recent_term or more_recent_index:
                # Grant vote and reset timeout, blocking new votes
                # until min election timeout has elapsed.
                self.persisted_state.set_voted_for(request.candidate_id)
                self.node_state.ticks_since_leader_message = 0
                
                return RequestVoteResponse(
                    term=current_term,
                    vote_granted=True,
                    reason=""
                )
            else:
                return RequestVoteResponse(
                    term=current_term,
                    vote_granted=False,
                    reason=LOG_INCOMPLETE
                )
        else:
            return RequestVoteResponse(
                term=current_term,
                vote_granted=False,
                reason=ALREADY_VOTED
            )
                

    def append_log_entries(self, request: AppendEntriesRequest):
        current_term = self.persisted_state.get_current_term()
        
        if len(request.entries) > 0:
            logger.info(
                "Node %s got log entries from leader %s",
                self.node_state.node_id,
                request.leader_id
            )

        if request.term < current_term:
            return AppendEntriesResponse(
                term=current_term,
                success=False,
                reason=TERM_MISMATCH
            )
        else:
            
            # This collapses a few code paths together. If request.term >= current_term
            # this results in updating the term and leader.
            # - If you're a candidate/leader, this is a state change + term change.
            # - If you're a follower, this may just be a term change.
            # There may be times you are already in this term with the right leader,
            # and then this is a NOOP.
            # TODO: there are some code paths here with an unnecessary durable write.
            self._become_follower(current_term, request.term, request.leader_id)
            
            # TODO: this is a NOOP as it's done in the become follower
            # but I may try break up that method into smaller parts
            self.node_state.ticks_since_leader_message = 0

            # NOTE: leader sends prev_log_index = -1 if no log
            # should be present on follower. This skips all log completeness checks
            # TODO: should there be some kind of check that if the leader is claiming
            # no logs, you don't already have a log from a higher term?
            # I think that is handled by the term check above?

            if request.prev_log_index >= 0:
                # TODO: handle compaction where log may not be complete
                # will require log to store 'length' separate from actual content
                if request.prev_log_index >= self.log.get_length():
                    # TODO: optimize by sending a hint on earliest index for term
                    return AppendEntriesResponse(
                        term=current_term,
                        success=False,
                        reason=FOLLOWER_LOG_INCOMPLETE
                    )
                
                # check if log diverged where term at prev log index is different
                prev_log_entry = self.log.get_entry_at_index(request.prev_log_index)
                if prev_log_entry.term != request.prev_log_term:
                    return AppendEntriesResponse(
                        term=current_term,
                        success=False,
                        reason=FOLLOWER_LOG_DIVERGED
                    )

            
            # wipe and append
            log_index_for_insert = (request.prev_log_index + 1)
            self.log.insert_entries_at_index(log_index_for_insert, request.entries)
            
            # new length is index + count of entries
            # eg when inserting one item at index 1 in list [0], the new length is 2
            new_log_length = log_index_for_insert + len(request.entries)
            assert new_log_length == self.log.get_length() # TODO: remove when confident
            
            # set commit to leader commit or one less than current log length (the last index in the log)
            self._update_commit_index(min(request.leader_commit, new_log_length-1))

            # TODO: apply all between lastApplied and commit to the state machine
            # Do this async? What if it doesn't persist? Where is last applied stored?
            return AppendEntriesResponse(
                term=request.term,
                success=True,
                reason=""
            )

    ################## CLIENT ##################

    def new_command(self, request: NewCommandRequest):
        if not self.node_state.is_leader:
            resp = NewCommandResponse(
                success=False,
                leader_id=self.node_state.leader_id,
                reason=NOT_LEADER
            )
            f = Future()
            f.set_result(resp)
            return f
        else:
            assert self.node_state.node_id == self.node_state.leader_id

        current_term = self.persisted_state.get_current_term()
        
        new_entry = LogEntry(
            term=current_term,
            command=request.command
        )
        
        # TODO: support batching in this command
        # such that new new commands can go into one commit cycle.
        new_entries = [new_entry]
        
        # Length of log = next empty slot
        # when length 0, inserting 1 at 0, shifts commit index to 0
        # when length N, inserting M at index N, shifts commit index to N + M - 1
        next_index = self.log.get_length()
        new_commit_index = next_index + len(new_entries) - 1

        logger.info("Inserting new command %s at index %s", request.command, next_index)
        self.log.insert_entries_at_index(next_index, [new_entry])
        
        # Start this in the main (blocking) thread so that the messages
        # constructed respect order and state of the log as of each message from the client.
        self._replicate_log_to_followers_v2(current_term)

        def wait_for_commit(response_future):
            # TODO: this is some longer client-facing timeout
            
            # Loop until your command is committed. This can be due
            # to the RPC above completing or due to a later command
            while True:
                self.entries_were_committed.wait()
                logger.info("Client thread polling for commit %s", new_commit_index)
                if self.node_state.commit_index >= new_commit_index:
                    logger.info("Client thread found commit %s", new_commit_index)
                    break
            
            # while True:
            #     self.commits_were_applied.wait()
            #     logger.info("Client thread checking for apply %s", new_commit_index)
            #     if self.node_state.last_applied_index >= new_commit_index:
            #         logger.info("Client thread found apply %s", new_commit_index)
            #         break

            # TODO: what happens if no longer leader? Need a way to get
            # a signal and fail hard here.
            resp = NewCommandResponse(
                success=True,
                leader_id=self.node_state.leader_id,
                reason=""
            )
            response_future.set_result(resp)
            return resp
            
        
        f = Future()
        t = threading.Thread(
            target=wait_for_commit,
            args=(f,),
            daemon=True
        )
        t.start()
        return f
    
    ################## INTERNAL ####################
    def _replicate_log_to_followers_v2(self, current_term: int) -> bool:
        
        # TODO: clean up this comment?
        # NOTE: was previously updating commit index in here so that it the commit
        # index was incremented on new command or heart-beat. Now only doing the update
        # when a new command is received if this method sends True
        #  so that we only commit a command from the leaders term.
        # It is unclear to us why  the commit index can't be safely updated on any replication.
        # (but then need to handle the starting case where the log is empty
        # and avoid 'committing' the next index in that case)
        q = queue.Queue()

        def send_request_to_peer(req, stable_peer_id):
            resp: AppendEntriesResponse = self.rpc_client.send_command(
                node_id=stable_peer_id,
                cmd=APPEND_LOG_ENTRIES,
                request_body=req,
                timeout=1,
            )

            return q.put({
                "peer_id": stable_peer_id,
                "request": req,
                "response": resp
            })
        
        peer_to_request = {}
        for peer_id in self.node_state.peers:
            # NOTE: When leader log is empty, the peer_next_index is set to log length (0)
            # which means prev_index is -1. This is a special case where the follow will
            # not run any parity checks. I think this is correct. If the leader is leader
            # and is empty, then it's right to force empty the followers.
            prev_index = self.node_state.peer_next_index[peer_id] - 1
            prev_term = self.log.get_entry_at_index(prev_index).term if prev_index >= 0 else 0
            existing_entries_to_send = self.log.get_entries(from_index=prev_index+1)

            req = AppendEntriesRequest(
                term=current_term,
                leader_id=self.node_state.node_id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=existing_entries_to_send,
                leader_commit=self.node_state.commit_index
            )
            peer_to_request[peer_id] = req

        def execute_requests():
            for peer_id, req in peer_to_request.items():
                threading.Thread(
                    target=send_request_to_peer,
                    args=(req, copy.deepcopy(peer_id),),
                    daemon=True
                ).start()
            
            # TODO Set a global timeout, this is peers x 1 which is wrong
            APPEND_TIMEOUT = 1

            start = time.time()
            for _ in range(len(self.node_state.peers)):
                if time.time() - start > APPEND_TIMEOUT:
                    break
                
                # Wait at most the max timeout, ideally much less.
                try:
                    response = q.get(timeout=APPEND_TIMEOUT)
                except queue.Empty:
                    break

                peer_id = response["peer_id"]
                resp = response["response"]
                req = response["request"]

                # TODO: verify the claim below
                # I think the mutations below are thread safe
                # due to ordering properties of the messages and responses
                # to the follower. Even though we may have N messages
                # in flight, the responses will be in order such that
                # sending new [1], [1, 2] would result in index_as_of_request
                # setting peer_next_index correctly.
                if resp is not None and resp.success:
                    index_as_of_request = req.prev_log_index + len(req.entries)
                    self.node_state.peer_next_index[peer_id] = index_as_of_request + 1
                    self.node_state.peer_match_index[peer_id] = index_as_of_request
                    
                    # TODO: this would be more efficient if it only
                    # triggered if the client actually updated based
                    # on the message. This triggers N times for NOOPs
                    if len(req.entries) > 0:
                        self.entries_pending_commit.set()

                elif resp is not None and not resp.success:
                    if (resp.reason in {FOLLOWER_LOG_INCOMPLETE, FOLLOWER_LOG_DIVERGED}): 
                        self.node_state.peer_next_index[peer_id] -= 1
                    elif resp.reason == TERM_MISMATCH:
                        # TODO: capture the leader here
                        # TODO: signal to client threads to cancel
                        self._become_follower(current_term, resp.term, None)
                        return
                    
        threading.Thread(
            target=execute_requests,
            daemon=True
        ).start()

    # TODO: think through timeout logic, factor timeout to a constant
    def _request_votes(self, term: int) -> bool:
        # TODO: switch to thread pool here
        # TODO: refactor this non-blocking RPC to a shared function
        # that can be used for replicate and request

        last_index = self.log.get_length() - 1
        last_term = self.log.get_entry_at_index(last_index).term if last_index >= 0 else 0

        req = RequestVoteRequest(
            term=term,
            candidate_id=self.node_state.node_id,
            last_log_index=last_index,
            last_log_term=last_term
        )
        
        q = queue.Queue()
        for peer_id in self.node_state.peers:
            def send_request_to_peer(req, stable_peer_id):
                resp: RequestVoteResponse = self.rpc_client.send_command(
                    node_id=stable_peer_id,
                    cmd=REQUEST_VOTE,
                    request_body=req,
                    timeout=1,
                )
                q.put({"peer_id": stable_peer_id, "request": req, "response": resp})
            
            threading.Thread(
                target=send_request_to_peer,
                args=(req, copy.deepcopy(peer_id),),
                daemon=True
            ).start()
        
        got_outcome = threading.Event()
        got_quorum = threading.Event()
        
        def process_responses(got_quorum, got_outcome):
            # TODO: validate that this always finishes all within N=timeout seconds
            processed = 0
            successes = 0

            assert len(self.node_state.peers) >= 2
            assert len(self.node_state.peers) % 2 == 0
            required_acks = len(self.node_state.peers) // 2

            for _ in range(len(self.node_state.peers)):
                response = q.get()
                processed += 1

                # TODO: cleanup unused
                peer_id = response["peer_id"]
                resp = response["response"]
                req = response["request"]

                if resp is not None:
                    logger.info(
                        "Node %s got vote response %s from node %s with reason %s",
                        self.node_state.node_id,
                        resp.vote_granted,
                        peer_id,
                        resp.reason
                    )
                else:
                    logger.info(
                        "Node %s got no vote response from node %s",
                        self.node_state.node_id,
                        peer_id
                    )

                if resp is not None and resp.vote_granted:
                    successes += 1
                
                elif resp is not None and not resp.vote_granted:
                    
                    # TODO: test this is working correctly
                    if resp.reason == TERM_MISMATCH:
                        # NOTE: the None here is intentional. Allows
                        # the now-follower to vote in this turn.
                        self._become_follower(term, resp.term, None)
                        got_outcome.set()

                if successes >= required_acks:
                    got_quorum.set()
                    got_outcome.set()
                
                # exit as soon as those remaining + successes
                # can't make up a successful quorum
                remaining = len(self.node_state.peers) - processed
                if remaining + successes < required_acks:
                    got_outcome.set()
        
        # Starting processing responses in a separate thread
        threading.Thread(
            target=process_responses,
            args=(got_quorum, got_outcome),
            daemon=True
        ).start()

        # Wait for an outcome or timeout
        # TODO: set a proper timeout here
        got_outcome.wait(timeout=1)
    
        if got_quorum.is_set():
            return True
        else:
            return False

    def _attempt_election(self):
        self.node_state.ticks_since_leader_message = 0
        
        # Increment term and vote for self in that term
        next_term = self.persisted_state.get_current_term() + 1
        self._update_term(new_term=next_term, voted_for=self.node_state.node_id)

        logger.info(
            "Node %s attempting election for term %s",
            self.node_state.node_id,
            next_term
        )

        did_win = self._request_votes(next_term)
        if did_win:
            self._become_leader()
        
        return did_win

    def _init_peer_state(self):
        self.node_state.peer_next_index = { p:self.log.get_length() for p in self.node_state.peers}
        self.node_state.peer_match_index = { p:-1 for p in self.node_state.peers}
    
    def _update_commit_index(self, new_index: int):
        # only signals for async processing if changed
        if new_index != self.node_state.commit_index:
            logger.info(
                "Node %s updating commit index from %s to %s",
                self.node_state.node_id,
                self.node_state.commit_index,
                new_index,
            )
            self.commits_pending_apply.set()
            self.node_state.commit_index = new_index
        
    def _update_term(self, new_term: int, voted_for: Optional[int]):
        self.persisted_state.set_current_term(new_term)
        self.persisted_state.set_voted_for(voted_for)

    def _become_candidate(self):
        logger.info(
            "Node %s became candidate",
            self.node_state.node_id,
        )

        assert self.node_state.is_follower
        self.node_state.is_candidate = True
        self.node_state.leader_id = None
        
        # TODO: should the candidate immediately trigger an election?
        # self._attempt_election()

    def _become_follower(self, current_term: int, new_term: int, leader_id: int):
        # TODO: need to unconflate the code paths here
        # Shouldn't be writing to durable store on every heartbeat
        if leader_id != self.node_state.leader_id or new_term != current_term:
            logger.info(
                "Node %s became follower of node %s",
                self.node_state.node_id,
                leader_id
            )
        
            assert leader_id != self.node_state.node_id
            self.node_state.is_candidate = False
            
            self.node_state.leader_id = leader_id

            self.node_state.ticks_since_leader_message = 0

            self._update_term(new_term=new_term, voted_for=leader_id)

    def _become_leader(self):
        logger.info(
            "Node %s became leader of term %s",
            self.node_state.node_id,
            self.persisted_state.get_current_term()
        )

        assert self.node_state.is_candidate
        assert not self.node_state.is_leader

        self.node_state.leader_id = self.node_state.node_id
        self.node_state.is_candidate = False

        self._init_peer_state()
        
        # TODO: needed?
        self.entries_pending_commit.clear()
        self.entries_were_committed.clear()
        self.commits_pending_apply.clear()
        self.commits_were_applied.clear()

        # TODO: send empty heartbeat to all, then can respond to reads
    
    def _state_str(self):
        return dedent(f"""
        ------------------------------------------
        Node ID: {self.node_state.node_id}
        Election Timeout: {self.node_state.election_timeout}
        Time Since Leader Message: {self.node_state.ticks_since_leader_message}
        Is Leader: {self.node_state.is_leader}
        Is Candidate: {self.node_state.is_candidate}
        Leader ID: {self.node_state.leader_id}
        Current Term: {self.persisted_state.get_current_term()}
        Voted For: {self.persisted_state.get_voted_for()}
        Commit Index: {self.node_state.commit_index}
        Last Applied Index: {self.node_state.last_applied_index}
        Persistence path: {self.persisted_state.db_path}
        Log path: {self.log.db_path}
        ------------------------------------------
        """)