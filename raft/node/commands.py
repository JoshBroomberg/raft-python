from collections import defaultdict
from pydantic import BaseModel
from typing import Optional

APPEND_LOG_ENTRIES = "append_log_entries"
REQUEST_VOTE = "request_vote"
NEW_COMMAND = "new_command"
DUMP_STATE = "dump_state"
PING = "ping"
FORCE_ELECTION = "force_election"

class PingRequestBody(BaseModel):
    x: str
    
class PingResponseBody(BaseModel):
    x: str

class LogEntry(BaseModel):
    term: int
    command: str

class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    reason: str

class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int

class RequestVoteResponse(BaseModel):
    term: int
    vote_granted: bool
    reason: str

class NewCommandRequest(BaseModel):
    command: str

class NewCommandResponse(BaseModel):
    success: bool
    leader_id: Optional[int]
    reason: str

class NOOPRequest(BaseModel):
    pass

class DumpStateResponse(BaseModel):
    len_log: int
    commit_index: int
    last_applied_index: int


class ForceElectionResponse(BaseModel):
    won_election: bool


class Error(BaseModel):
    error: str

COMMAND_TO_REQUEST_MODEL = {
    APPEND_LOG_ENTRIES: AppendEntriesRequest,
    REQUEST_VOTE: RequestVoteRequest,
    NEW_COMMAND: NewCommandRequest,
    DUMP_STATE: NOOPRequest,
    PING: PingRequestBody,
    FORCE_ELECTION: NOOPRequest,
}

COMMAND_TO_RESPONSE_MODEL = {
    APPEND_LOG_ENTRIES: AppendEntriesResponse,
    REQUEST_VOTE: RequestVoteResponse,
    NEW_COMMAND: NewCommandResponse,
    DUMP_STATE: DumpStateResponse,
    PING: PingResponseBody,
    FORCE_ELECTION: ForceElectionResponse,
}

COMMAND_IS_ASYNC = defaultdict(bool)
COMMAND_IS_ASYNC[NEW_COMMAND] = True