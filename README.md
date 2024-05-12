# TODO

- DONE: External Tick Mechanic
- DONE Vote mechanics
- DONE Fix the init of next and match
- DONE Commit mechanics - check leader and follower logic
- DONE apply mechanic
- Integrate actual KV app into the apply mechanism
- Block reads until ready.
- Test figure 7 and figure 8 cases

DEBUG: 
- HALF DONE: Clean up RPC layer to consistently encode/decode
- DONE: Add more to dump the state
- DONE: Add more logging

SMALL
- Use threadpool instead of custom hacky bullshit
- Set heartbeat interval to ~2x min election timeout
- Add sequence number / UUID to commands in RPC layer or lock each socket
- Immediate election
- Noop via overwrite when match

Extensions
- Batched log writrs

```
python -m raft.run_rpc_server 0

...

python -m raft.run_rpc_client
Say > 1 echo
```

Rough Plan

- Each node is an Pykka Actor
- Each node has a driver program which listens for messages and issues heartbeats
- Each node has access to a service to send messages to all / specific clients

- Can mock the Send / Receive Service to run locally

## Components

### Log Component
- Entry is index + command + term
- If two entries have same index and term, they are the same
- If two entries have same index and term, all prior are identical
- Needs an upsert which wipes all logs from index onwards

### Persisted State Component
- currentTerm
- votedFor (in current term)
- Action: castVote

## Actors

### State Machine Actor (so that it can operate Async)
- presents the API which is specified in log entries

### Leader Actor API

State:
- nextIndex (for each follower)
- matchIndex (highest index known to match)

- Receive command (from client)
  - Replicate to all followers
  - When majority respond, commit and apply to state machine
    - This commits all prior terms and entries due to log matching
    - Increment matchIndex in here somewhere
  - Respond to client

### Follower API

- commitIndex
- lastApplied

- Append Log (term, leaderId, prvLogIndex, prevLogTerm, entries[], leaderCommit) ->(term: int, success: bool)
  - Store last hear from leader
  - If currentTerm > leader term, return false and term.
  - If in candidate state and term >= currentTerm, convert to follower.
  - Check if index + term of 'prior' entry matches own.
    - If not, return false. Leader decrements next index and tries again
    - When it finds agreement, the follower wipes all log entries after that point
    and subs in the new append logs
    - Optimize by having follower send the first index it has for the current term
    which skips a bunch of decrement by one checks
  - If agreement, append (wiping any at index or higher and replacing using log)
  - Set commit to the min(leaderCommit, lastLog)
  - [applyLogsProc] Apply all up to commit, and modify last applied. Do this async.


- Request vote (term, candidateId, lastLogIndex, lastLogTerm)
  - Reject if heard from leader within electionTime (need to store this)
  - Reject if term < currentTerm
  - reject unless voted is null or matches candidate
  - Reject if the requester is not at least as up to date. Term >= follower term, index >= index in that term
  - reset electionTimeout

- On any message:
  - if T > currentTerm, set currentTerm and revert to follower
  - reset electionTimeout to 0 (? - only if not a reject)

- On heartbeat:
  - If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate, convert to candidate
  - If leader, send heartbeat (with content or not) to all followers. Content if current index in your log > nextIndex for follower. 
    - if succeeded, update next and match
    - if failed, decrement next and try again
    - after all follower RPCs: if there is an N > commitIndex and majority of clients have a matchIndex >= N and log[N].term = currentTerm, commitIndex = N.
    - if updated N, [applyLogsProc] apply to state machine. Apply all up to commit, and modify last applied. Do this async?

State Transitions
- On convert to candidate
  - increment current term
  - vote for self
  - reset election timer
  - send out request votes
  - if majority, become leader
  - if receive append message for T >= current, become follower

- On convert to follower:
  - reset election timer

- On convert to leader:
  - Send out empty heartbeat broadcast immediately and ensure it's committed
  - You can then respond to read-only requests from followers


## Client
- Connect to random, possibly get redirect if not leader
- Assign serial number to requests. Servers reject if serial number already executed.
- If some timeout occurs, retry with random node

