# Python Raft

A WIP python implementation of the RAFT distributed consensus algorithm.

## TODO

- Integrate actual KV app into the apply mechanism
- Block reads until ready.
- Test figure 7 and figure 8 cases
- Batched log writes

## Run

```
python -m raft.run_rpc_server 0

...

python -m raft.run_rpc_client
Say > 1 new_command command=hello
```