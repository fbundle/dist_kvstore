# kvstore

implementation of distributed kvstore using paxos

paxos is easier to understand and prove unlike raft

[proof](https://github.com/khanh101/dist_kvstore/blob/master/docs/paxos.pdf)

[pkg.go.dev](https://pkg.go.dev/github.com/khanh101/paxos)

## EXAMPLE

cluster is online if and only if a quorum is online

```bash
go run main.go conf/kvstore.json 0
go run main.go conf/kvstore.json 1
go run main.go conf/kvstore.json 2
```

```bash
# get all keys
curl http://localhost:4000/kvstore/ -X GET
# read, unset key are with '{"val": "", "ver": 0}' by default 
curl http://localhost:4000/kvstore/<key> -X GET
# update key 
curl http://localhost:4000/kvstore/<key> -X PUT -d '{"val": "<value>", "ver": <ver>}'
# delete key
curl http://localhost:4000/kvstore/<key> -X PUT -d '{"val": "", "ver": <ver>}'
```

## RUN CLUSTER

```bash
# fire up cluster
./run_all "<private_key_for_rpc>" "<ip1>,<ip2>,<ip3>"
# stop running
./stop_all "<private_key_for_rpc>" "<ip1>,<ip2>,<ip3>"
```

## TODO 

- implement log compaction (3/5 difficulty, 4/5 complexity)

- implement leader election (3/5)

- implement dynamic membership changes (5/5)