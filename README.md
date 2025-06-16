# kvstore

implementation of distributed kvstore using paxos

paxos is easier to understand and prove. raft is unnecessarily complicated

[proof](https://github.com/khanh101/khanh101.github.io/blob/master/blog/pdf/paxos-algorithm.pdf)

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
# read
curl http://localhost:4000/kvstore/<key> -X GET
# write new key 
curl http://localhost:4000/kvstore/<key> -X PUT -d '{"val": "<value>", "ver": 1}'
# update key 
curl http://localhost:4000/kvstore/<key> -X PUT -d '{"val": "<value>", "ver": <ver>}'
# delete key
curl http://localhost:4000/kvstore/<key> -X PUT -d '{"val": "", "ver": <ver>}'
```


