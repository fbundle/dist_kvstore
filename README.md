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
# write 
curl http://localhost:4000/kvstore/key1 -X PUT -d "value1"
# read
curl http://localhost:4000/kvstore/key1 -X GET
# delete 
curl http://localhost:4000/kvstore/key1 -X DELETE
```


