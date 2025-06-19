package main

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"os"
	"strconv"
)

func main() {
	for i := 1; i < len(os.Args); i++ {
		badgerPath := os.Args[i]
		func(badgerPath string) {
			db, err := badger.Open(badger.DefaultOptions(badgerPath))
			if err != nil {
				panic(err)
			}
			defer db.Close()

			log := make(map[int]string)
			err = db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					keyBytes := item.Key()
					err := item.Value(func(valBytes []byte) error {
						key, err := strconv.Atoi(string(keyBytes))
						if err != nil {
							fmt.Println(err)
							return nil
						}
						val := string(valBytes)
						log[key] = val
						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				panic(err)
			}
			key := 0
			for {
				val, ok := log[key]
				if !ok {
					_, _ = fmt.Fprintf(os.Stderr, "stopped with %d keys left\n", len(log))
					break
				}
				fmt.Printf("%d: %s\n", key, val)
				delete(log, key)
				key++
			}
		}(badgerPath)
	}
}
