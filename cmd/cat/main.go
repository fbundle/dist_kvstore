package main

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"os"
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

			data := make(map[string]string)
			err = db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					keyBytes := item.Key()
					err := item.Value(func(valBytes []byte) error {
						key := string(keyBytes)
						if err != nil {
							fmt.Println(err)
							return nil
						}
						val := string(valBytes)
						data[key] = val
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

			for k, v := range data {
				_, _ = fmt.Fprintf(os.Stdout, "%s -> %s\n", k, v)
			}
		}(badgerPath)
	}
}
