package dist_kvstore

import (
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
)

type Entry struct {
	Key string `json:"key"`
	Val string `json:"val"`
	Ver uint64 `json:"ver"`
}

type Cmd = Entry

type stateMachine struct {
	store kvstore.MemStore[string, Entry]
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		store: kvstore.NewMemStore[string, Entry](),
	}
}

func (sm *stateMachine) Get(key string) Entry {
	return sm.store.Update(func(txn kvstore.Txn[string, Entry]) any {
		return getDefaultEntry(txn, key)
	}).(Entry)
}

func (sm *stateMachine) Keys() []string {
	return sm.store.Update(func(txn kvstore.Txn[string, Entry]) any {
		return sm.store.Keys()
	}).([]string)
}

func (sm *stateMachine) Apply(logId paxos.LogId, cmd Cmd) {
	sm.store.Update(func(txn kvstore.Txn[string, Entry]) any {
		entry := cmd
		oldEntry := getDefaultEntry(txn, entry.Key)
		if entry.Ver <= oldEntry.Ver {
			return nil // ignore update
		}
		if len(entry.Val) == 0 {
			txn.Del(entry.Key)
		} else {
			txn.Set(entry.Key, entry)
		}
		return nil
	})
}
