package index

import (
	"sync"

	"github.com/google/btree"
	"github.com/yi-ge-dian/bitcask-kv/data"
)

type BTree struct {
	tree *btree.BTree  // btree
	lock *sync.RWMutex // lock
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key, pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{key, nil}
	res := bt.tree.Get(it)
	if res == nil {
		return nil
	}
	return res.(Item).Pos
}

func (bt *BTree) Delete(key []byte) bool {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	it := Item{key, nil}
	res := bt.tree.Delete(it)
	return res != nil
}
