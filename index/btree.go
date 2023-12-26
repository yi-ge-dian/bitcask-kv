package index

import (
	"sync"

	"github.com/google/btree"
	"github.com/yi-ge-dian/bitcask-kv/data"
)

// BTree 索引，封装了 google 的 btree
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		mu:   new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key, nil}
	res := bt.tree.Get(it)
	if res == nil {
		return nil
	}
	return res.(*Item).Pos
}

func (bt *BTree) Delete(key []byte) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	it := &Item{key, nil}
	olditem := bt.tree.Delete(it)
	return olditem != nil
}
