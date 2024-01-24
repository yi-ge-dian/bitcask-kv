package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
	"github.com/yi-ge-dian/bitcask-kv/data"
)

// BTree
// Use the btree to implement the index
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

// NewBTree
// Returns a new BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		mu:   new(sync.RWMutex),
	}
}

// Put the index
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

// Get the index
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key, nil}
	res := bt.tree.Get(it)
	if res == nil {
		return nil
	}
	return res.(*Item).pos
}

// Delete the index
func (bt *BTree) Delete(key []byte) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	it := &Item{key, nil}
	olditem := bt.tree.Delete(it)
	return olditem != nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTreeIterator
type btreeIterator struct {
	// the subscript position of the current traversal
	currIndex int

	// whether a reverse traversal
	// true: from big to small
	// false: from small to big
	reverse bool

	// key + pos
	values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// store all the data in an array
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	// find the first key greater than (or less than) or equal to the passed key
	if bti.reverse {
		// if it is a reverse traversal, find the first key less than or equal to the passed key
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		// if it is a forward traversal, find the first key greater than or equal to the passed key
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
