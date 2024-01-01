package index

import (
	"bytes"
	"sort"
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

// Item btree索引的item
type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

// Less 比较两个item的大小，用于btree的排序
func (item *Item) Less(that btree.Item) bool {
	return bytes.Compare(item.Key, that.(*Item).Key) == -1
}

// NewBTree 创建btree索引
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		mu:   new(sync.RWMutex),
	}
}

// Put 添加索引
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

// Get 获取索引
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key, nil}
	res := bt.tree.Get(it)
	if res == nil {
		return nil
	}
	return res.(*Item).Pos
}

// Delete 删除索引
func (bt *BTree) Delete(key []byte) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	it := &Item{key, nil}
	olditem := bt.tree.Delete(it)
	return olditem != nil
}

// Size 返回索引中的key数量
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// Iterator 返回一个迭代器
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	return NewBTreeIterator(bt.tree, reverse)
}

// BTree Iterator 迭代器
type BTreeIterator struct {
	currIndex int     // 当前遍历到的索引
	reverse   bool    // 是否反向遍历
	values    []*Item // key 位置信息
}

// NewBTreeIterator 创建btree迭代器
func NewBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var idx int = 0
	values := make([]*Item, tree.Len())

	// saveValues 保存遍历到的item
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		// Descend 从大到小遍历
		tree.Descend(saveValues)
	} else {
		// Ascend 从小到大遍历
		tree.Ascend(saveValues)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个key
func (bit *BTreeIterator) Rewind() {
	bit.currIndex = 0
}

// Seek 根据传入的key，定位到第一个大于等于（或者小于等于）key的位置，然后从该位置开始迭代
func (bit *BTreeIterator) Seek(key []byte) {
	if bit.reverse {
		// Search 从大到小遍历，找到第一个小于等于key的位置
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].Key, key) <= 0
		})
	} else {
		// Search 从小到大遍历，找到第一个大于等于key的位置
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].Key, key) >= 0
		})
	}
}

// Next 迭代到下一个key
func (bit *BTreeIterator) Next() {
	bit.currIndex += 1
}

// Valid 判断迭代器是否有效
func (bit *BTreeIterator) Valid() bool {
	return bit.currIndex < len(bit.values)
}

// Key 获取当前迭代到的key
func (bit *BTreeIterator) Key() []byte {
	return bit.values[bit.currIndex].Key
}

// Value 获取当前迭代到的value
func (bit *BTreeIterator) Value() *data.LogRecordPos {
	return bit.values[bit.currIndex].Pos
}

// Close 关闭迭代器
func (bit *BTreeIterator) Close() {
	bit.values = nil
}
