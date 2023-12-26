package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/yi-ge-dian/bitcask-kv/data"
)

// Indexer 通用索引接口
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type IndexType = byte

const (
	// BTreeIndex btree索引
	Btree IndexType = iota

	// 自适应基数树索引
	AdaptiveRadixTree
)

// NewIndexer 根据索引类型创建索引
func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case Btree:
		return NewBTree()
	case AdaptiveRadixTree:
		// todo
		return nil
	default:
		panic("invalid index type")
	}
}

type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

func (item *Item) Less(that btree.Item) bool {
	return bytes.Compare(item.Key, that.(*Item).Key) == -1
}
