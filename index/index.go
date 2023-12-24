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

type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

func (item *Item) Less(that btree.Item) bool {
	return bytes.Compare(item.Key, that.(*Item).Key) == -1
}
