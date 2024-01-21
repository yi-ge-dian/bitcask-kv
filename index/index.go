package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/yi-ge-dian/bitcask-kv/data"
)

// Indexer
// Abstracts the index
type Indexer interface {
	// Put the index
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get the index
	Get(key []byte) *data.LogRecordPos

	// Delete the index
	Delete(key []byte) bool
}

// Item
type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

// Less
// Compare the key of the item with the key of the that item
func (item *Item) Less(that btree.Item) bool {
	return bytes.Compare(item.Key, that.(*Item).Key) == -1
}
