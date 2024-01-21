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

type IndexType = int8

const (
	// Btree
	Btree IndexType = iota + 1

	// Adpative Radix Tree
	ART
)

// NewIndexer
// Create a new index given the index type
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
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
