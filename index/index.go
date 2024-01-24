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

	// Size of the index
	Size() int

	// Iterator of the index
	Iterator(reverse bool) Iterator
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
	key []byte
	pos *data.LogRecordPos
}

// Less
// Compare the key of the item with the key of the that item
func (item *Item) Less(that btree.Item) bool {
	return bytes.Compare(item.key, that.(*Item).key) == -1
}

// Iterator
// Abstracts the iterator
type Iterator interface {
	// Rewind, Go back to the starting point of the iterator, the first data
	Rewind()

	// Seek, Find the first target key greater than (or less than) or equal to the passed key,
	// and iterate from this key
	Seek(key []byte)

	// Next, Jump to the next key
	Next()

	// Valid, Whether it is valid, that is, whether all keys have been traversed, used to exit the traversal
	Valid() bool

	// Key data for the current traversal position
	Key() []byte

	// Value data for the current traversal position
	Value() *data.LogRecordPos

	// Close the iterator and release the corresponding resources
	Close()
}
