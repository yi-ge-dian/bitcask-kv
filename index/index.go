package index

import (
	"github.com/yi-ge-dian/bitcask-kv/data"
)

// Indexer
// Abstracts the index
type Indexer interface {
	// Put the index, return the old value
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get the index
	Get(key []byte) *data.LogRecordPos

	// Delete the index, return the old value and whether it is successful
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size of the index
	Size() int

	// Close the index
	Close() error

	// Iterator of the index
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btree index
	Btree IndexType = iota + 1

	// Adpative Radix Tree index
	ART

	// B+Tree index
	BPTree
)

// NewIndexer
// Create a new index given the index type
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
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
