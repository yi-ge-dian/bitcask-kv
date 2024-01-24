package bitcaskkv

import (
	"bytes"

	"github.com/yi-ge-dian/bitcask-kv/index"
)

// Iterator
type Iterator struct {
	// index iterator
	indexIter index.Iterator

	// db instance
	db *DB

	// iterator options
	options IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// Rewind
// Go back to the starting point of the iterator, the first data
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek
// Find the first target key greater than (or less than) or equal to the passed key,
// and iterate from this key
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next
// Jump to the next key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid
// Whether it is valid, that is, whether all keys have been traversed, used to exit the traversal
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key
// Key data for the current traversal position
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value
// Value data for the current traversal position
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close
// Close the iterator and release the corresponding resources
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// skipToNext
// Skip to the key that meets the prefix condition
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
