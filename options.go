package bitcaskkv

import "os"

type Options struct {
	// Database directory path
	DirPath string

	// Maximum data file size
	DataFileSize int64

	// Whether to enable data file sync write after each write
	SyncWrites bool

	// Index type
	IndexType IndexerType
}

type IndexerType = int8

const (
	// BTree
	BTree IndexerType = iota + 1

	// Adpative Radix Tree
	ART
)

type IteratorOptions struct {
	// Traverses a Key prefixed with the specified value, defaults to null
	Prefix []byte

	// Whether to traverse in reverse, the default is false, means from small to large
	Reverse bool
}

type WriteBatchOptions struct {
	// The largest amount of data in a batch
	MaxBatchNum uint

	// Whether persistent when committing
	SyncWrites bool
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
