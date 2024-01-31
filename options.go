package bitcaskkv

import "os"

type Options struct {
	// Database directory path
	DirPath string

	// Maximum data file size
	DataFileSize int64

	// Whether to enable data file sync write after each write
	SyncWrites bool

	// How many bytes are written cumulatively and then persist
	BytesPerSync uint

	// Whether to use MMap to load data at startup
	MMapAtStartup bool

	// Index type
	IndexType IndexerType
}

type IndexerType = int8

const (
	// BTree
	BTree IndexerType = iota + 1

	// Adpative Radix Tree
	ART

	// BPlusTree B+ Tree, Store the index to disk
	BPlusTree
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
	DirPath:       os.TempDir(),
	DataFileSize:  256 * 1024 * 1024, // 256MB
	SyncWrites:    false,
	BytesPerSync:  0,
	MMapAtStartup: true,
	IndexType:     BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
