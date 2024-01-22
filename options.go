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

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}
