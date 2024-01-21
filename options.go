package bitcaskkv

type Options struct {
	// Database directory path
	DirPath string

	// Maximum data file size
	DataFileSize int64

	// Whether to enable data file sync write after each write
	SyncWrites bool
}
