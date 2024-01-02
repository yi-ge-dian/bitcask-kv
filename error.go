package bitcaskkv

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpateFailed       = errors.New("the index update failed")
	ErrKeyNotFound            = errors.New("the key not in database")
	ErrDataFileNotFound       = errors.New("the data file not found")
	ErrDataDirectoryCorrupted = errors.New("the data directory is corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch num")
)
