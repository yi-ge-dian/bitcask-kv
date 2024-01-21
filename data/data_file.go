package data

import "github.com/yi-ge-dian/bitcask-kv/fio"

const DataFileNameSuffix = ".data"

// DataFile
type DataFile struct {
	// File Identifier
	FileId uint32

	// The WriteOff indicates where the data is written to the file
	WriteOff int64

	// IOManager
	IoManager fio.IOManager
}

// OpenDataFile
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
