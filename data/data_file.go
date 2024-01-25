package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/yi-ge-dian/bitcask-kv/fio"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

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
// Open a data file, such as 000000001.data
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId)
}

// OpenHintFile
// Open the hint file, such as hint-index
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// OpenMergeFinishedFile
// Open the merge finished file, such as merge-finished
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord
// Read LogRecord from data file based on offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// If the maximum header length to be read already exceeds the length of the file,
	// just read to the end of the file
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// read the header of the log record
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// if the header is nil, it means that the data file is empty
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// get the key size and value size of the log record
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	var logRecord = &LogRecord{
		Type: header.recordType,
	}

	// read the key/value data actually stored by the user
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// calculate the crc of the log record
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// WriteHintRecord
// Write index information to hint file
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// readNBytes read n bytes from the specified offset
func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	return b, err
}
