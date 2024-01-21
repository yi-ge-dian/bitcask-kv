package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type keySize valueSize
// 4 +  1  +  5   +   5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord
// Records written to data files , It's called a log because the data in the data file is
// appended and written, similar to the format of a journal
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordHeader
type logRecordHeader struct {
	// crc checksum value
	crc uint32

	// the type of the log record
	recordType LogRecordType

	// the length of the key
	keySize uint32

	// the length of the value
	valueSize uint32
}

// LogRecordPos
// Record the position of the log record in the disk file
type LogRecordPos struct {
	// File id, indicating the file to which the data is stored.
	Fid uint32

	// Offset, which indicates where the data is stored in the data file
	Offset int64
}

// EncodeLogRecord
// Encodes the LogRecord and returns an array of bytes and their lengths.
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader
// Decode the log record header and return the header and the length of the header
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
