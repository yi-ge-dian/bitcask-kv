package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord
// Records written to data files , It's called a log because the data in the data file is
// appended and written, similar to the format of a journal
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
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
