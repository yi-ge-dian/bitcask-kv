package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

// TransactionRecord
// Temporarily stored transaction-related data
type TransactionRecord struct {
	// The log record of the transaction
	Record *LogRecord

	// The position of the log record in the data file
	Pos *LogRecordPos
}

// EncodeLogRecord
// Encodes the LogRecord and returns an array of bytes and their lengths.
// The format of the log record is as follows:
//
// +--------------+--------------+---------------+--------------+-----------+-----------+
// |      crc     |    type      |   keySize     |  valueSize   | 	 key    |   value   |
// +--------------+--------------+---------------+--------------+-----------+-----------+
// |    4 bytes   |   1 byte     |  1-5 bytes    |  1-5 bytes   |    var    |    var    |
// +--------------+--------------+---------------+--------------+-----------+-----------+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// initialize a byte array of header parts
	header := make([]byte, maxLogRecordHeaderSize)

	// the fifth byte stores the Type
	header[4] = logRecord.Type
	var index = 5
	// after 5 bytes, the information about the length of the key and value is stored.
	// use variable-length types to save space
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// copy the contents of the header section
	copy(encBytes[:index], header[:index])

	// copy key and value data into a byte array.
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// crc checksums on the entire LogRecord.
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader
// Decode the log record header and return the header and the length of the header
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) < crc32.Size {
		return nil, 0
	}

	// get the crc checksum value
	crc := binary.LittleEndian.Uint32(buf[:4])

	// get the type of the log record
	rt := LogRecordType(buf[4])

	header := &logRecordHeader{
		crc:        crc,
		recordType: rt,
	}

	// get the key size of the log record
	var index = 5
	keySize, keySizeLen := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += keySizeLen

	// get the value size of the log record
	valueSize, valueSizeLen := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += valueSizeLen

	return header, int64(index)
}

// getLogRecordCRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
