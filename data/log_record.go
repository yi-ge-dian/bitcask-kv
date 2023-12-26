package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota // 普通的数据记录
	LogRecordDelete                      // 删除的数据记录
)

// LogRecord 数据记录
type LogRecord struct {
	Key   []byte        // key
	Value []byte        // value
	Type  LogRecordType // 数据记录类型
}

// EncodeLogRecord 编码数据记录,返回编码后的字节数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// crc + type + keySize + valueSize + key + value
// 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecordHeader 数据头部记录
type LogRecordHeader struct {
	Crc        uint32        // crc校验码 4
	RecordType LogRecordType // 数据记录类型 1
	KeySize    uint32        // key的长度
	ValueSize  uint32        // value的长度
}

// DecodeLogRecordHeader 解码数据记录头部,返回解码后的数据记录头部,以及解码后的长度
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

// LogRecordPos 数据内存索引主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id,表示将数据存储到了哪个文件当中
	Offset int64  // 偏移,表示将数据存储到了数据文件中的哪个位置
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	return 0
}
