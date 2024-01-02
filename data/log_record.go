package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota // 普通的数据记录
	LogRecordDeleted                          // 删除的数据记录
	LogRecordTxnFinished                      // 事务的数据记录
)

// LogRecordHeader 数据头部记录
type LogRecordHeader struct {
	Crc        uint32        // crc校验码 4
	RecordType LogRecordType // 数据记录类型 1
	KeySize    uint32        // key的长度（变长）
	ValueSize  uint32        // value的长度（变长）
}

// crc + type + keySize + valueSize + key + value
// 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 数据记录
type LogRecord struct {
	Key   []byte        // key
	Value []byte        // value
	Type  LogRecordType // 数据记录类型
}

// EncodeLogRecord 编码数据记录
// 返回编码后的字节数组和长度
// +--------------+--------------+---------------+--------------+-----------+-----------+
// |      crc     |    type      |   keySize     |  valueSize   | 	 key    |   value   |
// +--------------+--------------+---------------+--------------+-----------+-----------+
// |    4 bytes   |   1 byte     |  1-5 bytes    |  1-5 bytes   |    变长    |    变长   |
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	// 第五个字节存储type
	header[4] = byte(logRecord.Type)
	var pos = 5
	// 第六个字节开始存储key和value的长度，使用变长编码可以节省空间
	pos += binary.PutUvarint(header[pos:], uint64(len(logRecord.Key)))
	pos += binary.PutUvarint(header[pos:], uint64(len(logRecord.Value)))

	var size = pos + len(logRecord.Key) + len(logRecord.Value)
	var encBytes = make([]byte, size)

	// 将header部分的字节数组拷贝到encBytes中
	copy(encBytes[:pos], header[:pos])
	// 将key和value拷贝到encBytes中
	copy(encBytes[pos:], logRecord.Key)
	copy(encBytes[pos+len(logRecord.Key):], logRecord.Value)

	// 计算crc校验码
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 将crc校验码拷贝到encBytes中
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// DecodeLogRecordHeader 解码数据记录头部
// 返回解码后的header, 以及header的长度
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) < crc32.Size {
		return nil, 0
	}

	// 读取crc校验码
	crc := binary.LittleEndian.Uint32(buf[:4])
	// 读取type
	recordType := LogRecordType(buf[4])

	var pos = 5
	// 读取keySize
	keySize, keySizeLen := binary.Uvarint(buf[pos:])
	pos += keySizeLen
	// 读取valueSize
	valueSize, valueSizeLen := binary.Uvarint(buf[pos:])
	pos += valueSizeLen

	return &LogRecordHeader{
		Crc:        crc,
		RecordType: recordType,
		KeySize:    uint32(keySize),
		ValueSize:  uint32(valueSize),
	}, int64(pos)
}

// LogRecordPos 数据内存索引主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id,表示将数据存储到了哪个文件当中
	Offset int64  // 偏移,表示将数据存储到了数据文件中的哪个位置
}

// GetLogRecordCRC 计算crc校验码
func GetLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}

	// 计算crc校验码
	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

	return crc
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
