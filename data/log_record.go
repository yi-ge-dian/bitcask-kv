package data

type LogRecordType byte

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

// LogRecordPos 数据内存索引主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id,表示将数据存储到了哪个文件当中
	Offset int64  // 偏移,表示将数据存储到了数据文件中的哪个位置
}
