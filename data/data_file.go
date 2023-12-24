package data

import "github.com/yi-ge-dian/bitcask-kv/fio"

type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 写入偏移
	IOManager fio.IOManager // io管理器
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// 读取数据文件
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

// 写入数据文件
func (df *DataFile) Write(data []byte) error {
	return nil
}

// 持久化数据文件
func (df *DataFile) Sync() error {
	return nil
}
