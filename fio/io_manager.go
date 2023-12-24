package fio

const DataFilePerm = 0644

type IOManager interface {
	// Read 从文件中给定的位置读取数据
	Read([]byte, int64) (int, error)
	// Write 将数据写入到文件中给定的位置
	Write([]byte) (int, error)
	// Sync 将数据同步到磁盘
	Sync() error
	// Close 关闭文件
	Close() error
}
