package fio

const DataFilePerm = 0644

// IOManager 是一个抽象的IO接口，用于实现不同的IO
type IOManager interface {
	// Read 从指定位置读取指定长度的数据
	Read([]byte, int64) (int, error)

	// Write 将数据写入到文件中给定的位置
	Write([]byte) (int, error)

	// Sync 将数据同步到磁盘
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 返回文件的大小
	Size() (int64, error)
}

// NewIOManager 初始化IOManager，目前只支持标准的文件IO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
