package fio

const DataFilePerm = 0644

// IOManager 是一个抽象的IO接口，用于实现不同的IO
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

// NewIOManager 初始化IOManager，目前只支持标准的文件IO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
