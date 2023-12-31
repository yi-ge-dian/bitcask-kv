package fio

import (
	"os"
)

// FileIO 文件IO
type FileIO struct {
	//  系统文件描述符
	fd *os.File
}

// NewFileIOManager 初始化文件IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// Read 从文件中给定的位置读取数据
func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}

// Write 将数据写入到文件中给定的位置
func (fio *FileIO) Write(buf []byte) (int, error) {
	return fio.fd.Write(buf)
}

// Sync 将数据同步到磁盘
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

// Size 返回文件的大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
