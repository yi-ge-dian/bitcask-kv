package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/yi-ge-dian/bitcask-kv/fio"
)

var ErrInvalidCRC = fmt.Errorf("invalid crc checksum, data may be corrupted")

const DataFileSuffix = ".data"

// DataFile 数据文件, 用于存储数据(写入磁盘)
type DataFile struct {
	// 文件唯一标识
	FileId uint32

	// 当前文件写入偏移
	WriteOff int64

	// IOManager 用于管理文件IO
	IOManager fio.IOManager
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 假设dirPath为"/tmp/bitcask-kv" fileId为1 则fileName为"/tmp/bitcask-kv/000000001.data"
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileSuffix)
	// 通过文件名创建IOManager
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	// 创建DataFile
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

// ReadLogRecord 读取数据文件，根据offset读取数据文件中的数据
// 返回值：LogRecord, 读取的字节数, error
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取最大的Header信息超过了文件大小，则只读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取Header信息
	headerBuffer, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解码Header信息
	header, headerSize := DecodeLogRecordHeader(headerBuffer)
	// 下面的这两种情况都表示读取到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.Crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的key和value的长度
	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	var recordSize = headerSize + keySize + valueSize

	// 初始化 LogRecord, 目前只能从头部拿到 LogRecord 的类型，等待去拿 key和value
	logRecord := &LogRecord{
		Type: header.RecordType,
	}

	// 读取数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解码数据
		logRecord.Key, logRecord.Value = kvBuf[:keySize], kvBuf[keySize:]
	}

	// 计算crc校验码
	crc := GetLogRecordCRC(logRecord, headerBuffer[crc32.Size:headerSize])
	if crc != header.Crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// Write 写入数据文件
func (df *DataFile) Write(buf []byte) error {
	// 写入数据
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	// 更新写入偏移
	df.WriteOff += int64(n)
	return nil
}

// Sync 持久化数据文件
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// Close 关闭数据文件
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// ReadNBytes 从数据文件中读取n个字节
func (df *DataFile) ReadNBytes(n int64, offset int64) ([]byte, error) {
	buffer := make([]byte, n)
	_, err := df.IOManager.Read(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}
