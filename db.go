package bitcaskkv

import (
	"sync"

	"github.com/yi-ge-dian/bitcask-kv/data"
	"github.com/yi-ge-dian/bitcask-kv/index"
)

type DB struct {
	options    *Options                  // 数据库配置
	activeFile *data.DataFile            // 活跃数据文件,可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧数据文件，只能用于读 fileId -> DataFile
	index      index.Indexer             // 内存索引
	mu         *sync.RWMutex             // 读写锁
}

// Put 向数据库中写入数据, key 不能为空
func (db *DB) Put(key, value []byte) error {
	// 判断key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将数据追加写数据到活跃文件中
	logRecordPos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, logRecordPos); !ok {
		return ErrIndexUpateFailed
	}

	return nil
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果当前活跃数据文件的写入偏移加上数据编码后的长度大于等于数据文件的最大长度
	// 则需要将当前活跃数据文件设置为旧数据文件，并且打开新的数据文件
	if db.activeFile.WriteOff+size > db.options.MaxDataFileSize {
		// 先持久化数据文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将当前活跃数据文件设置为旧数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 将数据写入到当前活跃数据文件中
	writeoff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否需要持久化数据文件
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造数据内存索引
	logRecordPos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeoff,
	}
	return logRecordPos, nil
}

// setActiveDataFile 设置活跃数据文件
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile == nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	// 将新打开的数据文件设置为活跃数据文件
	db.activeFile = dataFile
	return nil
}

// Get 从数据库中获取数据, key 不能为空
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 判断key是否为空
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引中获取数据
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 如果数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量从数据文件中读取数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 如果数据记录类型为删除类型，则返回空
	if logRecord.Type == data.LogRecordDelete {
		return nil, nil
	}

	return logRecord.Value, nil
}
