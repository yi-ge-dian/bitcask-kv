package bitcaskkv

import (
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/yi-ge-dian/bitcask-kv/data"
	"github.com/yi-ge-dian/bitcask-kv/index"
)

// DB bitcask 数据库存储引擎
type DB struct {
	// 数据库配置
	options Options

	// 活跃数据文件,可以用于写入
	activeFile *data.DataFile

	// 旧数据文件，只能用于读 fileId -> DataFile
	olderFiles map[uint32]*data.DataFile

	// 内存索引
	index index.Indexer

	// 读写锁
	mu *sync.RWMutex

	// 数据文件id，仅在加载数据文件索引时使用
	fileIds []int

	// 事务序列号，全局递增
	seqNo uint64
}

// Open 打开 bitcask 数据库存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户配置进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据库目录是否存在，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化数据库
	db := &DB{
		options:    options,
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
		mu:         &sync.RWMutex{},
		fileIds:    nil,
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	// 返回数据库实例
	return db, nil
}

// checkOptions 对用户配置进行校验
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("the dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("the max data file size is invalid")
	}
	return nil
}

// Put 向数据库中写入数据, key 不能为空
func (db *DB) Put(key, value []byte) error {
	// 判断key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将数据追加写数据到活跃文件中
	logRecordPos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, logRecordPos); !ok {
		return ErrIndexUpateFailed
	}

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

	// 根据数据内存索引从数据文件中读取数据
	return db.getValueByPosition(logRecordPos)
}

// Delete 从数据库中删除数据, key 不能为空
func (db *DB) Delete(key []byte) error {
	// 判断key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先利用内存索引检查key是否存在
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: nil,
		Type:  data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中删除对应的key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpateFailed
	}

	return nil
}

// ListKeys 列出数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 将数据库中的数据按照指定的函数进行处理
// 如果函数返回false，则停止处理
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭活跃数据文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧数据文件
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 同步数据到磁盘
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// appendLogRecordWithLock 追加写数据到活跃文件中
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
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
	if db.activeFile != nil {
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

// loadDataFiles 加载数据文件
func (db *DB) loadDataFiles() error {
	// 获取数据库目录下的所有文件
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	// 遍历所有文件,找到所有以 .data 为结尾的数据文件
	var FileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			FileIds = append(FileIds, fileId)
		}
	}

	// 对文件id进行排序,从小到大
	sort.Ints(FileIds)
	db.fileIds = FileIds

	// 遍历所有文件id,打开数据文件
	for i, fid := range FileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(FileIds)-1 { // 最后一个文件为活跃数据文件
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// loadIndexFromDataFiles 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	// 如果没有数据文件，则直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		// 如果数据记录类型为删除类型，则从内存索引中删除
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at start up")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有数据文件id, 处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构造数据内存索引
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			// 解析数据记录的key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 如果事务序列号为0，则表示不是事务数据
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 更新偏移量
			offset += size
		}

		// 如果是最后一个文件，则需要将最后一个文件的写入偏移设置为数据文件的写入偏移
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 如果数据记录类型为删除类型，则返回空
	if logRecord.Type == data.LogRecordDeleted {
		return nil, nil
	}

	return logRecord.Value, nil
}
