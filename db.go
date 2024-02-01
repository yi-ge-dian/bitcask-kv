package bitcaskkv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
	"github.com/yi-ge-dian/bitcask-kv/data"
	"github.com/yi-ge-dian/bitcask-kv/fio"
	"github.com/yi-ge-dian/bitcask-kv/index"
	"github.com/yi-ge-dian/bitcask-kv/utils"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB
// Bitcask KV Storage Engine Instance
type DB struct {
	// user options
	options Options

	// concurrency control
	mu *sync.RWMutex

	// file ids
	// can only be used when the index is loaded, and cannot be updated and used elsewhere
	fileIds []int

	// active data file, only for write
	activeFile *data.DataFile

	// old data files, only for read, fid ==> data file
	olderFiles map[uint32]*data.DataFile

	// memory index
	index index.Indexer

	// Transaction sequence number, globally incrementing
	seqNo uint64

	// Whether the database is merging
	isMerging bool

	// Whether the file that did not participate in the merge recently exists
	seqNoFileExists bool

	// Whether it is the first time to initialize the data directory
	isInitial bool

	// file lock to ensure mutual exclusion between multiple processes
	fileLock *flock.Flock

	// how many bytes have been write
	bytesWrite uint

	// indicates how much data is invalid
	reclaimSize int64
}

// Stat
// Bitcask KV Storage Engine Instance Statistics
type Stat struct {
	// Key count
	KeyNum uint

	// Data file count
	DataFileNum uint

	// The amount of data that can be recovered by merge, in bytes
	ReclaimableSize int64

	// The amount of disk space occupied by the data directory
	DiskSize int64
}

// Open a Bitcask KV Storage Engine Instance
func Open(options Options) (*DB, error) {
	// perform checks on configuration items passed in by the user
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// determine whether the data directory exists, and if not, create the directory
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// check if the database is being used
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// create a new Bitcask KV Storage Engine Instance
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// load merge data files
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// load data files from disk
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// if index type is not B+Tree, load index from data files
	if options.IndexType != BPlusTree {
		// load index from hint file
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// load index from data files
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// reset the IO type to standard file IO
		if db.options.MMapAtStartup {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	}

	// if index type is B+Tree, load transaction sequence number
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// Close the Bitcask KV Storage Engine Instance
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// close the index
	if err := db.index.Close(); err != nil {
		return err
	}

	// save the current transaction sequence number
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//	close the currently active data file
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// close all old data files
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync the Bitcask KV Storage Engine Instance
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Stat the Bitcask KV Storage Engine Instance
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Put Key/Value data, key can not be empty
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// construct log record
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, db.seqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log record to the currently active data file
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// update index
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Delete Key/Value data, key can not be empty
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// first check if the key exists, and if not, return it directly.
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// constructs a LogRecord that identifies it as deleted.
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, db.seqNo),
		Type: data.LogRecordDeleted,
	}

	// write to data file
	pos, err := db.appendLogRecordWithLock(logRecord)

	if err != nil {
		return nil
	}
	db.reclaimSize += int64(pos.Size)

	// remove the corresponding key from the in-memory index
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Get Key/Value data, key can not be empty
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// retrieve the index information corresponding to the key from the in-memory index
	logRecordPos := db.index.Get(key)

	// if the key is not in the in-memory index
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// get the corresponding value according to the index information
	return db.getValueByPosition(logRecordPos)
}

// ListKeys, Get all keys
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

// Fold , Get all the data and perform the user-specified operation.
// Terminate the traversal when the function returns false
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
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

// getValueByPosition
// get the corresponding value according to the index information
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// find the corresponding data file according to the file ID
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// if data file is empty
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// read the corresponding data according to the offset
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// if the log record is deleted, it means that the key does not exist
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecordWithLock
// append log record to the currently active data file with lock
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord
// append log record to the currently active data file
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// determine if the currently active data file exists, Since no file is
	// generated when the database is not being written to
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// encode log record
	encRecord, size := data.EncodeLogRecord(logRecord)

	// if the data written has reached the active file threshold, the active file is closed
	// and a new file is opened
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// first persist the data file to ensure that the existing data is persisted to disk.
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// current active file converted to old data file
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// open a new data file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// write log record to the active data file
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	// persistence based on user configuration
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// clear the accumulated value
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// return the position of the log record in the data file as index value
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// setActiveDataFile
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	// Determine if the currently active data file exists
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// open data file
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}

	// set active data file
	db.activeFile = dataFile
	return nil
}

// loadDataFiles from the disk
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// iterate through the directory to find all files ending in .data
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// there's a chance that the data directory may have been corrupted
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//	sort the file ids and load them in order from smallest to largest
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// iterate over each file id and open the corresponding data file
	for i, fid := range fileIds {

		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			// the last one, with the largest id, means it's the currently active file
			db.activeFile = dataFile
		} else {
			// indicates an old data file
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles, iterate over all records in the file
// and update to the in-memory indexes
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// check if a merge has occurred
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// temporary transaction data, transaction id ==> transaction record
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// iterate over all file ids and process records in the file
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)

		// If it is smaller than the ID of the file that did not participate in the merge recently,
		// the index has been loaded from the Hint file
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

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

			// construct an in-memory index and put
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}

			// parse the key from the log record, get the transaction sequence number
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// Non-transactional operation, directly updating the memory index
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// Transaction operation, can be updated to the memory index
				if logRecord.Type == data.LogRecordTxnFinished {
					// 1. if the transaction is complete, the transaction record is deleted
					// from the temporary transaction data
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 2. if the transaction is not complete, the transaction record is
					// added to the temporary transaction data
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// update transaction sequence number
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// increment offset, next read from new position
			offset += size
		}

		// if it is a currently active file, update this file's WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// update db instance transaction sequence number
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}

// set the data file's IO type to standard file IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
