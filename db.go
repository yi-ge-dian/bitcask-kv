package bitcaskkv

import (
	"sync"

	"github.com/yi-ge-dian/bitcask-kv/data"
	"github.com/yi-ge-dian/bitcask-kv/index"
)

// DB
// Bitcask KV Storage Engine Instance
type DB struct {
	// user options
	options Options

	// concurrency control
	mu *sync.RWMutex

	// active data file, only for write
	activeFile *data.DataFile

	// old data files, only for read, fid ==> data file
	olderFiles map[uint32]*data.DataFile

	// memory index
	index index.Indexer
}

// Put Key/Value data, key can not be empty
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// construct log record
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log record to the currently active data file
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// update index
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
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

	// find the corresponding data file based on the file id
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// if the data file is not found
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// read the corresponding data according to the offset
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// if the log record is deleted, the key is not found
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecord log record to the currently active data file
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

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

	// persistence based on user configuration
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// return the position of the log record in the data file as index value
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	// set active data file
	db.activeFile = dataFile
	return nil
}
