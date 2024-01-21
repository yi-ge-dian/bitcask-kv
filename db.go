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
}

// Open a Bitcask KV Storage Engine Instance
func Open(options Options) (*DB, error) {
	// perform checks on configuration items passed in by the user
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// determine if the data directory exists, and create it if it does not.
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create a new Bitcask KV Storage Engine Instance
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// load data files from disk
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// load index from data files
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// if the log record is deleted, the key is not found
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
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
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// write to data file
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}

	// remove the corresponding key from the in-memory index
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
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
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
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

	// iterate over all file ids and process records in the file
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

			// construct an in-memory index and put
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}

			// increment offset, next read from new position
			offset += size
		}

		// if it is a currently active file, update this file's WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
