package bitcaskkv

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/yi-ge-dian/bitcask-kv/data"
)

var txnFinKey = []byte("txn-fin")

const nonTransactionSeqNo uint64 = 0

// WriteBatch
// Write data in batches to ensure atomicity
type WriteBatch struct {
	// user write batch options
	options WriteBatchOptions

	// concurrency control
	mu *sync.Mutex

	// db instance
	db *DB

	// pending writes
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put, batch write data
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// put data into pending writes
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete, batch delete data
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		// if the data does not exist, it will be returned directly.
		if wb.pendingWrites[string(key)] != nil {
			// if the data exists in pending writes, it will be deleted directly.
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// put data into pending writes
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit
// Commit the transaction, write the staging data to the data file, and
// update the in-memory index
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// lock guarantees serialization of transaction commits
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// get the current latest transaction sequence number
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// start writing data to the data file
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// write a log record that identifies the completion of the transaction
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// determine whether to persist according to the user configuration
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// update memory index
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// clean the staging data
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// logRecordKeyWithSeq
// key+Seq Number Encoding
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// parseLogRecordKey
// parse the LogRecord key and get the actual key and transaction sequence number
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
