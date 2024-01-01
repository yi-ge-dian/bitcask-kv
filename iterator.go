package bitcaskkv

import (
	"bytes"

	"github.com/yi-ge-dian/bitcask-kv/index"
)

type Iterator struct {
	index   index.Iterator // 索引迭代器
	db      *DB            // 数据库
	options IteratorOption // 迭代器选项
}

// NewIterator 创建迭代器
func (db *DB) NewIterator(opts IteratorOption) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:      db,
		index:   indexIter,
		options: opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一个key
func (it *Iterator) Rewind() {
	it.index.Rewind()
	it.skipToNext()
}

// Seek 根据传入的key，定位到第一个大于等于（或者小于等于）key的位置，然后从该位置开始迭代
func (it *Iterator) Seek(key []byte) {
	it.index.Seek(key)
	it.skipToNext()
}

// Next 迭代到下一个key
func (it *Iterator) Next() {
	it.index.Next()
	it.skipToNext()
}

// Valid 判断迭代器是否有效
func (it *Iterator) Valid() bool {
	return it.index.Valid()
}

// Key 获取当前迭代到的key
func (it *Iterator) Key() []byte {
	return it.index.Key()
}

// Value 获取当前迭代到的value
func (it *Iterator) Value() ([]byte, error) {
	LogRecordPos := it.index.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(LogRecordPos)
}

// Close 关闭迭代器
func (it *Iterator) Close() {
	it.index.Close()
}

// SkipToNext 跳过不满足前缀的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.index.Valid(); it.index.Next() {
		key := it.index.Key()
		if prefixLen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
