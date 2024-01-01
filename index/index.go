package index

import (
	"github.com/yi-ge-dian/bitcask-kv/data"
)

// Indexer 通用索引接口
type Indexer interface {
	// Put 添加索引
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获取索引
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引
	Delete(key []byte) bool

	// Size 返回索引中的key数量
	Size() int

	// Iterator 返回一个迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = byte

const (
	// BTreeIndex btree索引
	Btree IndexType = iota

	// 自适应基数树索引
	AdaptiveRadixTree
)

// NewIndexer 根据索引类型创建索引
func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case Btree:
		return NewBTree()
	case AdaptiveRadixTree:
		// todo
		return nil
	default:
		panic("invalid index type")
	}
}

// Iterator 迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个key
	Rewind()

	// Seek 根据传入的key，定位到第一个大于等于（或者小于等于）key的位置，然后从该位置开始迭代
	Seek(key []byte)

	// Next 迭代到下一个key
	Next()

	// Valid 判断迭代器是否有效
	Valid() bool

	// Key 返回当前迭代到的key
	Key() []byte

	// Value 返回当前迭代到的value
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放资源
	Close()
}
