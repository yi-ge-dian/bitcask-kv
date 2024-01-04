package bitcaskkv

// Options 数据库配置选项
type Options struct {
	// 数据库存储目录
	DirPath string

	// 数据文件最大大小
	DataFileSize int64

	// 是否同步写入，每次写完之后由用户指定是否进行一次安全的持久化
	SyncWrites bool

	// 索引类型
	IndexType IndexerType
}

type IndexerType = byte

const (
	// BTree
	BTree IndexerType = iota

	// 自适应前缀树
	AdaptiveRadixTree
)

// IteratorOption 迭代器选项
type IteratorOption struct {
	// 遍历前缀为指定值的 Key,默认为空
	Prefix []byte

	// 是否反向遍历,默认false是正向
	Reverse bool
}

// WriteBatchOptions 批量写选项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 是否同步写入，每次写完之后由用户指定是否进行一次安全的持久化
	SyncWrites bool
}

var DefaultOptions = Options{
	DirPath:      "/tmp/bitcask-go",
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}

var DefaultIteratorOption = IteratorOption{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
