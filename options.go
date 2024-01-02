package bitcaskkv

// Options 数据库配置选项
type Options struct {
	DirPath      string      // 数据库存储目录
	DataFileSize int64       // 数据文件最大大小
	SyncWrites   bool        // 每次写完之后由用户指定是否进行一次安全的持久化
	IndexType    IndexerType // 索引类型
}

type IndexerType = byte

const (
	BTree             IndexerType = iota // BTreeIndex btree索引
	AdaptiveRadixTree                    // 自适应基数树索引
)

var DefaultOptions = Options{
	DirPath:      "/tmp/bitcask-go",
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}

// IteratorOption 迭代器选项
type IteratorOption struct {
	Prefix  []byte // 遍历前缀为指定值的 Key,默认为空
	Reverse bool   // 是否反向遍历,默认false是正向
}

var DefaultIteratorOption = IteratorOption{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 批量写选项
type WriteBatchOptions struct {
	MaxBatchNum uint // 一个批次当中最大的数据量
	SyncWrites  bool // 提交时是否 sync 持久化
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
