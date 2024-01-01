package bitcaskkv

type Options struct {
	DirPath      string      // 数据库存储目录
	DataFileSize int64       // 数据文件最大大小
	SyncWrites   bool        // 每次写完之后由用户指定是否进行一次安全的持久化
	IndexType    IndexerType // 索引类型
}

type IndexerType = byte

const (
	// BTreeIndex btree索引
	BTree IndexerType = iota

	// 自适应基数树索引
	AdaptiveRadixTree
)

type IteratorOption struct {
	//遍历前缀为指定值的 Key,默认为空
	Prefix []byte
	// 是否反向遍历,默认false是正向
	Reverse bool
}

// DefaultOptions 默认的Options
var DefaultOptions = Options{
	DirPath:      "/tmp/bitcask-go",
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}

// DefaultIteratorOption 默认的IteratorOption
var DefaultIteratorOption = IteratorOption{
	Prefix:  nil,
	Reverse: false,
}
