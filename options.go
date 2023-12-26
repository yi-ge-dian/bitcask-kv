package bitcaskkv

type Options struct {
	DirPath         string      // 数据库存储目录
	MaxDataFileSize int64       // 数据文件最大大小
	SyncWrites      bool        // 每次写完之后由用户指定是否进行一次安全的持久化
	IndexType       IndexerType // 索引类型
}

type IndexerType = byte

const (
	// BTreeIndex btree索引
	BTree IndexerType = iota
	
	// 自适应基数树索引
	AdaptiveRadixTree
)
