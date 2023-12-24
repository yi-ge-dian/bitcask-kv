package bitcaskkv

type Options struct {
	DirPath         string // 数据库存储目录
	MaxDataFileSize int64  // 数据文件最大大小
	SyncWrites      bool   // 每次写完之后由用户指定是否进行一次安全的持久化
}
