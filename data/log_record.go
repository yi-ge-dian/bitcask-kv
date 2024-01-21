package data

// LogRecordPos
// Record the position of the log record in the disk file
type LogRecordPos struct {
	Fid uint32

	Offset int64
}
