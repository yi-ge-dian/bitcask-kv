package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO
	StandardFIO FileIOType = iota

	// MemoryMap
	MemoryMap
)

// IOManager
// Abstracts the file read and write
type IOManager interface {
	// Read the corresponding data from a given location in the file
	Read([]byte, int64) (int, error)

	// Write a byte array to a file
	Write([]byte) (int, error)

	// Sync the data to disk
	Sync() error

	// Close the file
	Close() error

	// Get the size of the file
	Size() (int64, error)
}

// NewIOManager
// Create a new IOManager
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
