package fio

const DataFilePerm = 0644

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
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
