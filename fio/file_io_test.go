package fio

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(path string) {
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
}

func TestFileIOManager(t *testing.T) {
	path := filepath.Join("test", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("test", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	n1, err := fio.Write([]byte("bitcask kv"))
	assert.Nil(t, err)
	assert.Equal(t, 10, n1)

	n2, err := fio.Write([]byte("storage"))
	assert.Nil(t, err)
	assert.Equal(t, 7, n2)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("test", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n1, err := fio.Write([]byte("bitcask kv"))
	assert.Nil(t, err)
	assert.Equal(t, 10, n1)

	n2, err := fio.Write([]byte("storage"))
	assert.Nil(t, err)
	assert.Equal(t, 7, n2)

	buf := make([]byte, 10)
	n3, err := fio.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 10, n3)

	n4, err := fio.Read(buf, 10)
	fmt.Println(string(buf))
	fmt.Println(n4)
	assert.NotNil(t, err) // due to EOF
	assert.Equal(t, 7, n4)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("test", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n1, err := fio.Write([]byte("bitcask kv"))
	assert.Nil(t, err)
	assert.Equal(t, 10, n1)

	n2, err := fio.Write([]byte("storage"))
	assert.Nil(t, err)
	assert.Equal(t, 7, n2)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("test", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n1, err := fio.Write([]byte("bitcask kv"))
	assert.Nil(t, err)
	assert.Equal(t, 10, n1)

	n2, err := fio.Write([]byte("storage"))
	assert.Nil(t, err)
	assert.Equal(t, 7, n2)

	err = fio.Close()
	assert.Nil(t, err)
}
