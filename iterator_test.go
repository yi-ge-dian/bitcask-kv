package bitcaskkv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yi-ge-dian/bitcask-kv/utils"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Put
	mockValue := utils.RandomValue(10)
	err = db.Put(utils.GetTestKey(10), mockValue)
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	value, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, mockValue, value)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnedc"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aeeue"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("esnue"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnede"), utils.RandomValue(10))
	assert.Nil(t, err)

	iter1 := db.NewIterator(DefaultIteratorOption)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		// t.Log("key= ", string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()

	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		// t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}

	// Reverse
	iterOpts1 := DefaultIteratorOption
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		// t.Log("key= ", string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}

	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		// t.Log(string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}

	// Prefix
	iterOpts2 := DefaultIteratorOption
	iterOpts2.Prefix = []byte("a")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		// t.Log("key= ", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}
}
