package redis

import (
	"encoding/binary"
	"errors"
	"time"

	bitcask "github.com/yi-ge-dian/bitcask-kv"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding tthe wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// Redis data structure
type RedisDataStructure struct {
	db *bitcask.DB
}

func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{
		db: db,
	}, nil
}

// =================================== String ===================================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// encode value: type + expire + payload
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// call bitcask engine to put
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// decode value
	var dataType = encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	// check expire or not
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}
