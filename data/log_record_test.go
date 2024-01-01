package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	// t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空的情况
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	// t.Log(res2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 删除的情况
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordDelete,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	// 正常情况
	headerBuffer1 := []byte{154, 6, 195, 152, 0, 4, 10}
	header1, n1 := DecodeLogRecordHeader(headerBuffer1)
	assert.NotNil(t, header1)
	assert.Equal(t, int64(7), n1)
	assert.Equal(t, uint32(2562918042), header1.Crc)
	assert.Equal(t, LogRecordNormal, header1.RecordType)
	assert.Equal(t, uint32(4), header1.KeySize)
	assert.Equal(t, uint32(10), header1.ValueSize)

	// value 为空的情况
	headerBuffer2 := []byte{114, 60, 154, 121, 0, 4, 0}
	header2, n2 := DecodeLogRecordHeader(headerBuffer2)
	assert.NotNil(t, header2)
	assert.Equal(t, int64(7), n2)
	assert.Equal(t, uint32(2040151154), header2.Crc)
	assert.Equal(t, LogRecordNormal, header2.RecordType)
	assert.Equal(t, uint32(4), header2.KeySize)
	assert.Equal(t, uint32(0), header2.ValueSize)

	// 删除的情况
	headerBuffer3 := []byte{198, 55, 237, 223, 1, 4, 0}
	header3, n3 := DecodeLogRecordHeader(headerBuffer3)
	assert.NotNil(t, header3)
	assert.Equal(t, int64(7), n3)
	assert.Equal(t, uint32(3756865478), header3.Crc)
	assert.Equal(t, LogRecordDelete, header3.RecordType)
	assert.Equal(t, uint32(4), header3.KeySize)
	assert.Equal(t, uint32(0), header3.ValueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	headerBuffer1 := []byte{154, 6, 195, 152, 0, 4, 10}
	crc1 := GetLogRecordCRC(rec1, headerBuffer1[crc32.Size:])
	assert.Equal(t, uint32(2562918042), crc1)

	// value 为空的情况
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordNormal,
	}
	headerBuffer2 := []byte{114, 60, 154, 121, 0, 4, 0}
	crc2 := GetLogRecordCRC(rec2, headerBuffer2[crc32.Size:])
	assert.Equal(t, uint32(2040151154), crc2)

	// 删除的情况
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordDelete,
	}
	headerBuffer3 := []byte{198, 55, 237, 223, 1, 4, 0}
	crc3 := GetLogRecordCRC(rec3, headerBuffer3[crc32.Size:])
	assert.Equal(t, uint32(3756865478), crc3)
}
