package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randSrc = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 获取测试用的key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

// RandomValue 获取测试用的value
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randSrc.Intn(len(letters))]
	}
	return []byte("bitcask-go-value-" + string(b))
}
