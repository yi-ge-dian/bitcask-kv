package main

import (
	"fmt"

	bitcask "github.com/yi-ge-dian/bitcask-kv"
)

func main() {
	opts := bitcask.DefaultOption
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	// 写入数据
	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		panic(err)
	}

	// 读取数据
	val, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// 删除数据
	err = db.Delete([]byte("hello"))
	if err != nil {
		panic(err)
	}
}
