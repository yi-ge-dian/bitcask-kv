package redis

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(encValue) == 0 {
		return 0, nil
	}
	// the first byte is the data type
	return encValue[0], nil
}
