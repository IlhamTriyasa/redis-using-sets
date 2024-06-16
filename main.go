package main

import (
	"context"
	"fmt"
	redis "github.com/go-redis/redis/v8"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "",
		Password: "",
	})

	log.Printf("Connected to Redis")

	// Simulasi menambahkan kunci ke Redis
	for i := 0; i < 10000; i++ {
		log.Printf("Adding key:%d", i)
		key := fmt.Sprintf("key:%d", i)
		rdb.Set(ctx, key, "value", 0)
	}

	log.Printf("Added 10000 keys to Redis")

	// Pengujian menggunakan SCAN
	startScan := time.Now()
	err := clearKeysUsingScan(ctx, rdb, "key:*")
	if err != nil {
		log.Printf("Error clearing keys using SCAN: %v", err)
	}
	elapsedScan := time.Since(startScan)
	fmt.Printf("SCAN method took: %s\n", elapsedScan)

	// Simulasi menambahkan kunci ke Redis lagi
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key:%d", i)
		rdb.Set(ctx, key, "value", 0)
		rdb.SAdd(ctx, "keys_set", key)
	}

	log.Printf("Added 10000 keys to Redis again")

	// Pengujian menggunakan Sets
	startSets := time.Now()
	err = clearKeysUsingSets(ctx, rdb, "keys_set")
	if err != nil {
		log.Printf("Error clearing keys using Sets: %v", err)
	}
	elapsedSets := time.Since(startSets)
	fmt.Printf("Sets method took: %s\n", elapsedSets)
}

func clearKeysUsingScan(ctx context.Context, rdb *redis.Client, pattern string) error {
	cursor := uint64(0)
	for {
		var keys []string
		var err error
		keys, cursor, err = rdb.Scan(ctx, cursor, pattern, 10).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func clearKeysUsingSets(ctx context.Context, rdb *redis.Client, setKey string) error {
	keys, err := rdb.SMembers(ctx, setKey).Result()
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		rdb.Del(ctx, keys...)
		rdb.Del(ctx, setKey)
	}
	return nil
}
