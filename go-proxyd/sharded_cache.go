package main

import (
	"github.com/badoo/thunder/go-proxyd/vhash"
)

type shardedCache struct {
	caches []*vhash.Vhash
}

var sh *shardedCache

func newShardedCache(shards int) *shardedCache {
	scache := shardedCache{
		caches: make([]*vhash.Vhash, shards),
	}

	for i := 0; i < len(scache.caches); i++ {
		scache.caches[i] = vhash.NewVhash()
	}

	return &scache
}

func (scache *shardedCache) getCache(key uint64) *vhash.Vhash {
	return scache.caches[key%uint64(len(scache.caches))]
}

func (scache *shardedCache) Get(key uint64) (v vhash.Value, ok bool) {
	shard := scache.getCache(key)
	v, ok = shard.Get(key)
	return
}

func (scache *shardedCache) Set(key uint64, value vhash.Value) {
	shard := scache.getCache(key)
	shard.Set(key, value)
}

func (scache *shardedCache) Delete(key uint64) bool {
	shard := scache.getCache(key)
	return shard.Delete(key)
}

func (scache *shardedCache) Lock(key uint64) {
	shard := scache.getCache(key)
	shard.Lock()
}

func (scache *shardedCache) Unlock(key uint64) {
	shard := scache.getCache(key)
	shard.Unlock()
}
