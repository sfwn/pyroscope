package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/bytebufferpool"

	"github.com/pyroscope-io/pyroscope/pkg/storage/cache/lfu"
)

type ClickHouseCache struct {
	ch      driver.Conn
	lfu     *lfu.Cache
	metrics *Metrics
	codec   Codec

	tableName string

	prefix string
	ttl    time.Duration

	evictionsDone chan struct{}
	writeBackDone chan struct{}
	flushOnce     sync.Once
}

type ClickHouseConfig struct {
	clickhouse.Conn
	*Metrics
	Codec

	// Prefix for badger DB keys.
	Prefix string
	// TTL specifies number of seconds an item can reside in cache after
	// the last access. An obsolete item is evicted. Setting TTL to less
	// than a second disables time-based eviction.
	TTL time.Duration
}

// ClickHouseCodec is a shorthand of coder-decoder. A Codec implementation
// is responsible for type conversions and binary representation.
type ClickHouseCodec interface {
	Serialize(w io.Writer, key string, value interface{}) error
	Deserialize(r io.Reader, key string) (interface{}, error)
	// New returns a new instance of the type. The method is
	// called by GetOrCreate when an item can not be found by
	// the given key.
	New(key string) interface{}
}

func NewClickHouse(c ClickHouseConfig) *ClickHouseCache {
	cache := &ClickHouseCache{
		lfu:           lfu.New(),
		ch:            c.Conn,
		codec:         c.Codec,
		metrics:       c.Metrics,
		prefix:        c.Prefix,
		ttl:           c.TTL,
		evictionsDone: make(chan struct{}),
		writeBackDone: make(chan struct{}),
	}

	evictionChannel := make(chan lfu.Eviction)
	writeBackChannel := make(chan lfu.Eviction)

	// eviction channel for saving cache items to disk
	cache.lfu.EvictionChannel = evictionChannel
	cache.lfu.WriteBackChannel = writeBackChannel
	cache.lfu.TTL = int64(c.TTL.Seconds())

	// start a goroutine for saving the evicted cache items to disk
	go func() {
		for e := range evictionChannel {
			// TODO(kolesnikovae): these errors should be at least logged.
			//  Perhaps, it will be better if we move it outside of the cache.
			//  Taking into account that writes almost always happen in batches,
			//  We should definitely take advantage of BadgerDB write batch API.
			//  Also, WriteBack and Evict could be combined. We also could
			//  consider moving caching to storage/db.
			cache.saveToDisk(e.Key, e.Value)
		}
		close(cache.evictionsDone)
	}()

	// start a goroutine for saving the evicted cache items to disk
	go func() {
		for e := range writeBackChannel {
			cache.saveToDisk(e.Key, e.Value)
		}
		close(cache.writeBackDone)
	}()

	return cache
}

func (cache *ClickHouseCache) Put(key string, val interface{}) {
	cache.lfu.Set(key, val)
}

func (cache *ClickHouseCache) saveToDisk(key string, val interface{}) error {
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	if err := cache.codec.Serialize(b, key, val); err != nil {
		return fmt.Errorf("serialization: %w", err)
	}
	cache.metrics.DBWrites.Observe(float64(b.Len()))
	return cache.ch.Exec(context.Background(), "insert into ? values (?, ?)", cache.tableName, cache.prefix+key, b.Bytes())
}

func (cache *ClickHouseCache) Sync() error {
	return cache.lfu.Iterate(func(k string, v interface{}) error {
		return cache.saveToDisk(k, v)
	})
}

func (cache *ClickHouseCache) Flush() {
	cache.flushOnce.Do(func() {
		// Make sure there is no pending items.
		close(cache.lfu.WriteBackChannel)
		<-cache.writeBackDone
		// evict all the items in cache
		cache.lfu.Evict(cache.lfu.Len())
		close(cache.lfu.EvictionChannel)
		// wait until all evictions are done
		<-cache.evictionsDone
	})
}

// Evict performs cache evictions. The difference between Evict and WriteBack is that evictions happen when cache grows
// above allowed threshold and write-back calls happen constantly, making pyroscope more crash-resilient.
// See https://github.com/pyroscope-io/pyroscope/issues/210 for more context
func (cache *ClickHouseCache) Evict(percent float64) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(cache.metrics.EvictionsDuration.Observe))
	cache.lfu.Evict(int(float64(cache.lfu.Len()) * percent))
	timer.ObserveDuration()
}

func (cache *ClickHouseCache) WriteBack() {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(cache.metrics.WriteBackDuration.Observe))
	cache.lfu.WriteBack()
	timer.ObserveDuration()
}

func (cache *ClickHouseCache) Delete(key string) error {
	cache.lfu.Delete(key)
	return cache.ch.Exec(context.Background(), "delete from ? where key = ?", cache.tableName, cache.prefix+key)
}

func (cache *ClickHouseCache) Discard(key string) {
	cache.lfu.Delete(key)
}

// DiscardPrefix deletes all data that matches a certain prefix
// In both cache and database
func (cache *ClickHouseCache) DiscardPrefix(prefix string) error {
	cache.lfu.DeletePrefix(prefix)
	return cache.dropPrefixCH(cache.prefix + prefix)
}

func (cache *ClickHouseCache) dropPrefixCH(prefix string) error {
	var err error
	for more := true; more; {
		if more, err = cache.dropPrefixBatchCH(prefix); err != nil {
			return err
		}
	}
	return nil
}

func (cache *ClickHouseCache) dropPrefixBatchCH(prefix string) (bool, error) {
	err := cache.ch.Exec(context.Background(), "delete from ? where key like '?%'", cache.tableName, prefix)
	return false, err
}

func (cache *ClickHouseCache) GetOrCreate(key string) (interface{}, error) {
	return cache.get(key, true)
}

func (cache *ClickHouseCache) Lookup(key string) (interface{}, bool) {
	v, err := cache.get(key, false)
	return v, v != nil && err == nil
}

type TableRow struct {
	k string `db:"k"`
	v string `db:"v"`
}

func (cache *ClickHouseCache) get(key string, createNotFound bool) (interface{}, error) {
	cache.metrics.ReadsCounter.Inc()
	return cache.lfu.GetOrSet(key, func() (interface{}, error) {
		cache.metrics.MissesCounter.Inc()
		rows, err := cache.ch.Query(context.Background(), "select v from ? where k = ? limit 1", cache.tableName, key)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		recordNotFound := true
		var row TableRow
		if rows.Next() {
			recordNotFound = false
			if err := rows.Scan(&row); err != nil {
				return nil, err
			}
		}
		if recordNotFound && createNotFound {
			return cache.codec.New(key), nil
		}

		cache.metrics.DBReads.Observe(float64(len(row.v)))
		return cache.codec.Deserialize(bytes.NewBufferString(row.v), key)
	})
}

func (cache *ClickHouseCache) Size() uint64 {
	return uint64(cache.lfu.Len())
}
