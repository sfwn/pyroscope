package storage

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pyroscope-io/pyroscope/pkg/storage/cache"
	"github.com/pyroscope-io/pyroscope/pkg/util/bytesize"
)

type ClickHouseDB interface {
	Update(func(conn clickhouse.Conn) error) error
	View(func(conn clickhouse.Conn) error) error
	NewWriteBatch() driver.Batch
	MaxBatchCount() int64
}

type ClickHouseDBWithCache interface {
	ClickHouseDB
	CacheLayer

	Size() bytesize.ByteSize
	CacheSize() uint64

	DBInstance() clickhouse.Conn
	CacheInstance() *cache.Cache
	Name() string
}
