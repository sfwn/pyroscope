package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dgraph-io/badger/v2"
	"github.com/pyroscope-io/pyroscope/pkg/storage"
	"github.com/pyroscope-io/pyroscope/pkg/storage/cache"
	"github.com/pyroscope-io/pyroscope/pkg/util/bytesize"
)

type ClickHouseDB interface {
	Update(func(conn clickhouse.Conn) error) error
	View(func(conn clickhouse.Conn) error) error
	NewWriteBatch() *badger.WriteBatch
	MaxBatchCount() int64
}

type ClickHouseDBWithCache interface {
	ClickHouseDB
	storage.CacheLayer

	Size() bytesize.ByteSize
	CacheSize() uint64

	DBInstance() clickhouse.Conn
	CacheInstance() *cache.Cache
	Name() string
}
