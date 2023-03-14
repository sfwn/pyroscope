package types

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseDB interface {
	Update(func(conn clickhouse.Conn) error) error
	View(func(conn clickhouse.Conn) error) error
	NewWriteBatch(sql string) (driver.Batch, error)
	MaxBatchCount() int64
	Close() error
	FQDN() string
}
