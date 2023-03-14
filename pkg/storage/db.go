package storage

import (
	"context"
	"os"
	"path/filepath"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/pyroscope-io/pyroscope/pkg/storage/types"
	"github.com/sirupsen/logrus"

	"github.com/pyroscope-io/pyroscope/pkg/storage/cache"
	"github.com/pyroscope-io/pyroscope/pkg/util/bytesize"
)

type db struct {
	name   string
	logger logrus.FieldLogger

	clickhouse.Conn
	*cache.Cache

	lastGC  bytesize.ByteSize
	gcCount prometheus.Counter
}

type Prefix string

const (
	segmentPrefix    Prefix = "s:"
	treePrefix       Prefix = "t:"
	dictionaryPrefix Prefix = "d:"
	dimensionPrefix  Prefix = "i:"
)

func (p Prefix) String() string      { return string(p) }
func (p Prefix) bytes() []byte       { return []byte(p) }
func (p Prefix) key(k string) []byte { return []byte(string(p) + k) }

func (p Prefix) trim(k []byte) ([]byte, bool) {
	if len(k) > len(p) {
		return k[len(p):], true
	}
	return nil, false
}

func (s *Storage) newClickHouse(name string, p Prefix, codec cache.Codec) (ClickHouseDBWithCache, error) {
	logger := logrus.New()
	logger.SetLevel(s.config.badgerLogLevel)

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr:  s.config.chAddrs,
		Debug: false,
	})
	if err != nil {
		return nil, err
	}

	var d *db = &db{
		name:    name,
		logger:  s.logger.WithField("db", name),
		Conn:    ch,
		gcCount: s.metrics.gcCount.WithLabelValues(name),
	}

	if codec != nil {
		d.Cache = cache.NewClickHouse(cache.ClickHouseConfig{
			ClickHouseDB: d,
			Metrics:      s.metrics.createCacheMetrics(name),
			TTL:          s.cacheTTL,
			Prefix:       p.String(),
			Codec:        codec,
		})
	}

	s.maintenanceTask(s.badgerGCTaskInterval, func() {
		//diff := calculateDBSize(badgerPath) - d.lastGC
		//if d.lastGC == 0 || s.gcSizeDiff == 0 || diff > s.gcSizeDiff {
		//	d.runGC(0.7)
		//	d.gcCount.Inc()
		//	d.lastGC = calculateDBSize(badgerPath)
		//}
	})

	return d, nil
}

func (d *db) Size() bytesize.ByteSize {
	//// The value is updated once per minute.
	//lsm, vlog := d.DB.Size()
	//return bytesize.ByteSize(lsm + vlog)
	return 0
}

func (d *db) CacheSize() uint64 {
	return d.Cache.Size()
}

func (d *db) Name() string {
	return d.name
}

func (d *db) DBInstance() types.ClickHouseDB {
	return d
}
func (d *db) CacheInstance() *cache.Cache {
	return d.Cache
}

func (d *db) Close() error {
	return d.Conn.Close()
}

func (d *db) runGC(discardRatio float64) (reclaimed bool) {
	//d.logger.Debug("starting badger garbage collection")
	//for {
	//	switch err := d.RunValueLogGC(discardRatio); err {
	//	default:
	//		d.logger.WithError(err).Warn("failed to run GC")
	//		return false
	//	case badger.ErrNoRewrite:
	//		return reclaimed
	//	case nil:
	//		reclaimed = true
	//		continue
	//	}
	//}
	return true
}

// TODO(kolesnikovae): filepath.Walk is notoriously slow.
//
//	Consider use of https://github.com/karrick/godirwalk.
//	Although, every badger.DB calculates its size (reported
//	via Size) in the same way every minute.
func calculateDBSize(path string) bytesize.ByteSize {
	var size int64
	_ = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		switch filepath.Ext(path) {
		case ".sst", ".vlog":
			size += info.Size()
		}
		return nil
	})
	return bytesize.ByteSize(size)
}

// TODO
// SELECT database, formatReadableSize(sum(bytes)) AS size FROM system.parts where database = 'monitor' GROUP BY database;
func (d *db) calculateDBSizeCH(path string) bytesize.ByteSize {
	return 0
}

func (d *db) Update(f func(conn clickhouse.Conn) error) error {
	return f(d.Conn)
}

func (d *db) View(f func(conn clickhouse.Conn) error) error {
	return f(d.Conn)
}

func (d *db) NewWriteBatch(sql string) (driver.Batch, error) {
	batch, err := d.Conn.PrepareBatch(context.Background(), sql)
	return batch, err
}

func (d *db) MaxBatchCount() int64 {
	return defaultBatchSize
}

func (d *db) FQDN() string {
	return d.name
}
