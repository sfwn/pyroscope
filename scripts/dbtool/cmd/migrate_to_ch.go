package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dgraph-io/badger/v2"
	"github.com/spf13/cobra"
)

func (d *dbTool) newMigrateCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:     "migrate",
		Short:   "Migrate to clickhouse",
		RunE:    d.runMigrate,
		PreRunE: d.openDB(true),
	}
	cmd.Flags().StringVarP(&d.prefix, "prefix", "p", "", "key prefix")
	return &cmd
}

func (d *dbTool) runMigrate(_ *cobra.Command, _ []string) error {
	// get all badgerDB
	parentDir := filepath.Dir(d.dir)
	files, err := os.ReadDir(parentDir)
	if err != nil {
		return fmt.Errorf("failed to read storage dirs, err: %v", err)
	}
	var dirs []string
	for _, f := range files {
		if f.IsDir() {
			dirs = append(dirs, filepath.Join(parentDir, f.Name()))
		}
	}
	fmt.Println(dirs)
	ctx := context.Background()
	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr:  []string{"localhost:9000"},
		Debug: false,
	})
	if err != nil {
		return fmt.Errorf("failed to open clickhouse, err: %v", err)
	}
	ch.Exec(ctx, "use pyroscope")
	for _, dir := range dirs {
		d.dir = dir
		db, err := openDB(dir, true)
		if err != nil {
			return fmt.Errorf("failed to open badgerDB, err: %v, db: %s", err, dir)
		}
		d.db = db
		dbName := filepath.Base(d.dir)
		// create ch table if not exist
		if err := ch.Exec(ctx, fmt.Sprintf("create table if not exists %s ( `k` String, `v` String ) Engine = MergeTree order by k", dbName)); err != nil {
			return fmt.Errorf("failed to create table, err: %v, table: %s", err, dbName)
		}
		if err := ch.Exec(ctx, fmt.Sprintf("delete from %s where 1=1", filepath.Base(dir))); err != nil {
			return fmt.Errorf("failed to delete table, err: %v, table: %s", err, dbName)
		}
		if err := d.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = []byte(d.prefix)
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				key := string(it.Item().Key())
				_, _ = fmt.Fprintf(os.Stdout, "%s\n", key)
				_value, err := it.Item().ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("failed to get value, err: %v, key: %s")
				}
				value := string(_value)
				if err := ch.Exec(ctx, fmt.Sprintf(`insert into pyroscope.%s values (?, ?)`, dbName), key, value); err != nil {
					return fmt.Errorf("failed to insert into clickhouse, dbName: %s, err: %v, key: %s", dbName, err, key)
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("failed, err: %v", err)
		}
	}
	return nil
}
