package kvenginebench

import (
	"github.com/juju/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type goleveldb struct {
	db *leveldb.DB
	wo *opt.WriteOptions
}

func NewGoLevelDB(path string, fsync bool) (Engine, error) {
	o := &opt.Options{
		BlockCacheCapacity:     64 * opt.MiB,
		BlockSize:              opt.MiB,
		CompactionL0Trigger:    4,
		WriteL0SlowdownTrigger: 16,
		WriteL0PauseTrigger:    24,
		CompactionTableSize:    64 * opt.MiB,
		CompactionTotalSize:    320 * opt.MiB,
		WriteBuffer:            128 * opt.MiB,
	}
	db, err := leveldb.OpenFile(path, o)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &goleveldb{
		db: db,
		wo: &opt.WriteOptions{Sync: fsync},
	}, nil
}

func (d *goleveldb) Set(key, value []byte) error {
	return d.db.Put(key, value, d.wo)
}

func (d *goleveldb) Get(key []byte) (value []byte, err error) {
	return d.db.Get(key, nil)
}

func (d *goleveldb) Each(fn func(key, value []byte) bool) {
	it := d.db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		if !fn(it.Key(), it.Value()) {
			break
		}
	}
}

func (d *goleveldb) Close() error {
	return d.db.Close()
}
