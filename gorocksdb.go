package kvenginebench

import (
	rocksdb "github.com/cryring/gorocksdb"
	"github.com/juju/errors"
)

type gorocksdb struct {
	db *rocksdb.DB
	wo *rocksdb.WriteOptions
	ro *rocksdb.ReadOptions
}

func NewRocksDB(path string, fsync bool) (Engine, error) {
	o := rocksdb.NewDefaultOptions()
	o.SetDbWriteBufferSize(256 * 1024 * 1024)
	db, err := rocksdb.OpenDb(o, "benchmark")
	if err != nil {
		return nil, errors.Trace(err)
	}

	gorocksdb := &gorocksdb{db: db}
	gorocksdb.wo = rocksdb.NewDefaultWriteOptions()
	gorocksdb.wo.SetSync(fsync)
	gorocksdb.ro = rocksdb.NewDefaultReadOptions()
	return gorocksdb, nil
}

func (d *gorocksdb) Set(key, value []byte) error {
	return d.db.Put(d.wo, key, value)
}

func (d *gorocksdb) Get(key []byte) (value []byte, err error) {
	slice, err := d.db.Get(d.ro, key)
	if err != nil {
		return nil, err
	}
	v := slice.Data()
	value = make([]byte, len(v))
	copy(value, v)
	slice.Free()
	return value, nil
}

func (d *gorocksdb) Each(fn func(key, value []byte) bool) {
	it := d.db.NewIterator(d.ro)
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		ok := fn(k.Data(), v.Data())
		k.Free()
		v.Free()
		if !ok {
			break
		}
	}
}

func (d *gorocksdb) Close() error {
	d.wo.Destroy()
	d.ro.Destroy()
	d.db.Close()
	return nil
}
