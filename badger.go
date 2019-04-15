package kvenginebench

import (
	"github.com/dgraph-io/badger"
	"github.com/juju/errors"
)

type badgerdb struct {
	db *badger.DB
}

func NewBadgerDB(path string, fsync bool) (Engine, error) {
	o := badger.DefaultOptions
	o.Dir = path
	o.ValueDir = path
	o.NumMemtables = 8
	o.NumLevelZeroTables = 8
	o.NumLevelZeroTablesStall = 16
	o.SyncWrites = fsync

	db, err := badger.Open(o)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &badgerdb{db: db}, nil
}

func (d *badgerdb) Set(key, value []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (d *badgerdb) Get(key []byte) (value []byte, err error) {
	var item *badger.Item
	d.db.View(func(txn *badger.Txn) error { item, err = txn.Get(key); return err })
	if err != nil {
		return
	}
	return item.ValueCopy(nil)
}

func (d *badgerdb) Each(fn func(key, value []byte) bool) {
	d.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   4,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !fn(k, v) {
				break
			}
		}
		return nil
	})
}

func (d *badgerdb) Close() error {
	return d.db.Close()
}
