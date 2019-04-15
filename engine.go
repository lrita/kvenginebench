package kvenginebench

type Engine interface {
	Set(key, value []byte) error
	Get(key []byte) (value []byte, err error)
	Each(func(key, value []byte) bool)
	Close() error
}
