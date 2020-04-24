package engine

import "errors"

var (
	NotExist = errors.New("Not Exist")
)

type DB interface {
	Sync() error
	Close() error
	NewBatch() (Batch, error)
	NewSnapshot() (Snapshot, error)
	NewIterator([]byte) (Iterator, error)

	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)
}

type Batch interface {
	Cancel() error
	Commit() error
	Del([]byte) error
	Set([]byte, []byte) error
}

type Iterator interface {
	Next() error
	Valid() bool
	Close() error
	Seek([]byte) error
	Key() []byte
	Value() ([]byte, error)
}

type Snapshot interface {
	Close() error
	Get([]byte) ([]byte, error)
	NewIterator([]byte) (Iterator, error)
}
