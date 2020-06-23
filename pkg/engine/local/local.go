package local

import (
	"io/ioutil"
	"os"
	"path"
	"syscall"

	"github.com/deepfabric/thinkkv/pkg/engine"
)

func New(path string) (*local, error) {
	if err := os.MkdirAll(path, os.FileMode(0775)); err != nil {
		return nil, err
	}
	return &local{path}, nil
}

func (_ *local) Sync() error {
	syscall.Sync()
	return nil
}

func (_ *local) Close() error {
	return nil
}

func (l *local) Del(k []byte) error {
	return os.Remove(path.Join(l.path, string(k)))
}

func (l *local) Set(k, v []byte) error {
	return ioutil.WriteFile(path.Join(l.path, string(k)), v, os.FileMode(0664))
}

func (l *local) Get(k []byte) ([]byte, error) {
	v, err := ioutil.ReadFile(path.Join(l.path, string(k)))
	if os.IsNotExist(err) {
		return nil, engine.NotExist
	}
	return v, err
}

func (_ *local) NewBatch() (engine.Batch, error) {
	return &batch{}, nil
}

func (_ *local) NewSnapshot() (engine.Snapshot, error) {
	return &snapshot{}, nil
}

func (_ *local) NewIterator(_ []byte) (engine.Iterator, error) {
	return &iterator{}, nil
}

func (_ *batch) Cancel() error {
	return nil
}

func (_ *batch) Commit() error {
	return nil
}

func (_ *batch) Del(_ []byte) error {
	return nil
}

func (_ *batch) Set(_, _ []byte) error {
	return nil
}

func (_ *iterator) Close() error {
	return nil
}

func (_ *iterator) Next() error {
	return nil
}

func (_ *iterator) Valid() bool {
	return false
}

func (_ *iterator) Seek(_ []byte) error {
	return nil
}

func (_ *iterator) Key() []byte {
	return nil
}

func (_ *iterator) Value() ([]byte, error) {
	return nil, nil
}

func (_ *snapshot) Close() error {
	return nil
}

func (_ *snapshot) Get(_ []byte) ([]byte, error) {
	return nil, nil
}

func (_ *snapshot) NewIterator(_ []byte) (engine.Iterator, error) {
	return &iterator{}, nil
}
