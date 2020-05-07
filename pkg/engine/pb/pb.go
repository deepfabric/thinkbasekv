package pb

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkkv/pkg/engine"
)

func New(name string, fs vfs.FS, syncWrite bool) engine.DB {
	if db, err := pebble.Open(name, &pebble.Options{FS: fs, DisableWAL: !syncWrite}); err != nil {
		return nil
	} else {
		return &pbEngine{db, &pebble.WriteOptions{syncWrite}}
	}
}

func (db *pbEngine) Sync() error {
	return db.db.Flush()
}

func (db *pbEngine) Close() error {
	return db.db.Close()
}

func (db *pbEngine) NewBatch() (engine.Batch, error) {
	return &pbBatch{db: db.db, bat: db.db.NewBatch(), opt: db.opt}, nil
}

func (db *pbEngine) NewSnapshot() (engine.Snapshot, error) {
	return &pbSnapshot{db.db.NewSnapshot()}, nil
}

func (db *pbEngine) NewIterator(k []byte) (engine.Iterator, error) {
	n := len(k) - 1
	u := make([]byte, len(k))
	copy(u, k)
	u[n] = k[n] + 1
	return &pbIterator{itr: db.db.NewIter(&pebble.IterOptions{
		LowerBound: k,
		UpperBound: u,
	})}, nil
}

func (db *pbEngine) Del(k []byte) error {
	return db.db.Delete(k, db.opt)
}

func (db *pbEngine) Set(k, v []byte) error {
	return db.db.Set(k, v, db.opt)
}

func (db *pbEngine) Get(k []byte) ([]byte, error) {
	v, c, err := db.db.Get(k)
	if err == pebble.ErrNotFound {
		err = engine.NotExist
	}
	if err != nil {
		return nil, err
	}
	r := make([]byte, len(v))
	copy(r, v)
	c.Close()
	return r, nil
}

func (b *pbBatch) Cancel() error {
	return nil
}

func (b *pbBatch) Commit() error {
	return b.db.Apply(b.bat, b.opt)
}

func (b *pbBatch) Del(k []byte) error {
	return b.bat.Delete(k, b.opt)
}

func (b *pbBatch) Set(k, v []byte) error {
	return b.bat.Set(k, v, b.opt)
}

func (itr *pbIterator) Close() error {
	itr.itr.Close()
	return nil
}

func (itr *pbIterator) Next() error {
	itr.itr.Next()
	return nil
}

func (itr *pbIterator) Valid() bool {
	return itr.itr.Valid()
}

func (itr *pbIterator) Seek(k []byte) error {
	itr.itr.SeekGE(k)
	return nil
}

func (itr *pbIterator) Key() []byte {
	k := itr.itr.Key()
	r := make([]byte, len(k))
	copy(r, k)
	return r
}

func (itr *pbIterator) Value() ([]byte, error) {
	v := itr.itr.Value()
	r := make([]byte, len(v))
	copy(r, v)
	return r, nil
}

func (s *pbSnapshot) Close() error {
	return s.s.Close()
}

func (s *pbSnapshot) Get(k []byte) ([]byte, error) {
	v, c, err := s.s.Get(k)
	if err == pebble.ErrNotFound {
		err = engine.NotExist
	}
	if err != nil {
		return nil, err
	}
	r := make([]byte, len(v))
	copy(r, v)
	c.Close()
	return r, nil
}

func (s *pbSnapshot) NewIterator(k []byte) (engine.Iterator, error) {
	n := len(k) - 1
	u := make([]byte, len(k))
	copy(u, k)
	u[n] = k[n] + 1
	return &pbIterator{itr: s.s.NewIter(&pebble.IterOptions{
		LowerBound: k,
		UpperBound: u,
	})}, nil
}
