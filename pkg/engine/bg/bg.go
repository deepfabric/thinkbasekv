package bg

import (
	"github.com/deepfabric/thinkbasekv/pkg/engine"
	"github.com/dgraph-io/badger"
)

func New(name string) engine.DB {
	opts := badger.DefaultOptions(name)
	opts.SyncWrites = false
	if db, err := badger.Open(opts); err != nil {
		return nil
	} else {
		return &bgEngine{db}
	}
}

func (db *bgEngine) Close() error {
	return db.db.Close()
}

func (db *bgEngine) NewBatch() (engine.Batch, error) {
	return &bgBatch{db.db.NewTransaction(true)}, nil
}

func (db *bgEngine) NewIterator(k []byte) (engine.Iterator, error) {
	tx := db.db.NewTransaction(false)
	opt := badger.DefaultIteratorOptions
	opt.Prefix = k
	opt.PrefetchValues = false
	return &bgIterator{k, tx, tx.NewIterator(opt)}, nil
}

func (db *bgEngine) Del(k []byte) error {
	tx := db.db.NewTransaction(true)
	defer tx.Discard()
	if err := del(tx, k); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *bgEngine) Set(k, v []byte) error {
	tx := db.db.NewTransaction(true)
	defer tx.Discard()
	if err := set(tx, k, v); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *bgEngine) Get(k []byte) ([]byte, error) {
	tx := db.db.NewTransaction(false)
	defer tx.Discard()
	return get(tx, k)
}

func (tx *bgBatch) Cancel() error {
	tx.tx.Discard()
	return nil
}

func (tx *bgBatch) Commit() error {
	return tx.tx.Commit()
}

func (tx *bgBatch) Del(k []byte) error {
	return del(tx.tx, k)
}

func (tx *bgBatch) Set(k, v []byte) error {
	return set(tx.tx, k, v)
}

func (itr *bgIterator) Close() error {
	itr.itr.Close()
	itr.tx.Discard()
	return nil
}

func (itr *bgIterator) Next() error {
	itr.itr.Next()
	return nil
}

func (itr *bgIterator) Valid() bool {
	return itr.itr.ValidForPrefix(itr.k)
}

func (itr *bgIterator) Seek(k []byte) error {
	itr.itr.Seek(k)
	return nil
}

func (itr *bgIterator) Key() []byte {
	return itr.itr.Item().KeyCopy(nil)
}

func (itr *bgIterator) Value() ([]byte, error) {
	return itr.itr.Item().ValueCopy(nil)
}

func del(tx *badger.Txn, k []byte) error {
	return tx.Delete(k)
}

func set(tx *badger.Txn, k, v []byte) error {
	return tx.Set(k, v)
}

func get(tx *badger.Txn, k []byte) ([]byte, error) {
	it, err := tx.Get(k)
	if err == badger.ErrKeyNotFound {
		err = engine.NotExist
	}
	if err != nil {
		return nil, err
	}
	return it.ValueCopy(nil)
}
