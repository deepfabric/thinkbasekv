package bg

import "github.com/dgraph-io/badger"

type bgEngine struct {
	db *badger.DB
}

type bgBatch struct {
	tx *badger.Txn
}

type bgIterator struct {
	k   []byte
	tx  *badger.Txn
	itr *badger.Iterator
}
