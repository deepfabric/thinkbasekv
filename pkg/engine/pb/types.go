package pb

import "github.com/cockroachdb/pebble"

type pbEngine struct {
	db  *pebble.DB
	opt *pebble.WriteOptions
}

type pbBatch struct {
	db  *pebble.DB
	bat *pebble.Batch
	opt *pebble.WriteOptions
}

type pbIterator struct {
	itr *pebble.Iterator
}

type pbSnapshot struct {
	s *pebble.Snapshot
}
