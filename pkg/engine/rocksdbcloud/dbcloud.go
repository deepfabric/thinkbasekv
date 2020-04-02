package rocksdbcloud

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"unsafe"
	"errors"
)

// DBCloud is a reusable handle to a RocksDB database on disk, created by Open.
type DBCloud struct {
	c    *C.rocksdb_cloud_t
}


// OpenDb opens a database with the specified options.
func OpenCloudDb(opts *CloudEnvOptions, name string, cachePath string, cacheSize uint64) (*DBCloud, error) {
	var (
		cErr  *C.char
	)
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cCachePath := C.CString(cachePath)
	defer C.free(unsafe.Pointer(cCachePath))



	db := C.rocksdb_cloud_open(opts.cc, cName, cCachePath, C.uint64_t(cacheSize), &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return &DBCloud{
		c: db,
	}, nil
}


// Close closes the database.
func (db *DBCloud) Close() {
	C.rocksdb_cloud_close(db.c)
	db.c = nil
}
