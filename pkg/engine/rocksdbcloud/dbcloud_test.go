package rocksdbcloud


import (
	"github.com/facebookgo/ensure"
	"github.com/tecbot/gorocksdb"
	"testing"
	"unsafe"
)

func newTestCloudDB(t *testing.T, applyOpts func(opts *gorocksdb.Options)) *DBCloud {

	//dir, err := ioutil.TempDir("", "/tmp/rocksdb_c_cloud_example")
	//ensure.Nil(t, err)

	kBucketname := "cloud-c-example-swj"
	kRegion := "cn-east-1"
	kDbPath := "/tmp/rocksdb_c_cloud_example"

	kBucketPrefix := "rockset-"


	cloudEnvOpts := NewCloudEnvOptions()

	//cloudEnvOpts.SetCredentials("W2YQeHO6c_79J9xszcEK3StdveVNdvjF0-gJuLqL", "9kU1ZT8jgs6ntO80lMSvtHrZqpM9jb3dEfBAHRzD")
	cloudEnvOpts.SetEndPoint("http://s3-cn-east-1.qiniucs.com")
	cloudEnvOpts.SetSrcBucket(kBucketname,kBucketPrefix)
	cloudEnvOpts.SetDstBucket(kBucketname,kBucketPrefix)

	cloudenv,err := NewAwsCloudEnv(kBucketname,kDbPath,kRegion,kBucketname,kDbPath,kRegion,cloudEnvOpts)
	ensure.Nil(t, err)

	cloudEnvOpts.SetEnv((*Env)(unsafe.Pointer(cloudenv)))

	//opts := gorocksdb.NewNativeOptions(cloudEnvOpts.cc)
	//opts := gorocksdb.NewDefaultOptions()
	// test the ratelimiter
	//rateLimiter := gorocksdb.NewRateLimiter(1024, 100*1000, 10)
	//opts.SetRateLimiter(rateLimiter)
	//opts.SetCreateIfMissing(true)

	//opts.SetEnv((*gorocksdb.Env)(unsafe.Pointer(cloudenv)))

	//if applyOpts != nil {
	//	applyOpts(opts)
	//}


	dbcloud, err := OpenCloudDb(cloudEnvOpts, kDbPath,"",0)
	ensure.Nil(t, err)

	return dbcloud
}

func TestOpenCloudDb(t *testing.T) {

	dbcloud := newTestCloudDB(t, nil)
	defer dbcloud.Close()
}


func TestCloudDBCRUD(t *testing.T) {
	dbcloud := newTestCloudDB(t, nil)
	defer dbcloud.Close()
	var db *gorocksdb.DB = (*gorocksdb.DB)(unsafe.Pointer(dbcloud))

	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("")
		givenVal2 = []byte("world1")
		wo        = gorocksdb.NewDefaultWriteOptions()
		ro        = gorocksdb.NewDefaultReadOptions()
	)

	// create
	ensure.Nil(t, db.Put(wo, givenKey, givenVal1))

	// retrieve
	v1, err := db.Get(ro, givenKey)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), givenVal1)

	// update
	ensure.Nil(t, db.Put(wo, givenKey, givenVal2))
	v2, err := db.Get(ro, givenKey)
	defer v2.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v2.Data(), givenVal2)

	// retrieve pinned
	v3, err := db.GetPinned(ro, givenKey)
	defer v3.Destroy()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v3.Data(), givenVal2)

	// delete
	ensure.Nil(t, db.Delete(wo, givenKey))
	v4, err := db.Get(ro, givenKey)
	ensure.Nil(t, err)
	ensure.True(t, v4.Data() == nil)

	// retrieve missing pinned
	v5, err := db.GetPinned(ro, givenKey)
	defer v5.Destroy()
	ensure.Nil(t, err)
	ensure.True(t, v5.Data() == nil)
}
