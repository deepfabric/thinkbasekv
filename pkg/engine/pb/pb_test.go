package pb

import (
	"fmt"
	"log"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/s3"
)

func TestPg(t *testing.T) {
	db := New("test.db", nil)
	//db := New("testinfinivision", newali())
	//db := New("testinfinivision-appid", newtencent())
	if err := db.Set([]byte("a"), []byte("a")); err != nil {
		log.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", string(v))
	bat, err := db.NewBatch()
	if err != nil {
		log.Fatal(err)
	}
	if err := bat.Set([]byte("b"), []byte("b")); err != nil {
		log.Fatal(err)
	}
	if err := bat.Commit(); err != nil {
		log.Fatal(err)
	}
	v, err = db.Get([]byte("b"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", string(v))
}

func newali() vfs.FS {
	endpoint := "http://oss-cn-hangzhou.aliyuncs.com"
	accessKeyID := ""
	accessKeySecret := ""
	acl := s3.PublicReadWrite
	fs, err := s3.New(endpoint, accessKeyID, accessKeySecret, acl)
	if err != nil {
		log.Fatal(err)
	}
	return fs
}

func newtencent() vfs.FS {
	endpoint := "cos.ap-chengdu.myqcloud.com"
	accessKeyID := ""
	accessKeySecret := ""
	acl := s3.PublicReadWrite
	fs, err := s3.New(endpoint, accessKeyID, accessKeySecret, acl)
	if err != nil {
		log.Fatal(err)
	}
	return fs
}
