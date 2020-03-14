package pb

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/ali"
)

func TestPg(t *testing.T) {
	endpoint := "http://oss-cn-hangzhou.aliyuncs.com"
	accessKeyID := ""
	accessKeySecret = ""
	fs, err := ali.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		log.Fatal(err)
	}
	db := New("testinfinivision", fs)
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
