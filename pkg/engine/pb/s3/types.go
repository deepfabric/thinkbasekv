package s3

import (
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/s3/cfs"
)

const (
	Private = iota
	PublicRead
	PublicReadWrite
)

type FS interface {
	vfs.FS
	Run()
	Stop()
}

type Config struct {
	CacheSize       int
	CacheDir        string
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
}

type message struct {
	path    string
	rowpath string
}

type alis3 struct {
	fs  cfs.FS
	mp  *sync.Map
	opt oss.Option
	cli *oss.Client
	ch  chan struct{}
	mch chan *message
	wg  sync.WaitGroup
}

type file struct {
	name string
	a    *alis3
	fs   cfs.FS
	cli  *oss.Client
	bkt  *oss.Bucket
}
