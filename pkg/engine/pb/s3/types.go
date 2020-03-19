package s3

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
	Region          string
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
}

type message struct {
	path    string
	rowpath string
}

type alis3 struct {
	cli  *s3.S3
	fs   cfs.FS
	opt  string
	mp   *sync.Map
	ch   chan struct{}
	mch  chan *message
	wg   sync.WaitGroup
	sess *session.Session
}

type file struct {
	dir  string
	name string
	a    *alis3
	fs   cfs.FS
}
