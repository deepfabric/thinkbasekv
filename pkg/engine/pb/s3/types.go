package s3

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/s3/cache"
)

const (
	Private = iota
	PublicRead
	PublicReadWrite
)

type Config struct {
	CacheSize       int
	CacheDir        string
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
}

type alis3 struct {
	c   cache.Cache
	opt oss.Option
	cli *oss.Client
}

type file struct {
	name string
	c    cache.Cache
	cli  *oss.Client
	bkt  *oss.Bucket
}
