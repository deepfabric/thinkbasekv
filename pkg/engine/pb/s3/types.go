package s3

import "github.com/aliyun/aliyun-oss-go-sdk/oss"

const (
	Private = iota
	PublicRead
	PublicReadWrite
)

type alis3 struct {
	opt oss.Option
	cli *oss.Client
}

type file struct {
	name string
	cli  *oss.Client
	bkt  *oss.Bucket
}
