package ali

import "github.com/aliyun/aliyun-oss-go-sdk/oss"

type ali struct {
	cli *oss.Client
}

type file struct {
	name string
	cli  *oss.Client
	bkt  *oss.Bucket
}
