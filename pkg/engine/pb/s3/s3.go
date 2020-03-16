package s3

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/s3/cache"
)

func New(cfg *Config, acl int) (*alis3, error) {
	c, err := cache.New(cfg.CacheSize, cfg.CacheDir)
	if err != nil {
		return nil, err
	}
	cli, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret)
	if err != nil {
		return nil, err
	}
	var opt oss.Option
	switch acl {
	case Private:
		opt = oss.ACL(oss.ACLPrivate)
	case PublicRead:
		opt = oss.ACL(oss.ACLPublicRead)
	case PublicReadWrite:
		opt = oss.ACL(oss.ACLPublicReadWrite)
	default:
		opt = oss.ACL(oss.ACLDefault)
	}
	return &alis3{c, opt, cli}, nil
}

func (a *alis3) Create(name string) (vfs.File, error) {
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return nil, err
	}
	if err := bkt.PutObject(s[1], strings.NewReader("")); err != nil {
		return nil, err
	}
	return &file{s[1], a.c, a.cli, bkt}, nil
}

func (a *alis3) Remove(name string) error {
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return err
	}
	if err := bkt.DeleteObject(s[1]); err != nil {
		return err
	}
	return nil
}

func (a *alis3) RemoveAll(name string) error {
	if s := strings.Split(name, "/"); len(s) > 2 {
		return a.Remove(name)
	}
	bkt, err := a.cli.Bucket(name)
	if err != nil {
		return err
	}
	marker := ""
	for {
		fs, err := bkt.ListObjects(oss.Marker(marker))
		if err != nil {
			return err
		}
		for _, f := range fs.Objects {
			if err := bkt.DeleteObject(f.Key); err != nil {
				return err
			}
		}
		if fs.IsTruncated {
			marker = fs.NextMarker
		} else {
			break
		}
	}
	return a.cli.DeleteBucket(name)
}

func (a *alis3) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	if err := a.Rename(oldname, newname); err != nil {
		return nil, err
	}
	return a.Open(newname)
}

func (a *alis3) Link(oldname, newname string) error {
	var r io.Reader

	{
		s := strings.Split(oldname, "/")
		bkt, err := a.cli.Bucket(s[0])
		if err != nil {
			return err
		}
		body, err := bkt.GetObject(s[1])
		if err != nil {
			return err
		}
		defer body.Close()
		r = body
	}
	{
		s := strings.Split(newname, "/")
		bkt, err := a.cli.Bucket(s[0])
		if err != nil {
			return err
		}
		if err := bkt.PutObject(s[1], r); err != nil {
			return err
		}
	}
	return nil
}

func (a *alis3) Rename(oldname, newname string) error {
	var r io.Reader

	{
		s := strings.Split(oldname, "/")
		bkt, err := a.cli.Bucket(s[0])
		if err != nil {
			return err
		}
		body, err := bkt.GetObject(s[1])
		if err != nil {
			return err
		}
		defer body.Close()
		r = body
	}
	{
		s := strings.Split(newname, "/")
		bkt, err := a.cli.Bucket(s[0])
		if err != nil {
			return err
		}
		if err := bkt.PutObject(s[1], r); err != nil {
			return err
		}
	}
	return a.Remove(oldname)
}

func (a *alis3) MkdirAll(dir string, _ os.FileMode) error {
	a.cli.CreateBucket(dir, a.opt)
	return nil
}

func (a *alis3) Lock(name string) (io.Closer, error) {
	return &file{c: a.c}, nil
}

func (a *alis3) OpenDir(name string) (vfs.File, error) {
	bkt, err := a.cli.Bucket(name)
	if err != nil {
		return nil, err
	}
	return &file{"", a.c, a.cli, bkt}, nil
}

func (a *alis3) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return nil, err
	}
	if !a.c.IsExist(name) { // file not exist in cache
		if _, err := bkt.GetObjectDetailedMeta(s[1]); err != nil {
			switch code := err.(oss.ServiceError).StatusCode; code {
			case 403, 404:
				return nil, os.ErrNotExist
			}
			return nil, err
		}
	}
	f := &file{s[1], a.c, a.cli, bkt}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

func (a *alis3) Stat(name string) (os.FileInfo, error) {
	f, err := a.Open(name)
	if err != nil {
		return nil, err
	}
	return f.(*file), nil
}

func (a *alis3) List(dir string) ([]string, error) {
	bkt, err := a.cli.Bucket(dir)
	if err != nil {
		return nil, err
	}
	marker := ""
	rs := []string{}
	for {
		lsRes, err := bkt.ListObjects(oss.Marker(marker))
		if err != nil {
			return nil, err
		}

		for _, object := range lsRes.Objects {
			rs = append(rs, object.Key)
		}
		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}
	return rs, nil
}

func (a *alis3) PathBase(p string) string {
	return path.Base(p)
}

func (a *alis3) PathJoin(elem ...string) string {
	return path.Join(elem...)
}

func (a *alis3) PathDir(p string) string {
	return path.Dir(p)
}

func (f *file) Sync() error {
	return nil
}

func (f *file) Close() error {
	return nil
}

func (f *file) Read(p []byte) (int, error) {
	if data, err := f.c.Read(f.bkt.BucketName+"/"+f.name, 0, len(p)); err == nil {
		copy(p, data)
		return len(p), nil
	}
	body, err := f.bkt.GetObject(f.name, oss.Range(0, int64(len(p))))
	if err != nil {
		return -1, err
	}
	defer body.Close()
	if data, err := ioutil.ReadAll(body); err != nil {
		return -1, err
	} else {
		copy(p, data)
		return len(data), nil
	}
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if data, err := f.c.Read(f.bkt.BucketName+"/"+f.name, off, len(p)); err == nil {
		copy(p, data)
		return len(p), nil
	}
	body, err := f.bkt.GetObject(f.name, oss.Range(off, off+int64(len(p))))
	if err != nil {
		return -1, err
	}
	defer body.Close()
	if data, err := ioutil.ReadAll(body); err != nil {
		return -1, err
	} else {
		copy(p, data)
		return len(data), nil
	}
}

func (f *file) Write(p []byte) (int, error) {
	if err := f.c.Write(f.bkt.BucketName+"/"+f.name, p); err != nil {
		return -1, err
	}
	{
		size := int(f.Size())
		data := make([]byte, size)
		if n, err := f.Read(data); err != nil {
			return -1, err
		} else {
			if n != size {
				return -1, errors.New("read failed")
			}
			p = append(data, p...)
		}
	}
	if err := f.bkt.PutObject(f.name, bytes.NewReader(p)); err != nil {
		return -1, err
	}
	return len(p), nil
}

func (f *file) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *file) Name() string {
	if len(f.name) == 0 {
		return f.bkt.BucketName
	}
	return f.name
}

func (f *file) Size() int64 {
	if len(f.name) == 0 {
		return 0
	}
	if md, err := f.bkt.GetObjectDetailedMeta(f.name); err != nil {
		return -1
	} else {
		if siz, err := strconv.Atoi(md["Content-Length"][0]); err == nil {
			return int64(siz)
		}
		return -1
	}
}

func (f *file) Mode() os.FileMode {
	return os.FileMode(0666)
}

func (f *file) ModTime() time.Time {
	return time.Now()
}

func (f *file) IsDir() bool {
	return len(f.name) == 0
}

func (f *file) Sys() interface{} {
	return nil
}
