package ali

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cockroachdb/pebble/vfs"
)

func New(endpoint, accessKeyID, accessKeySecret string) (*ali, error) {
	cli, err := oss.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		return nil, err
	}
	return &ali{cli}, nil
}

func (a *ali) Create(name string) (vfs.File, error) {
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return nil, err
	}
	if err := bkt.PutObject(s[1], strings.NewReader("")); err != nil {
		return nil, err
	}
	return &file{s[1], a.cli, bkt}, nil
}

func (a *ali) Remove(name string) error {
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

func (a *ali) RemoveAll(name string) error {
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

func (a *ali) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	if err := a.Rename(oldname, newname); err != nil {
		return nil, err
	}
	return a.Open(newname)
}

func (a *ali) Link(oldname, newname string) error {
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

func (a *ali) Rename(oldname, newname string) error {
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

func (a *ali) MkdirAll(dir string, _ os.FileMode) error {
	return a.cli.CreateBucket(dir, oss.ACL(oss.ACLPublicReadWrite))
}

func (a *ali) Lock(name string) (io.Closer, error) {
	return &file{}, nil
}

func (a *ali) OpenDir(name string) (vfs.File, error) {
	bkt, err := a.cli.Bucket(name)
	if err != nil {
		return nil, err
	}
	return &file{"", a.cli, bkt}, nil
}

func (a *ali) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return nil, err
	}
	if ok, err := bkt.IsObjectExist(s[1]); err != nil {
		return nil, err
	} else if !ok {
		return nil, os.ErrNotExist
	}
	f := &file{s[1], a.cli, bkt}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

func (a *ali) Stat(name string) (os.FileInfo, error) {
	f, err := a.Open(name)
	if err != nil {
		return nil, err
	}
	return f.(*file), nil
}

func (a *ali) List(dir string) ([]string, error) {
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

func (a *ali) PathBase(p string) string {
	return path.Base(p)
}

func (a *ali) PathJoin(elem ...string) string {
	return path.Join(elem...)
}

func (a *ali) PathDir(p string) string {
	return path.Dir(p)
}

func (f *file) Sync() error {
	return nil
}

func (f *file) Close() error {
	return nil
}

func (f *file) Read(p []byte) (int, error) {
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
