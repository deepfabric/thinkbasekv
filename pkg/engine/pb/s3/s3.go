package s3

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/s3/cfs"
)

func New(cfg *Config, acl int) (*alis3, cfs.FS, error) {
	a := new(alis3)
	fs, err := cfs.New(cfg.CacheSize, cfg.CacheDir, a, writeback)
	if err != nil {
		return nil, nil, err
	}
	cli, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret)
	if err != nil {
		return nil, nil, err
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
	a.ch = make(chan struct{})
	a.mch = make(chan *message, 1024)
	a.fs, a.opt, a.cli, a.mp = fs, opt, cli, new(sync.Map)
	return a, fs, nil
}

func (a *alis3) Run() {
	for {
		select {
		case <-a.ch:
			for len(a.mch) > 0 {
				msg := <-a.mch
				a.wg.Add(1)
				go a.dealMessage(msg)
			}
			a.ch <- struct{}{}
			return
		case msg := <-a.mch:
			a.wg.Add(1)
			go a.dealMessage(msg)
		}
	}
}

func (a *alis3) Stop() {
	a.ch <- struct{}{}
	<-a.ch
	close(a.ch)
	a.wg.Wait()
	close(a.mch)
}

func (a *alis3) Create(name string) (vfs.File, error) {
	if err := a.fs.Create(name); err != nil {
		return nil, err
	}
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return nil, err
	}
	return &file{s[1], a, a.fs, a.cli, bkt}, nil
}

func (a *alis3) Remove(name string) error {
	a.mp.Delete(name)
	if err, ok := a.fs.Remove(name); ok && err != nil {
		return err
	}
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
	if err := a.fs.RemoveAll(name); err != nil {
		return err
	}
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

	if err, ok := a.fs.Rename(oldname, newname); ok && err != nil {
		return err
	}
	{
		s := strings.Split(oldname, "/")
		bkt, err := a.cli.Bucket(s[0])
		if err != nil {
			return err
		}
		body, err := bkt.GetObject(s[1])
		if err != nil {
			switch code := err.(oss.ServiceError).StatusCode; code {
			case 403, 404:
				return nil
			}
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

	if err, ok := a.fs.Rename(oldname, newname); ok && err != nil {
		return err
	}
	{
		s := strings.Split(oldname, "/")
		bkt, err := a.cli.Bucket(s[0])
		if err != nil {
			return err
		}
		body, err := bkt.GetObject(s[1])
		if err != nil {
			switch code := err.(oss.ServiceError).StatusCode; code {
			case 403, 404:
				return nil
			}
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
	if err := a.cli.CreateBucket(dir, a.opt); err != nil && err.(oss.ServiceError).StatusCode != 409 {
		return err
	}
	return nil
}

func (a *alis3) Lock(name string) (io.Closer, error) {
	return &file{fs: a.fs}, nil
}

func (a *alis3) OpenDir(name string) (vfs.File, error) {
	bkt, err := a.cli.Bucket(name)
	if err != nil {
		return nil, err
	}
	return &file{"", a, a.fs, a.cli, bkt}, nil
}

func (a *alis3) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	s := strings.Split(name, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		return nil, err
	}
	if _, ok := a.fs.IsExist(name); !ok { // file not exist in cache
		if _, err := bkt.GetObjectDetailedMeta(s[1]); err != nil {
			switch code := err.(oss.ServiceError).StatusCode; code {
			case 403, 404:
				return nil, os.ErrNotExist
			}
			return nil, err
		}
	}
	f := &file{s[1], a, a.fs, a.cli, bkt}
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
	{
		fs, err := a.fs.List(dir)
		if err != nil {
			return nil, err
		}
		mp := make(map[string]struct{})
		for _, s := range rs {
			mp[s] = struct{}{}
		}
		for i, j := 0, len(fs); i < j; i++ {
			if _, ok := mp[fs[i]]; !ok {
				rs = append(rs, fs[i])
			}
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

func (a *alis3) dealMessage(msg *message) {
	defer a.wg.Done()
	s := strings.Split(msg.rowpath, "/")
	bkt, err := a.cli.Bucket(s[0])
	if err != nil {
		panic(err)
	}
	f, err := os.Open(msg.path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := bkt.PutObject(s[1], f); err != nil {
		panic(err)
	}
	if isSST(msg.path) {
		os.Remove(msg.path)
	}
}

func (f *file) Sync() error {
	return nil
}

func (f *file) Close() error {
	return nil
}

func (f *file) Read(p []byte) (int, error) {
	isEof := false
	if size := int(f.Size()); len(p) > size {
		p = p[:size]
		isEof = true
	}
	if len(p) == 0 {
		if isEof {
			return 0, io.EOF
		}
		return 0, nil
	}
	name := f.bkt.BucketName + "/" + f.name
	if data, err, ok := f.fs.Read(name, 0, len(p)); ok {
		if err != nil {
			return -1, nil
		}
		copy(p, data)
		if isEof {
			return len(p), io.EOF
		}
		return len(p), nil
	}
	if size, ok := f.a.mp.Load(name); ok {
		for { // waiting for synchronization to complete
			if md, err := f.bkt.GetObjectDetailedMeta(f.name); err != nil {
				if code := err.(oss.ServiceError).StatusCode; code != 403 && code != 404 {
					return -1, err
				}
			} else {
				length, err := strconv.Atoi(md["Content-Length"][0])
				if err != nil {
					return -1, err
				}
				if size == length {
					break
				}
			}
		}
		f.a.mp.Delete(name)
	}
	body, err := f.bkt.GetObject(f.name, oss.Range(0, int64(len(p)-1)))
	if err != nil {
		return -1, err
	}
	defer body.Close()
	if data, err := ioutil.ReadAll(body); err != nil {
		return -1, err
	} else {
		copy(p, data)
		if isEof {
			return len(data), io.EOF
		}
		return len(data), nil
	}
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	isEof := false
	if size := int(f.Size()); int(off)+len(p) > size {
		isEof = true
		p = p[:size-int(off)]
	}
	if len(p) == 0 {
		if isEof {
			return 0, io.EOF
		}
		return 0, nil
	}
	name := f.bkt.BucketName + "/" + f.name
	if data, err, ok := f.fs.Read(name, off, len(p)); ok {
		if err != nil {
			return -1, nil
		}
		copy(p, data)
		if isEof {
			return len(p), io.EOF
		}
		return len(p), nil
	}
	if size, ok := f.a.mp.Load(name); ok {
		for { // waiting for synchronization to complete
			if md, err := f.bkt.GetObjectDetailedMeta(f.name); err != nil {
				if code := err.(oss.ServiceError).StatusCode; code != 403 && code != 404 {
					return -1, err
				}
			} else {
				length, err := strconv.Atoi(md["Content-Length"][0])
				if err != nil {
					return -1, err
				}
				if size == length {
					break
				}
			}
		}
		f.a.mp.Delete(name)
	}
	body, err := f.bkt.GetObject(f.name, oss.Range(off, off+int64(len(p)-1)))
	if err != nil {
		return -1, err
	}
	defer body.Close()
	if data, err := ioutil.ReadAll(body); err != nil {
		return -1, err
	} else {
		copy(p, data)
		if isEof {
			return len(p), io.EOF
		}
		return len(data), nil
	}
}

func (f *file) Write(p []byte) (int, error) {
	name := f.bkt.BucketName + "/" + f.name
	if err, ok := f.fs.Write(name, p); ok {
		return len(p), err
	}
	if size, ok := f.a.mp.Load(name); ok {
		for { // waiting for synchronization to complete
			if md, err := f.bkt.GetObjectDetailedMeta(f.name); err != nil {
				if code := err.(oss.ServiceError).StatusCode; code != 403 && code != 404 {
					return -1, err
				}
			} else {
				length, err := strconv.Atoi(md["Content-Length"][0])
				if err != nil {
					return -1, err
				}
				if size == length {
					break
				}
			}
		}
		f.a.mp.Delete(name)
	}
	{
		size := int(f.Size())
		data := make([]byte, size)
		n, err := f.Read(data)
		switch {
		case err != nil:
			return -1, nil
		case n != size:
			return -1, errors.New("write failed")
		}
		if err = f.fs.Create(name); err != nil {
			return -1, err
		}
		if err, _ := f.fs.Write(name, data); err != nil {
			return -1, err
		}
	}
	if err, _ := f.fs.Write(name, p); err != nil {
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
	name := f.bkt.BucketName + "/" + f.name
	if size, ok := f.fs.IsExist(name); ok {
		return size
	}
	if size, ok := f.a.mp.Load(name); ok {
		return int64(size.(int))
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

func writeback(usr interface{}, path string, rowpath string, size int) {
	a := usr.(*alis3)
	a.mp.Store(rowpath, size)
	a.mch <- &message{path, rowpath}
}

func isSST(path string) bool {
	s := strings.Split(path, ".")
	return strings.Compare(s[len(s)-1], "sst") == 0
}
