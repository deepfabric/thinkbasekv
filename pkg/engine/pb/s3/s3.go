package s3

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkkv/pkg/engine/pb/s3/cfs"
)

func New(cfg *Config, acl int) (*alis3, cfs.FS, error) {
	a := new(alis3)
	fs, err := cfs.New(cfg.CacheSize, cfg.CacheDir, a, writeback)
	if err != nil {
		return nil, nil, err
	}
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         &cfg.Endpoint,
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.AccessKeySecret, ""),
	})
	if err != nil {
		fs.Close()
		return nil, nil, err
	}
	var opt string
	switch acl {
	case Private:
		opt = "private"
	case PublicRead:
		opt = "public-read"
	case PublicReadWrite:
		opt = "public-read-write"
	default:
		opt = "private"
	}
	a.ch = make(chan struct{})
	a.mch = make(chan *message, 1024)
	a.fs, a.opt, a.cli, a.mp, a.sess = fs, opt, s3.New(sess), new(sync.Map), sess
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
	return &file{s[0], s[1], a, a.fs}, nil
}

func (a *alis3) Remove(name string) error {
	if err, ok := a.fs.Remove(name); ok && err != nil {
		return err
	}
	s := strings.Split(name, "/")
	if _, err := a.cli.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s[0]),
		Key:    aws.String(s[1]),
	}); err != nil {
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
	iter := s3manager.NewDeleteListIterator(a.cli, &s3.ListObjectsInput{
		Bucket: aws.String(name),
	})
	if err := s3manager.NewBatchDeleteWithClient(a.cli).Delete(aws.BackgroundContext(), iter); err != nil {
		return err
	}
	if _, err := a.cli.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
		return err
	}
	if err := a.cli.WaitUntilBucketNotExists(&s3.HeadBucketInput{Bucket: aws.String(name)}); err != nil {
		return err
	}
	return nil
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
		buf := aws.NewWriteAtBuffer([]byte{})
		if _, err := s3manager.NewDownloader(a.sess).Download(buf, &s3.GetObjectInput{
			Bucket: aws.String(s[0]),
			Key:    aws.String(s[1]),
		}); err != nil {
			switch code := err.(awserr.RequestFailure).StatusCode(); code {
			case 403, 404:
				return nil
			}
			return err
		}
		r = bytes.NewReader(buf.Bytes())
	}
	{
		s := strings.Split(newname, "/")
		if _, err := s3manager.NewUploader(a.sess).Upload(&s3manager.UploadInput{
			Body:   r,
			Bucket: aws.String(s[0]),
			Key:    aws.String(s[1]),
		}); err != nil {
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
		buf := aws.NewWriteAtBuffer([]byte{})
		if _, err := s3manager.NewDownloader(a.sess).Download(buf, &s3.GetObjectInput{
			Bucket: aws.String(s[0]),
			Key:    aws.String(s[1]),
		}); err != nil {
			switch code := err.(awserr.RequestFailure).StatusCode(); code {
			case 403, 404:
				return nil
			}
			return err
		}
		r = bytes.NewReader(buf.Bytes())
	}
	{
		s := strings.Split(newname, "/")
		if _, err := s3manager.NewUploader(a.sess).Upload(&s3manager.UploadInput{
			Body:   r,
			Bucket: aws.String(s[0]),
			Key:    aws.String(s[1]),
		}); err != nil {
			return err
		}
	}
	return a.Remove(oldname)
}

func (a *alis3) MkdirAll(dir string, _ os.FileMode) error {
	if _, err := a.cli.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(dir)}); err != nil {
		if err.(awserr.RequestFailure).StatusCode() == 409 {
			return nil
		}
		return err
	}
	if err := a.cli.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: aws.String(dir)}); err != nil {
		return err
	}
	return nil
}

func (a *alis3) Lock(name string) (io.Closer, error) {
	return &file{fs: a.fs}, nil
}

func (a *alis3) OpenDir(name string) (vfs.File, error) {
	return &file{name, "", a, a.fs}, nil
}

func (a *alis3) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	s := strings.Split(name, "/")
	if _, ok := a.fs.IsExist(name); !ok { // file not exist in cache
		if _, ok := a.mp.Load(name); !ok {
			if _, err := a.cli.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(s[0]),
				Key:    aws.String(s[1]),
			}); err != nil {
				switch code := err.(awserr.RequestFailure).StatusCode(); code {
				case 403, 404:
					return nil, os.ErrNotExist
				}
				return nil, err
			}
		}
	}
	f := &file{s[0], s[1], a, a.fs}
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
	resp, err := a.cli.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dir)})
	if err != nil {
		return nil, err
	}
	rs := []string{}
	for _, item := range resp.Contents {
		rs = append(rs, *item.Key)
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
	for {
		f, err := os.Open(msg.path)
		if err != nil {
			continue
		}
		if _, err := s3manager.NewUploader(a.sess).Upload(&s3manager.UploadInput{
			Body:   f,
			Bucket: aws.String(s[0]),
			Key:    aws.String(s[1]),
		}); err != nil {
			continue
		}
		if isSST(s[1]) {
			os.Remove(msg.path)
		}
		return
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
	name := f.dir + "/" + f.name
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
			if md, err := f.a.cli.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(f.dir),
				Key:    aws.String(f.name),
			}); err != nil {
				if code := err.(awserr.RequestFailure).StatusCode(); code != 403 && code != 404 {
					return -1, err
				}
			} else {
				if int(*md.ContentLength) == size.(int) {
					break
				}
			}
		}
		f.a.mp.Delete(name)
	}
	buf := aws.NewWriteAtBuffer([]byte{})
	n, err := s3manager.NewDownloader(f.a.sess).Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(f.dir),
		Key:    aws.String(f.name),
		Range:  aws.String(fmt.Sprintf("bytes=%v-%v", 0, int64(len(p)-1))),
	})
	if err != nil {
		return -1, err
	}
	copy(p, buf.Bytes())
	if isEof {
		return int(n), io.EOF
	}
	return int(n), nil
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
	name := f.dir + "/" + f.name
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
			if md, err := f.a.cli.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(f.dir),
				Key:    aws.String(f.name),
			}); err != nil {
				if code := err.(awserr.RequestFailure).StatusCode(); code != 403 && code != 404 {
					return -1, err
				}
			} else {
				if int(*md.ContentLength) == size.(int) {
					break
				}
			}
		}
		f.a.mp.Delete(name)
	}
	buf := aws.NewWriteAtBuffer([]byte{})
	n, err := s3manager.NewDownloader(f.a.sess).Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(f.dir),
		Key:    aws.String(f.name),
		Range:  aws.String(fmt.Sprintf("bytes=%v-%v", off, off+int64(len(p)-1))),
	})
	if err != nil {
		return -1, err
	}
	copy(p, buf.Bytes())
	if isEof {
		return int(n), io.EOF
	}
	return int(n), nil
}

func (f *file) Write(p []byte) (int, error) {
	name := f.dir + "/" + f.name
	if err, ok := f.fs.Write(name, p); ok {
		return len(p), err
	}
	if size, ok := f.a.mp.Load(name); ok {
		for { // waiting for synchronization to complete
			if md, err := f.a.cli.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(f.dir),
				Key:    aws.String(f.name),
			}); err != nil {
				if code := err.(awserr.RequestFailure).StatusCode(); code != 403 && code != 404 {
					return -1, err
				}
			} else {
				if int(*md.ContentLength) == size.(int) {
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
		return f.dir
	}
	return f.name
}

func (f *file) Size() int64 {
	if len(f.name) == 0 {
		return 0
	}
	name := f.dir + "/" + f.name
	if size, ok := f.fs.IsExist(name); ok {
		return size
	}
	if size, ok := f.a.mp.Load(name); ok {
		return int64(size.(int))
	}
	if md, err := f.a.cli.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(f.dir),
		Key:    aws.String(f.name),
	}); err != nil {
		return -1
	} else {
		return *md.ContentLength
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
	if size >= 0 {
		a.mp.Store(rowpath, size)
		a.mch <- &message{path, rowpath}
	} else {
		if _, ok := a.mp.Load(rowpath); !ok {
			if isSST(rowpath) {
				os.Remove(path)
			}
		}
	}
}

func isSST(path string) bool {
	s := strings.Split(path, ".")
	return strings.Compare(s[len(s)-1], "sst") == 0
}
