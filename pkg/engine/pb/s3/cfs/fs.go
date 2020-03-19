package cfs

import (
	"container/list"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func New(limit int, dir string, usr interface{}, cbk CallBack) (*fs, error) {
	if err := os.MkdirAll(dir, os.FileMode(0774)); err != nil {
		return nil, err
	}
	c := &fs{
		usr:   usr,
		cbk:   cbk,
		dir:   dir,
		limit: limit,
		mp:    make(map[string]*file),
		hq:    &queue{new(list.List)},
		cq:    &queue{new(list.List)},
	}
	if err := c.load(dir); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *fs) Close() error {
	c.Lock()
	defer c.Unlock()
	for _, f := range c.mp {
		if len(f.buf) > 0 {
			f.write(f.buf)
		}
		if f.fi != nil {
			f.fi.Close()
			f.fi = nil
		}
		if f.dirty {
			c.cbk(c.usr, f.path, f.rowpath, f.size)
		}
	}
	return nil
}

func (c *fs) IsExist(path string) (int64, bool) {
	c.Lock()
	defer c.Unlock()
	if f, ok := c.mp[path]; ok {
		return int64(f.size + len(f.buf)), ok
	}
	return -1, false
}

func (c *fs) Remove(path string) (error, bool) {
	var f *file
	var ok bool

	c.Lock()
	if f, ok = c.mp[path]; ok {
		if f.fi != nil {
			f.fi.Close()
			f.fi = nil
		}
		if f.h != nil {
			c.hq.l.Remove(f.h)
		}
		if f.c != nil {
			c.cq.l.Remove(f.c)
		}
		c.size -= f.size
		delete(c.mp, path)
		c.cbk(c.usr, f.path, f.rowpath, -1)
	}
	c.Unlock()
	return nil, ok
}

func (c *fs) RemoveAll(path string) error {
	fs, err := c.List(path)
	if err != nil {
		return err
	}
	c.Lock()
	for i, j := 0, len(fs); i < j; i++ {
		if f, ok := c.mp[path+"/"+fs[i]]; ok {
			if f.fi != nil {
				f.fi.Close()
				f.fi = nil
			}
			if f.h != nil {
				c.hq.l.Remove(f.h)
			}
			if f.c != nil {
				c.cq.l.Remove(f.c)
			}
			c.size -= f.size
			delete(c.mp, path+"/"+fs[i])
			c.cbk(c.usr, f.path, f.rowpath, -1)
		}
	}
	c.Unlock()
	return nil
}

func (c *fs) List(path string) ([]string, error) {
	f, err := os.Open(c.dir + "/" + path)
	if err != nil {
		return nil, err
	}
	return f.Readdirnames(-1)
}

func (c *fs) Link(oldname, newname string) (error, bool) {
	var ok bool
	var f *file
	var err error

	c.Lock()
	if f, ok = c.mp[oldname]; ok {
		if len(f.buf) > 0 {
			if err := f.write(f.buf); err != nil {
				c.Unlock()
				return err, ok
			}
			f.buf = f.buf[:0]
		}
		if err = os.Link(c.dir+"/"+oldname, c.dir+"/"+newname); err == nil {
			if f.fi != nil {
				f.fi.Close()
				f.fi = nil
			}
			nf := &file{size: f.size, dirty: true, path: c.dir + "/" + newname, rowpath: newname}
			c.mp[newname] = nf
			c.set(nf)
		}
	}
	c.Unlock()
	return err, ok
}

func (c *fs) Rename(oldname, newname string) (error, bool) {
	var ok bool
	var f *file
	var err error

	c.Lock()
	if f, ok = c.mp[oldname]; ok {
		if err = os.Rename(c.dir+"/"+oldname, c.dir+"/"+newname); err == nil {
			if f.fi != nil {
				f.fi.Close()
				f.fi = nil
			}
			f.rowpath = newname
			f.path = c.dir + "/" + newname
			c.mp[newname] = f
			delete(c.mp, oldname)
			c.get(f, 0)
		}
	}
	c.Unlock()
	return err, ok
}

func (c *fs) Create(path string) error {
	f := &file{dirty: true, path: c.dir + "/" + path, rowpath: path}
	if err := c.newFile(f.path); err != nil {
		return err
	}
	c.Lock()
	c.mp[path] = f
	c.set(f)
	c.Unlock()
	return nil
}

func (c *fs) Read(path string, off int64, length int) ([]byte, error, bool) {
	c.Lock()
	data, err, ok := c.readFile(path, off, length)
	c.Unlock()
	return data, err, ok
}

func (c *fs) Write(path string, data []byte) (error, bool) {
	c.Lock()
	err, ok := c.writeFile(path, data)
	c.Unlock()
	return err, ok
}

func (c *fs) load(dir string) error {
	d, err := os.Open(dir)
	switch {
	case err == nil:
		break
	case os.IsNotExist(err):
		return nil
	default:
		return err
	}
	fs, err := d.Readdir(-1)
	if err != nil {
		return err
	}
	for _, fp := range fs {
		if fp.IsDir() {
			if err := c.load(dir + "/" + fp.Name()); err != nil {
				return err
			}
		}
		path, _ := filepath.Rel(c.dir, dir+"/"+fp.Name())
		f := &file{size: int(fp.Size()), dirty: false, path: c.dir + "/" + path, rowpath: path}
		c.mp[path] = f
		c.set(f)
	}
	return nil
}

func (c *fs) readFile(path string, off int64, length int) ([]byte, error, bool) {
	if f, ok := c.mp[path]; ok {
		c.get(f, 0)
		if data, err := f.readFile(off, length); err != nil {
			return nil, err, true
		} else {
			return data, nil, true
		}
	}
	return nil, nil, false
}

func (c *fs) writeFile(path string, data []byte) (error, bool) {
	if f, ok := c.mp[path]; ok {
		c.get(f, len(data))
		if err := f.writeFile(data); err != nil {
			return err, ok
		}
		return nil, ok
	}
	return nil, false
}

func (c *fs) get(f *file, size int) {
	c.size += size
	switch f.typ {
	case H:
		isBack := f.h.Next() == nil
		c.hq.l.MoveToFront(f.h)
		if isBack {
			c.reduce()
		}
		return
	}
	switch {
	case f.h == nil:
		c.cq.l.MoveToFront(f.c)
		f.h = c.hq.l.PushFront(f)
	default:
		f.typ = H
		c.cq.l.Remove(f.c)
		f.c = nil
		c.hq.l.MoveToFront(f.h)
		c.exchange()
		c.reduce()
	}
}

func (c *fs) set(f *file) {
	c.size += f.size
	switch {
	case c.size < c.limit-c.limit/ColdMultiples:
		f.typ = H
		f.h = c.hq.l.PushFront(f)
	case c.size < c.limit:
		f.typ = C
		f.c = c.cq.l.PushFront(f)
	default:
		c.release()
		f.typ = C
		f.c = c.cq.l.PushFront(f)
	}
}

func (c *fs) release() {
	for e := c.cq.l.Back(); e != nil; e = e.Prev() {
		f := e.Value.(*file)
		if f.dirty {
			if len(f.buf) > 0 {
				f.write(f.buf)
			}
			c.cbk(c.usr, f.path, f.rowpath, f.size)
			f.dirty = false
		}
		if isSST(f.rowpath) {
			if f.fi != nil {
				f.fi.Close()
			}
			f.c = nil
			c.cq.l.Remove(e)
			if f.h != nil {
				f.h = nil
				c.hq.l.Remove(e)
			}
			c.size -= f.size
			delete(c.mp, f.rowpath)
			if c.size < c.limit {
				return
			}
		}
	}
}

func (c *fs) reduce() {
	for e := c.hq.l.Back(); e != nil; e = c.hq.l.Back() {
		f := e.Value.(*file)
		if f.typ == H {
			return
		}
		f.h = nil
		c.hq.l.Remove(e)
	}
}

func (c *fs) exchange() {
	if e := c.hq.l.Back(); e != nil {
		f := e.Value.(*file)
		if f.typ != H {
			return
		}
		c.hq.l.Remove(e)
		f.h = nil
		f.typ = C
		f.c = c.cq.l.PushFront(f)
	}
}

func (c *fs) newFile(path string) error {
	dir, _ := filepath.Split(path)
	if err := os.MkdirAll(dir, os.FileMode(0774)); err != nil {
		return err
	}
	if err := ioutil.WriteFile(path, []byte{}, os.FileMode(0664)); err != nil {
		return err
	}
	return nil
}

func (f *file) readFile(off int64, length int) ([]byte, error) {
	var data []byte

	if int(off) < f.size { // get from file
		fi, err := os.Open(f.path)
		if err != nil {
			return nil, err
		}
		defer fi.Close()
		size := length
		if int(off)+length > f.size {
			size = f.size - int(off)
		}
		data = make([]byte, size)
		n, err := fi.ReadAt(data, off)
		switch {
		case err != nil:
			return nil, err
		case n != size:
			return nil, errors.New("read failed")
		}
	}
	if int(off)+length > f.size { // get from buffer
		start := 0
		if int(off) > f.size {
			start = int(off) - f.size
		}
		end := int(off) + length - f.size
		data = append(data, f.buf[start:end]...)
	}
	return data, nil
}

func (f *file) writeFile(data []byte) error {
	f.dirty = true
	f.buf = append(f.buf, data...)
	if len(f.buf) >= FlushSize {
		if err := f.write(f.buf); err != nil {
			return err
		}
		f.buf = f.buf[:0]
	}
	return nil
}

func (f *file) write(data []byte) error {
	if f.fi == nil {
		fi, err := os.OpenFile(f.path, os.O_WRONLY|os.O_APPEND, os.FileMode(0664))
		if err != nil {
			return err
		}
		f.fi = fi
	}
	if _, err := f.fi.Write(data); err != nil {
		return err
	}
	f.size += len(data)
	return nil
}

func isSST(path string) bool {
	s := strings.Split(path, ".")
	return strings.Compare(s[len(s)-1], "sst") == 0
}
