package cache

import (
	"container/list"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
)

func New(size int, dir string) (*cache, error) {
	if err := os.MkdirAll(dir, os.FileMode(0774)); err != nil {
		return nil, err
	}
	return &cache{
		dir:  dir,
		size: size,
		mp:   make(map[string]*entry),
		hq:   &queue{0, new(list.List)},
		cq:   &queue{0, new(list.List)},
	}, nil
}

func (c *cache) IsExist(path string) bool {
	c.Lock()
	_, ok := c.mp[path]
	c.Unlock()
	return ok
}

func (c *cache) Read(path string, off int64, length int) ([]byte, error) {
	c.Lock()
	data, err := c.read(path, off, length)
	c.Unlock()
	return data, err
}

func (c *cache) Write(path string, data []byte) error {
	c.Lock()
	err := c.write(path, data)
	c.Unlock()
	return err
}

func (c *cache) read(path string, off int64, length int) ([]byte, error) {
	if e, ok := c.mp[path]; ok {
		c.get(e, 0)
		if data, err := c.readFile(e.path, off, length); err != nil {
			return nil, err
		} else {
			return data, nil
		}
	}
	return nil, errors.New("file not exist")
}

func (c *cache) write(path string, data []byte) error {
	if e, ok := c.mp[path]; ok {
		if err := c.writeFile(e.path, e.size, data, true); err != nil {
			return err
		}
		c.get(e, len(data))
		e.size += len(data)
		return nil
	}
	e := &entry{size: len(data), path: c.dir + "/" + path}
	if err := c.writeFile(e.path, 0, data, false); err != nil {
		return err
	}
	c.mp[path] = e
	c.set(e)
	return nil
}

func (c *cache) get(e *entry, size int) {
	switch e.typ {
	case H:
		isBack := e.h.Next() == nil
		c.hq.l.MoveToFront(e.h)
		if isBack {
			c.reduce()
		}
		return
	}
	switch {
	case e.h == nil:
		c.cq.l.MoveToFront(e.c)
		c.cq.size += size
		c.hq.size += size
		e.h = c.hq.l.PushFront(e)
	default:
		e.typ = H
		c.cq.l.Remove(e.c)
		c.cq.size -= e.size + size
		e.c = nil
		c.hq.l.MoveToFront(e.h)
		c.exchange()
		c.reduce()
	}
}

func (c *cache) set(e *entry) {
	switch {
	case c.hq.size < c.size:
		e.typ = H
		c.hq.size += e.size
		e.h = c.hq.l.PushFront(e)
	case c.cq.size < c.size/ColdMultiples:
		e.typ = C
		c.cq.size += e.size
		e.c = c.cq.l.PushFront(e)
	default:
		c.release()
		e.typ = C
		c.cq.size += e.size
		e.c = c.cq.l.PushFront(e)
	}
}

func (c *cache) release() {
	if ele := c.cq.l.Back(); ele != nil {
		e := ele.Value.(*entry)
		e.c = nil
		c.cq.l.Remove(ele)
		c.cq.size -= e.size
		if e.h != nil {
			c.hq.l.Remove(e.h)
			e.h = nil
			c.hq.size -= e.size
		}
		delete(c.mp, e.path)
		os.Remove(e.path)
	}
}

func (c *cache) reduce() {
	for ele := c.hq.l.Back(); ele != nil; ele = c.hq.l.Back() {
		e := ele.Value.(*entry)
		if e.typ == H {
			return
		}
		e.h = nil
		c.hq.l.Remove(ele)
		c.hq.size -= e.size
	}
}

func (c *cache) exchange() {
	if ele := c.hq.l.Back(); ele != nil {
		e := ele.Value.(*entry)
		if e.typ != H {
			return
		}
		c.hq.l.Remove(ele)
		e.h = nil
		e.typ = C
		c.hq.size -= e.size
		e.c = c.cq.l.PushFront(e)
	}
}

func (c *cache) readFile(path string, off int64, length int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data := make([]byte, length)
	n, err := f.ReadAt(data, off)
	switch {
	case err != nil:
		return nil, err
	case n != length:
		return nil, errors.New("read failed")
	}
	return data, nil
}

func (c *cache) writeFile(path string, size int, data []byte, isAppend bool) error {
	if !isAppend {
		dir, _ := filepath.Split(path)
		if err := os.MkdirAll(dir, os.FileMode(0774)); err != nil {
			return err
		}
		if err := ioutil.WriteFile(path, data, os.FileMode(0664)); err != nil {
			return err
		}
	} else {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, os.FileMode(0664))
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err = f.Write(data); err != nil {
			f.Truncate(int64(size))
			return err
		}
	}
	return nil
}
