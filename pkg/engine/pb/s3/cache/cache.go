package cache

import (
	"container/list"
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

func (c *cache) Get(path string) ([]byte, bool) {
	c.Lock()
	data, ok := c.get(path)
	c.Unlock()
	return data, ok
}

func (c *cache) Add(path string, data []byte) bool {
	c.Lock()
	ok := c.add(path, data)
	c.Unlock()
	return ok
}

func (c *cache) get(path string) ([]byte, bool) {
	if e, ok := c.mp[path]; ok {
		c.chg(e)
		if e.worn {
			return nil, false
		}
		if data, err := ioutil.ReadFile(e.path); err != nil {
			return nil, false
		} else {
			return data, true
		}
	}
	return nil, false
}

func (c *cache) add(path string, data []byte) bool {
	if e, ok := c.mp[path]; ok {
		c.chg(e)
		if !c.writeFile(e.path, data) {
			e.worn = true
		} else {
			e.worn = false
		}
		return false
	}
	e := &entry{size: len(data), path: c.dir + "/" + path}
	if !c.writeFile(e.path, data) {
		e.worn = true
	} else {
		e.worn = false
	}
	c.mp[path] = e
	c.set(e)
	return true
}

func (c *cache) chg(e *entry) {
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
		e.h = c.hq.l.PushFront(e)
	default:
		e.typ = H
		c.cq.l.Remove(e.c)
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
		e.h = c.hq.l.PushFront(e)
	case c.cq.size < c.size/ColdMultiples:
		e.typ = C
		e.c = c.cq.l.PushFront(e)
	default:
		c.release()
		e.typ = C
		e.c = c.cq.l.PushFront(e)
	}
}

func (c *cache) release() {
	if ele := c.cq.l.Back(); ele != nil {
		e := ele.Value.(*entry)
		e.c = nil
		c.cq.l.Remove(ele)
		if e.h != nil {
			c.hq.l.Remove(e.h)
			e.h = nil
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
		e.c = c.cq.l.PushFront(e)
	}
}

func (c *cache) writeFile(path string, data []byte) bool {
	dir, _ := filepath.Split(path)
	if err := os.MkdirAll(dir, os.FileMode(0774)); err != nil {
		return false
	}
	if err := ioutil.WriteFile(path, data, os.FileMode(0664)); err != nil {
		return false
	}
	return true
}
