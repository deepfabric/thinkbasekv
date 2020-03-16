package cache

import (
	"container/list"
	"sync"
)

const (
	I = iota
	H
	C
)

const (
	ColdMultiples = 1024
)

type CallBack func(interface{}, string, string)

type Cache interface {
	Close() error
	Write(string, []byte) error
	IsExist(string) (int64, bool)
	Read(string, int64, int) ([]byte, error)
}

type entry struct {
	typ     int
	size    int
	dirty   bool
	path    string
	rowpath string
	h, c    *list.Element
}

type queue struct {
	size int
	l    *list.List
}

type cache struct {
	sync.RWMutex
	size   int
	dir    string
	hq, cq *queue
	cbk    CallBack
	usr    interface{}
	mp     map[string]*entry
}
