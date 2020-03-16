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

type Cache interface {
	IsExist(string) bool
	Add(string, []byte) bool
	Get(string) ([]byte, bool)
}

type entry struct {
	typ  int
	size int
	worn bool
	path string
	h, c *list.Element
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
	mp     map[string]*entry
}
