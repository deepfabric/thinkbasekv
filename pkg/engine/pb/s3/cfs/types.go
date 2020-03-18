package cfs

import (
	"container/list"
	"os"
	"sync"
)

const (
	H = iota
	C
)

const (
	ColdMultiples = 1024
)

const (
	FlushSize = 1024 * 1024
)

type CallBack func(interface{}, string, string, int)

type FS interface {
	Close() error
	Create(string) error
	IsExist(string) (int64, bool)
	Remove(string) (error, bool)
	Link(string, string) (error, bool)
	Rename(string, string) (error, bool)

	RemoveAll(string) error
	List(string) ([]string, error)

	Write(string, []byte) (error, bool)
	Read(string, int64, int) ([]byte, error, bool)
}

type file struct {
	typ     int
	size    int
	dirty   bool
	path    string
	rowpath string
	buf     []byte
	fi      *os.File
	h, c    *list.Element
}

type queue struct {
	l *list.List
}

// cache filesystem
type fs struct {
	sync.RWMutex
	size   int
	limit  int
	dir    string
	hq, cq *queue
	cbk    CallBack
	usr    interface{}
	mp     map[string]*file
}
