package local

import (
	"fmt"
	"log"
	"testing"
)

func TestLocal(t *testing.T) {
	db, err := New("test.db")
	if err != nil {
		log.Fatal(err)
	}
	v, err := db.Get([]byte("1"))
	fmt.Printf("%v: %v\n", v, err)
}
