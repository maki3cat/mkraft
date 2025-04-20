package raft

import (
	"fmt"
	"testing"
)

type DummyIface interface {
	Dummay()
}

type DummyImpl struct {
}

func (d *DummyImpl) Dummay() {
	fmt.Println("DummyImpl Dummay called")
}

func TestPrintIface(t *testing.T) {
	var iface DummyIface = &DummyImpl{}
	fmt.Println(iface)
}
