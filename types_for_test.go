package datastore_test

import "encoding/gob"

type NameDocument struct {
	Identifier uint64
	Name       string
}

func (n *NameDocument) ID() uint64 {
	return n.Identifier
}

func (n *NameDocument) SetID(i uint64) {
	n.Identifier = i
}

func init() {
	gob.Register(&NameDocument{})
}

type NumberDocument struct {
	Identifier uint64
	Number int
}

func (n *NumberDocument) ID() uint64 {
	return n.Identifier
}

func (n *NumberDocument) SetID(id uint64) {
	n.Identifier = id
}

func init() {
	gob.Register(&NumberDocument{})
}
