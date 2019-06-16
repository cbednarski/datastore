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

type NumberDocument struct {
	Identifier uint64
	Number     int
}

func (n *NumberDocument) ID() uint64 {
	return n.Identifier
}

func (n *NumberDocument) SetID(id uint64) {
	n.Identifier = id
}

func init() {
	gob.Register(&NameDocument{})
	gob.Register(&NumberDocument{})
	// InvalidDocument is NOT to be included in the init func because it is
	// specifically used in tests where we check what happens when we don't
	// do this.
}

type InvalidDocument struct {
	Identifier uint64
	Name       string
}

func (i *InvalidDocument) ID() uint64 {
	return i.Identifier
}

func (i *InvalidDocument) SetID(id uint64) {
	i.Identifier = id
}
