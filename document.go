package datastore

type Document interface {
	ID() uint64
	SetID(id uint64)
}
