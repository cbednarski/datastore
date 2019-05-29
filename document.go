package datastore

// Document is the interface that allows a struct to be stored in a Collection.
type Document interface {
	// ID is the "primary key" used to load and save a Document. It must be
	// unique in each collection, and the ID field must be public so it can be
	// encoded / decoded by the Gob package.
	ID() uint64

	// SetID is called by Datastore internally, and while it is required to
	// satisfy the interface you should never need to call this yourself.
	SetID(id uint64)
}
