# Datastore

Datastore is a simple embedded database for Go programs. It is suitable for
cases where you don't want to use a complex datastore like Postgres, don't have
the performance needs of LSM or LMDB, and prefer a serialization format that is
more Go-native than JSON.

Datastore is designed to be **simple**. It allows you to use Go's native types
by implementing a simple interface (no code generation needed). It reads and
writes arbitrary data to a single file. It is safe for concurrent use and
includes a handful of basic safety features to help prevent footgun situations.

## Motivation

I wrote datastore to provide disk-based persistence for a feed scraping tool to
keep track of subscriptions, feed updates, discovered items, and to track which
items had already been retrieved so they weren't fetched multiple times.

Datastore tracks metadata while the feed content itself is written to disk in
raw format. I did not want to setup Postgres just to run a simple CLI, and,
while I have used BoltDB in the past, I felt it was a bit cumbersome for this
project which has complex types and relations but did not need super duper
performance in the database layer.

## Considerations

It may be useful to think of datastore not as a database, but as an encoder. In
fact, Gob is doing most of the heavy lifting for us. As a direct consequence,
your entire database must fit into memory, and must be flushed to disk in order
to save it. There are no transactions -- just a "snapshot" when you call Flush.

The abstraction datastore provides makes it much easier to write your program
because all of the type reflection, encoding, and file I/O are abstracted away
behind a simple interface, freeing you to focus on business logic instead.

If your goal is to build a high performance networked server, or your data has
rapid, unbounded growth, or you need ACID features like transactions, datastore
is not the best choice. However, for less demanding projects like CLIs or
networked services with smaller datasets, datastore will work just fine.

## Examples

### Open a Database

	ds, err := datastore.CreateOrOpen("filename"+datastore.Extension, "myappv1")
	...

The second parameter to `CreateOrOpen` is a sanity check that you're opening the
right datastore. Think of it as like an API version.

### Implement the Document Interface

	type MyType struct {
		ID uint64
		...
	}

	func (m *MyType) ID() uint64 {
		return m.ID
	}

	func (m *MyType) SetID(id uint64) {
		m.ID = id
	}

### Insert a Document

	myType := &MyType{}
	collection := ds.InType(myType)
	collection.Upsert(myType)

Documents are stored in type-specific collections.

### Retrieve a Document

	document := collection.FindOne(func(doc datastore.Document) bool {
		if myType, ok := doc.(*MyType); ok {
			return myType.Name == "this is the one!"
		}
		return false
	}
	myType, ok := document.(*MyType)
	...

### Flush Database to Disk

**Warning**: Changes are only saved when you flush them to disk. Don't forget to
do this!

	ds.Flush()

### In-memory Datastore / Testing Collections

For testing or *ephemeral in-memory usage* you can create a Datastore without
the Open or Create calls:

    ds := &datastore.Datastore{}
    collection := ds.In("temp")

Flush will error so there is no way to persist this Datastore. However, this
allows you to get a Collection that works for testing, and aside from Flush the
rest of the API works the same way as a persistent Datastore.

## Developing

`datastore` is a library. Tests are written in the `datastore_test` package (not
`datastore`) to ensure that the third party API works as expected and does not
rely on private member access.

Make sure to always run tests with the `-race` flag to ensure safe concurrent
behavior.

There is a fixture file in `testdata` that is generated using `go generate`.
Certain changes to the tests may require you to regenerate this file. After
regenerating it you should check it in.
