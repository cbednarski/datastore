# Datastore

Datastore is a simple embedded database for Go programs. It allows you to store complex structures on disk in a single file, and is a great option when you don't want to maintain a separate database like Postgres, don't need the raw performance of an embedded LSM or LMDB database, and don't want the overhead of JSON.

Datastore is designed to be **simple**. You can read and understand the code in a single sitting! It allows you to use Go's native types without code generation! It is safe for concurrent use and includes a handful of basic safety features to help prevent footgun situations.

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
