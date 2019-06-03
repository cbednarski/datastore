// Package datastore provides a simple, embedded database for Go applications
// that want to store complex types on disk using a binary encoding.
//
// datastore is suitable for cases where you don't want to use a complex
// database like Postgres, don't have the performance needs of LSM or LMDB, and
// want a serialization format that is more Go-native than JSON.
//
// Data stored in a Datastore must implement the Document interface and are
// stored in Collections. Each Collection may include one type of Document. Each
// Document is indexed by a uint64 that behaves as an autoincrement primary key
// field. Datastore supports basic CRUD and Find operations on each Collection.
//
// Creating a collection:
//
//	ds, err := datastore.OpenOrCreate("mypets.datastore")
//	pets := ds.In("pets")
//
// Adding a new document:
//
//	pet := &Pet{}
//	pets.Upsert(pet)
//
// When retrieving a document from a collection you must use a Go type assertion:
//
//	pets.FindOne(func(doc datastore.Document) bool {
//		if pet, ok := doc.(*Pet); ok {
//			return pet.Name == "Chomper"
//		}
//		return false
//	}
//
// Under the hood datastore encodes structs into a gzipped Gob stream and writes
// them to a file when you call Flush. Aside from Open and Flush, all other
// operations are performed in memory, so your dataset (plus some overhead) must
// not exceed available memory. In addition to decoding stored data, transitory
// data structures are re-created during the Open call.
//
// As mentioned, collections are created based on the type of the data stored in
// them. There are currently no special facilities provided for migrating data,
// so take care when renaming types. Gob is designed to handle addition and
// deletion of fields but renaming a type will likely cause data to be ignored
// the next time the datastore is opened. You may safely Open a datastore that
// contains incompatible types but calling Flush will destroy any incompatible
// type data.
//
// datastore is designed to be safe for concurrent use by a single process (with
// multiple goroutines). Datastore uses an atomic write during Flush, but
// otherwise does not attempt to be crash-safe. datastore is designed to be
// small, simple, and safe but is not designed for high performance -- for high
// performance or high capacity embedded data stores, see any number of
// embeddable LSM or LMDB derivatives.
package datastore

import (
	"compress/gzip"
	"encoding/gob"
	"errors"
	"os"
	"sync"
	"time"
)

// Extension is used as a common file extension to facilitate identifying a
// particular file as a datastore (like .sqlite). This is a recommendation and
// is not required. Note that datastore files will always have a gzip comment
// header "datastore" that may be used to identify them.
const Extension = ".datastore"

var ErrInvalidSignature = errors.New("datastore signature does not match")
var ErrInvalidType = errors.New("type does not match collection")

// Datastore contains Collections of Documents and coordinates reading / writing
// them to a file.
type Datastore struct {
	// Path to the datastore. It must be writable, and usually ends with the
	// Datafile constant.
	path string

	// see Signature
	signature string

	flush sync.Mutex

	// TODO replace this with an atomic value so we don't need to check flush
	//  mutex to set the dirty flag. Or maybe just delete dirty completely and
	//  flush every time.
	dirty bool

	// Collections is public because Gob needs to read it. You should not modify
	// this map directly. Use In(), InType(), and the Collection API instead.
	Collections map[string]*Collection
}

// Signature returns the signature for this datastore. See the Signature
// package-level function for details.
func (d *Datastore) Signature() string {
	return d.signature
}

// Path returns the filesystem path where the datastore is located. This path is
// the same as the one passed in during Open or Create and is NOT further
// processed by path.Abs or any similar functions.
func (d *Datastore) Path() string {
	return d.path
}

// In provides a pseudo-fluent interface to select a specific Collection from
// this Datastore by name. Each collection may only hold one type of Document.
//
// If the Collection does not already exist it will be initialized. Newly-
// initialized Collections do not have a type until a Document is added.
//
// Note: We recommend using constants for your Collection names so a typo
// doesn't cause your data to go into the wrong collection.
func (d *Datastore) In(name string) *Collection {
	// Find existing collection
	if c, ok := d.Collections[name]; ok {
		return c
	}

	// Create a new collection
	c := &Collection{
		Items:     map[uint64]Document{},
		datastore: d,
	}
	d.Collections[name] = c
	return c
}

// Init wraps In to initialize a Collection with type information. The document
// is not stored so it may be a zero type or an initialized document. Init
// returns an error if the collection already exists and has a different type.
func (d *Datastore) Init(name string, document Document) (*Collection, error) {
	c := d.In(name)

	if err := c.SetType(document); err != nil {
		return nil, err
	}

	return c, nil
}

// Flush writes changes to disk, or no-ops if it has already flushed all
// changes. This uses atomic replace and is not compatible with Windows.
func (d *Datastore) Flush() error {
	d.flush.Lock()
	defer d.flush.Unlock()
	if !d.dirty {
		return nil
	}

	temp := d.path + ".tmp"
	final := d.path

	if err := os.RemoveAll(temp); err != nil {
		return err
	}

	file, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}

	writer := gzip.NewWriter(file)
	writer.Comment = d.signature
	writer.ModTime = time.Now()

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(d); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(temp, final); err != nil {
		return err
	}

	return nil
}

// New creates a new in-memory Datastore. Flush will never succeed with this
// type of Datastore. For a persistent Datastore, start with Open or Create.
func New() *Datastore {
	return &Datastore{
		Collections: map[string]*Collection{},
	}
}

// Create creates a new datastore and flushes it to disk. For details on
// the signature parameter, see the docs for Signature. Create will immediately
// call Flush to create the datastore file and detect any I/O problems.
func Create(path, signature string) (*Datastore, error) {
	ds := New()
	ds.path = path
	ds.signature = Signature(signature)

	if err := ds.Flush(); err != nil {
		return nil, err
	}

	return ds, nil
}

// Open opens a Datastore for reading and writing. The given signature must
// match the signature stored in the specified Datastore.
//
// Important note: Before calling Open you must call gob.Register for each type
// you expect to read from the Datastore or Gob will not be able to decode them.
// Typically you should perform the gob.Register call in an init() func in the
// same source file where you define the ID() and SetID() methods for your types.
//
// If Open fails with ErrInvalidSignature you can call ds.Signature() on the
// result to see what Signature was found on disk.
func Open(path, signature string) (ds *Datastore, err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return
	}

	// TODO acquire exclusive read/write lock when opening the file
	//  Q: Is this actually necessary since we use an atomic write/rename? Probably...
	file, err := os.OpenFile(path, os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return
	}
	defer reader.Close()

	ds = &Datastore{
		path: path,
		signature: reader.Comment,
	}

	// Validate signature matches before we decode
	if reader.Comment != Signature(signature) {
		return nil, ErrInvalidSignature
	}

	decoder := gob.NewDecoder(reader)

	if err = decoder.Decode(ds); err != nil {
		return
	}

	// Restore transient data structures (private fields)
	for _, c := range ds.Collections {
		c.datastore = ds
		c.generateList()
	}

	return
}

// OpenOrCreate is a convenience function that can be called to read or
// initialize a datastore in a single call. We first call Open, and if the Open
// call fails because of os.IsNotExist we will attempt to Create it.
func OpenOrCreate(path, signature string) (store *Datastore, err error) {
	store, err = Open(path, signature)
	if err != nil && os.IsNotExist(err) {
		store, err = Create(path, signature)
	}
	return
}

// Signature (the function) prepends "datastore:" to whatever value you supply.
// It is called internally by Open and Create so you generally won't need to
// call this function yourself.
//
// Signature (the value) is used as a schema identifier to prevent programs from
// reading or writing incompatible encodings to a Datastore file on disk.
//
// Each program should use a unique identifier for its Datastore signature. The
// recommended format is program_name.schema_version, but it may be any string
// of ISO-8859-1 characters. Including the schema version will allow your
// program to change the format of the datastore over time without breaking
// compatibility with older versions of the program or silently corrupting your
// data if the wrong program version is used.
//
// datastore's Open call enforces an exact match and datastore does not do any
// fuzzy version matching. Remember that this is a version for your *data*, not
// your program. See the Gob package for details on how the types themselves are
// allowed to change over time without requiring a change in signature.
//
// Signature is stored in the gzip header for the Datastore so you can scan for
// and read signatures without having to decode the entire structure. You can
// use ReadSignature to inspect this header without attempting to read the
// entire document into memory.
func Signature(signature string) string {
	return "datastore:" + signature
}

// ReadSignature is used to read the signature from a Datastore on disk without
// decoding the entire file. The file will be opened in non-exclusive read mode.
// Note that if the file is already locked this function may return an error.
func ReadSignature(path string) (signature string, err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return
	}

	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return
	}
	defer reader.Close()

	return reader.Comment, nil
}
