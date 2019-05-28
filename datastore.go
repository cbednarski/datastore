// Package datastore provides a simple, embedded database for Go applications
// that want to store complex types on disk using a binary encoding.
//
// datastore is suitable for cases where you don't want to use a complex
// database like Postgres, don't have the performance needs of LSM or LMDB, and
// want a serialization format that is more Go-native than JSON.
//
// Data stored in a datastore must implement the Document interface and are
// stored in a Collection based on their type. Each Document is indexed by a
// uint64 that behaves as an autoincrement primary key field. Datastore
// supports basic CRUD and Find* operations on each Collection.
//
// Creating a collection:
//
//	ds, err := OpenOrCreate("datastore")
//	collection := ds.InType(&MyType{})
//
// Adding a new document:
//
//	myType := &MyType{}
//	collection.Upsert(myType)
//
// When retrieving a document from a collection you must use a Go type assertion:
//
//	collection.FindOne(func(doc datastore.Document) bool {
//		if myType, ok := doc.(*MyType); ok {
//			return myType.Name == "this is the one!"
//		}
//		return false
//	}
//
// Under the hood datastore encodes structs into a gzipped gob stream and writes
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
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Extension is used as a common file extension to facilitate identifying a
// particular file as a datastore (like .sqlite). This is a recommendation and
// is not required. Note that datastore files will always have a gzip comment
// header "datastore" that may be used to identify them.
const Extension = ".datastore"

// Collection is a RWMutex-managed map containing a type that embeds Document.
// The fields Type, Items, and CurrentIndex are public for serialization
// purposes but you should NEVER modify these directly. Use the methods instead.
type Collection struct {
	Type         string
	Items        map[uint64]Document
	CurrentIndex uint64
	list         []uint64
	mutex        sync.RWMutex
	datastore    *Datastore
}

func (c *Collection) Upsert(document Document) error {
	c.datastore.flush.Lock()
	c.datastore.dirty = true
	c.datastore.flush.Unlock()

	kind := CName(document)
	if c.Type != kind {
		return fmt.Errorf("collection holds %s but document is %s", c.Type, kind)
	}

	c.mutex.Lock()

	if document.ID() == 0 {
		c.CurrentIndex += 1
		document.SetID(c.CurrentIndex)
	}

	c.Items[document.ID()] = document
	c.list = append(c.list, document.ID())
	c.mutex.Unlock()
	return nil
}

func (c *Collection) DeleteKey(key uint64) {
	c.datastore.flush.Lock()
	c.datastore.dirty = true
	c.datastore.flush.Unlock()

	c.mutex.Lock()
	delete(c.Items, key)
	deleteKeyFromList(&c.list, key)
	c.mutex.Unlock()
}

func (c *Collection) Delete(document Document) {
	if document.ID() == 0 {
		return
	}
	c.DeleteKey(document.ID())
}

func (c *Collection) FindKey(key uint64) Document {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	item, ok := c.Items[key]
	if !ok {
		return nil
	}
	return item
}

func (c *Collection) Find(finder func(Document) bool) []Document {
	found := []Document{}
	c.mutex.RLock()

	for _, document := range c.Items {
		if finder(document) {
			found = append(found, document)
		}
	}

	c.mutex.RUnlock()
	return found
}

func (c *Collection) FindOne(finder func(Document) bool) Document {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	for _, document := range c.Items {
		if finder(document) {
			return document
		}
	}
	return nil
}

func (c *Collection) List() []uint64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.list
}

func (c *Collection) generateList() {
	c.mutex.Lock()
	c.list = []uint64{}
	for _, item := range c.Items {
		c.list = append(c.list, item.ID())
	}
	sort.Sort(UIntSlice(c.list))
	c.mutex.Unlock()
}

type Datastore struct {
	// Path to the datastore. It must be writable, and usually ends with the
	// Datafile constant.
	path string

	// see Format
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

// Signature is an application identifier that is validated during the Open
// call. It is used to prevent different programs or incompatible versions of a
// program from clobbering each other's data. It is also written into the file's
// gzip header so it may be read before attempting to decode the data.
//
// Per the gzip spec, Signature must consist of ISO 8859-1 characters only.
func (d *Datastore) Signature() string {
	return d.signature
}

func (d *Datastore) In(name string) *Collection {
	// Find existing collection
	if c, ok := d.Collections[name]; ok {
		return c
	}

	// Create a new collection
	c := &Collection{
		Type:      name,
		Items:     map[uint64]Document{},
		datastore: d,
	}
	d.Collections[name] = c
	return c
}

func (d *Datastore) InType(t interface{}) *Collection {
	return d.In(CName(t))
}

// Flush writes any pending changes to disk, or no-ops if it has already flushed
// all changes. This uses atomic replace and is not compatible with Windows.
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

// Create creates a new datastore and flushes it to disk
func Create(path, signature string) (*Datastore, error) {
	store := &Datastore{
		path:        path,
		signature:   Signature(signature),
		Collections: map[string]*Collection{},
	}

	if err := store.Flush(); err != nil {
		return nil, err
	}

	return store, nil
}

// Open opens a datastore for reading and writing
func Open(path, signature string) (*Datastore, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}

	// Read file from disk
	// TODO acquire exclusive read/write lock when opening the file
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Decompress file
	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	if reader.Comment != Signature(signature) {
		return nil, fmt.Errorf("datastore signature does not match. Expected %s, found %s", Signature(signature), reader.Comment)
	}

	decoder := gob.NewDecoder(reader)

	store := &Datastore{
		path: path,
	}

	if err := decoder.Decode(store); err != nil {
		return nil, err
	}

	for _, c := range store.Collections {
		c.datastore = store
		c.generateList()
	}

	return store, nil
}

func OpenOrCreate(path, signature string) (store *Datastore, err error) {
	store, err = Open(path, signature)
	if err != nil && os.IsNotExist(err) {
		store, err = Create(path, signature)
	}
	return
}

// CName derives the Collection name from a type
func CName(kind interface{}) string {
	return reflect.TypeOf(kind).String()
}

func Signature(signature string) string {
	return "datastore:" + signature
}

type UIntSlice []uint64

func (u UIntSlice) Len() int           { return len(u) }
func (u UIntSlice) Less(i, j int) bool { return u[i] < u[j] }
func (u UIntSlice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }

func deleteKeyFromList(list *[]uint64, key uint64) {
	// TODO replace this with a binary search when the list grows so we get
	//  O(log n) instead of O(n). For smaller datasets it doesn't matter.
	idx := -1
	for i, val := range *list {
		if val == key {
			idx = i
			break
		}
	}

	// not found, exit
	if idx < 0 {
		return
	}
	// First item
	if idx == 0 {
		*list = (*list)[1:]
		return
	}
	// Last item
	if idx == len(*list)-1 {
		*list = (*list)[0:idx]
		return
	}
	// Middle item
	*list = append((*list)[0:idx], (*list)[idx+1:]...)
}
