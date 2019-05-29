package datastore

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// Collection is a RWMutex-managed map containing a type that embeds Document.
// The fields Type, Items, and CurrentIndex are public for serialization
// purposes but you should NEVER modify these directly. Use the methods instead.
type Collection struct {
	// Type indicates the reflected type of the Documents stored in this
	// collection. DO NOT MODIFY.
	Type         string

	// Items holds the map of Documents in this Collection. DO NOT MODIFY.
	Items        map[uint64]Document

	// CurrentIndex holds the autoincrement value for this Collection. DO NOT MODIFY.
	CurrentIndex uint64

	list         []uint64
	mutex        sync.RWMutex
	datastore    *Datastore
}

// Upsert inserts or updates a Document in the collection.
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
	// TODO this currently assumes that insertion is always in ascending order,
	//  but in the case of a re-insertion (and possibly other cases) this is not
	//  guaranteed. We need to perform a binary search / insert to fix this.
	c.list = append(c.list, document.ID())
	c.mutex.Unlock()
	return nil
}

// DeleteKey removes the indicated key from the Collection, or no-ops if the key
// is not present.
func (c *Collection) DeleteKey(key uint64) {
	c.datastore.flush.Lock()
	c.datastore.dirty = true
	c.datastore.flush.Unlock()

	c.mutex.Lock()
	delete(c.Items, key)
	deleteKeyFromList(&c.list, key)
	c.mutex.Unlock()
}

// Delete removes the Document from the Collection. Note that Delete does not
// invalidate the Document, so if you later call Upsert with the same Document
// it will be re-inserted in place.
func (c *Collection) Delete(document Document) {
	if document.ID() == 0 {
		return
	}
	c.DeleteKey(document.ID())
}

// Find a Document by key. This is useful for "foreign key" type relationships
// where you do not want to create a cyclical reference between objects, or a
// fast lookup by ID if you already know it. Returns nil if the key is not found
// in the Collection.
func (c *Collection) FindKey(key uint64) Document {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	item, ok := c.Items[key]
	if !ok {
		return nil
	}
	return item
}

// Filter is a lookup-style function that returns a list of Documents that
// satisfy the filter. If no Documents satisfy the filter, the list will be
// empty. Your callback should perform a type assertion so you can inspect the
// fields of the specific type, since filtering on Document type is useless. The
// list of Documents is returned in random (map) order. This function enumerates
// the entire Collection (i.e. table scan).
func (c *Collection) Filter(finder func(Document) bool) []Document {
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

// FindOne is a lookup-style function that returns the first Document that
// satisfies the callback. Note that the map is iterated in random (map) order
// so it's possible that calling this function multiple times with the same
// function will return different documents. If no Document satisfying the
// callback is found, FindOne returns nil. This function enumerates the entire
// Collection (i.e. table scan) until a match is found.
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

// List returns a sorted list of keys (in ascending order) for all Documents
// currently held in the Collection.
func (c *Collection) List() []uint64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.list
}

// generateList is an internal call that rebuilds the list of keys after
// restoring a datastore from disk. It should not need to be called otherwise.
func (c *Collection) generateList() {
	c.mutex.Lock()
	c.list = []uint64{}
	for _, item := range c.Items {
		c.list = append(c.list, item.ID())
	}
	sort.Sort(UIntSlice(c.list))
	c.mutex.Unlock()
}

// CName derives the Collection name from a type.
func CName(kind interface{}) string {
	return reflect.TypeOf(kind).String()
}
